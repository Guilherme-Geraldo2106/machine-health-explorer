using MachineHealthExplorer.Agent.Abstractions;
using MachineHealthExplorer.Agent.Models;
using System.Text;
using System.Text.Json;

namespace MachineHealthExplorer.Agent.Services;

internal sealed class AgentEphemeralWorkerRunner
{
    private readonly AgentOptions _options;
    private readonly IAgentChatClient _chatClient;

    public AgentEphemeralWorkerRunner(AgentOptions options, IAgentChatClient chatClient)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _chatClient = chatClient ?? throw new ArgumentNullException(nameof(chatClient));
    }

    public static string FormatMemoryBlock(AgentConversationMemory memory)
    {
        var builder = new StringBuilder();
        if (!string.IsNullOrWhiteSpace(memory.CurrentUserIntent))
        {
            builder.AppendLine($"Current intent: {memory.CurrentUserIntent}");
        }

        if (!string.IsNullOrWhiteSpace(memory.LanguagePreference))
        {
            builder.AppendLine($"Preferred language: {memory.LanguagePreference}");
        }

        if (memory.PendingQuestions.Count > 0)
        {
            builder.AppendLine("Pending questions:");
            foreach (var question in memory.PendingQuestions.Take(6))
            {
                builder.AppendLine($"- {question}");
            }
        }

        if (memory.ConfirmedFacts.Count > 0)
        {
            builder.AppendLine("Confirmed facts:");
            foreach (var fact in memory.ConfirmedFacts.Take(12))
            {
                builder.AppendLine($"- {fact}");
            }
        }

        if (memory.ToolEvidenceDigests.Count > 0)
        {
            builder.AppendLine("Tool evidence:");
            foreach (var digest in memory.ToolEvidenceDigests.TakeLast(8))
            {
                builder.AppendLine($"- {digest.ToolName}: {digest.Digest}");
            }
        }

        if (!string.IsNullOrWhiteSpace(memory.RollingSummary))
        {
            builder.AppendLine("Rolling summary:");
            builder.AppendLine(AgentPromptBudgetGuard.CompactPlain(memory.RollingSummary, 2400));
        }

        return builder.ToString().Trim();
    }

    public async Task<string> SummarizeConversationHeadAsync(
        string model,
        IReadOnlyList<AgentConversationMessage> headMessages,
        AgentConversationMemory memory,
        CancellationToken cancellationToken)
    {
        if (headMessages.Count == 0)
        {
            return string.Empty;
        }

        if (!_options.EnableWorkerPasses || _options.MaxWorkerPasses <= 0)
        {
            return HeuristicTranscriptSummary(headMessages, memory);
        }

        var systemPrompt = """
You are a context compression worker for a data-analysis assistant.
Summarize ONLY what is needed for future turns: user goals, resolved facts, tool results, and open questions.
Output plain text with short bullet lines. Do not invent dataset facts.
""";

        var memoryBlock = FormatMemoryBlock(memory);
        var userPrompt = AgentPromptBudgetGuard.BuildBoundedSummarizerUserContent(
            _options,
            systemPrompt,
            memoryBlock,
            headMessages,
            _options.WorkerMaxOutputTokens);

        AgentModelResponse response;
        try
        {
            response = await _chatClient.CompleteAsync(new AgentModelRequest
            {
                Model = model,
                SystemPrompt = systemPrompt,
                Messages =
                [
                    new AgentConversationMessage
                    {
                        Role = AgentConversationRole.User,
                        Content = userPrompt
                    }
                ],
                Tools = Array.Empty<AgentToolDefinition>(),
                Temperature = _options.WorkerTemperature,
                MaxOutputTokens = _options.WorkerMaxOutputTokens,
                EnableTools = false
            }, cancellationToken).ConfigureAwait(false);
        }
        catch (AgentModelBackendException ex) when (ex.IsContextLengthExceeded)
        {
            return HeuristicTranscriptSummary(headMessages, memory);
        }

        var summary = (response.Content ?? string.Empty).Trim();
        return string.IsNullOrWhiteSpace(summary) ? HeuristicTranscriptSummary(headMessages, memory) : summary;
    }

    public async Task<AgentConversationMemory> RefreshMemoryAsync(
        string model,
        AgentConversationMemory memory,
        string latestUserInput,
        IReadOnlyList<AgentToolExecutionRecord> toolExecutions,
        IReadOnlyList<AgentConversationMessage> recentTranscript,
        CancellationToken cancellationToken)
    {
        var language = LanguageHeuristics.DetectLanguage(latestUserInput, memory.LanguagePreference);
        var next = memory with
        {
            CurrentUserIntent = string.IsNullOrWhiteSpace(latestUserInput) ? memory.CurrentUserIntent : latestUserInput.Trim(),
            LanguagePreference = string.IsNullOrWhiteSpace(language) ? memory.LanguagePreference : language,
            LastUpdatedUtc = DateTimeOffset.UtcNow
        };

        next = next.WithToolEvidence(
            toolExecutions.Select(execution => new AgentToolEvidenceDigest
            {
                ToolName = execution.ToolName,
                Digest = BuildToolDigest(execution.ToolName, execution.ResultJson, _options.MemoryEvidenceDigestMaxChars)
            }),
            _options.MemoryEvidenceDigestMaxChars,
            _options.MemoryToolEvidenceMaxDigests);

        if (!_options.EnableWorkerPasses || _options.MaxWorkerPasses <= 0)
        {
            return next;
        }

        var systemPrompt = """
You are a structured memory worker for a dataset assistant.
Return ONLY compact JSON with this shape:
{
  "currentUserIntent": string,
  "pendingQuestions": string[],
  "confirmedFacts": string[],
  "toolHighlights": [{"tool": string, "summary": string}],
  "language": "pt"|"en"|"",
  "rollingSummary": string
}
Rules:
- Do not invent dataset values; only restate what appears in the transcript or tool lines.
- Keep arrays short.
- If unknown, use empty strings/arrays.
""";

        var compactMemoryJson = AgentPromptBudgetGuard.BuildCompactMemoryProjectionJson(next, _options);
        var userPrompt = AgentPromptBudgetGuard.BuildBoundedMemoryWorkerUserContent(
            _options,
            systemPrompt,
            latestUserInput,
            recentTranscript,
            toolExecutions,
            compactMemoryJson,
            _options.WorkerMaxOutputTokens);

        AgentModelResponse response;
        try
        {
            response = await _chatClient.CompleteAsync(new AgentModelRequest
            {
                Model = model,
                SystemPrompt = systemPrompt,
                Messages =
                [
                    new AgentConversationMessage
                    {
                        Role = AgentConversationRole.User,
                        Content = userPrompt
                    }
                ],
                Tools = Array.Empty<AgentToolDefinition>(),
                Temperature = _options.WorkerTemperature,
                MaxOutputTokens = _options.WorkerMaxOutputTokens,
                EnableTools = false
            }, cancellationToken).ConfigureAwait(false);
        }
        catch (AgentModelBackendException ex) when (ex.IsContextLengthExceeded)
        {
            return next;
        }

        return MergeMemoryFromWorkerJson(next, AgentMemoryWorkerJsonNormalizer.PrepareMemoryWorkerJson(response.Content));
    }

    public async Task<string> RunEpilogueAsync(
        string model,
        string languagePreference,
        string partialAnswer,
        CancellationToken cancellationToken)
    {
        var languageHint = string.IsNullOrWhiteSpace(languagePreference) ? "the same language as the partial answer" : languagePreference;
        var systemPrompt = $"""
You finalize an assistant answer that was cut off by output limits.
Write ONLY the missing continuation text.
Rules:
- Do not repeat paragraphs already present in the partial answer.
- Keep the same language as the partial answer ({languageHint}).
- Stay consistent with any tool evidence implied by the transcript.
- If you cannot safely continue, write one short closing sentence that ends gracefully.
""";

        var userPrompt = $"""
Partial answer:
{AgentPromptBudgetGuard.CompactPlain(partialAnswer, 12000)}
""";

        var response = await _chatClient.CompleteAsync(new AgentModelRequest
        {
            Model = model,
            SystemPrompt = systemPrompt,
            Messages =
            [
                new AgentConversationMessage
                {
                    Role = AgentConversationRole.User,
                    Content = userPrompt
                }
            ],
            Tools = Array.Empty<AgentToolDefinition>(),
            Temperature = Math.Min(0.4, _options.WorkerTemperature + 0.1),
            MaxOutputTokens = Math.Clamp(_options.WorkerMaxOutputTokens, 128, 1200),
            EnableTools = false
        }, cancellationToken).ConfigureAwait(false);

        var tail = (response.Content ?? string.Empty).Trim();
        if (string.IsNullOrWhiteSpace(tail))
        {
            return partialAnswer.Trim();
        }

        return AgentResponseMerger.MergeAssistantFragments(partialAnswer.Trim(), tail);
    }

    public async Task<AgentModelResponse> RequestUserVisibleAnswerAsync(
        string model,
        AgentConversationMemory memory,
        IReadOnlyList<AgentConversationMessage> conversation,
        CancellationToken cancellationToken)
    {
        var systemPrompt = """
You produce ONLY the final assistant answer visible to an end user.
Rules:
- Put the full user-visible reply in the assistant message content field; keep any internal reasoning extremely short so it does not consume the output budget before the visible answer.
- No chain-of-thought, no hidden reasoning, no <think> tags, no internal analysis.
- Keep the same language as the user when it is obvious from the transcript or memory.
- Use only facts grounded in tool evidence already present in the transcript or memory.
- If evidence is missing, ask a brief clarifying question or suggest which dataset tool to use next (without inventing numbers).
""";

        var memoryBlock = FormatMemoryBlock(memory);
        var userPrompt = string.IsNullOrWhiteSpace(memoryBlock)
            ? "Write the final answer now."
            : $"""
Use this memory as authoritative facts:
{memoryBlock}

The conversation contains the latest user request and any tool evidence.
Write the final answer now.
""";

        var transcript = conversation.ToList();
        transcript.Add(new AgentConversationMessage
        {
            Role = AgentConversationRole.User,
            Content = userPrompt
        });

        var workerCap = Math.Clamp(_options.WorkerMaxOutputTokens, 128, 1600);
        var maxOut = Math.Max(
            Math.Clamp(_options.MinAssistantCompletionTokens, 96, _options.MaxOutputTokens),
            workerCap);
        maxOut = Math.Min(maxOut, _options.MaxOutputTokens);
        var fitted = AgentPromptBudgetGuard.FitConversationTail(
            _options,
            systemPrompt,
            transcript,
            maxOut,
            keepTail: 4);

        try
        {
            return await _chatClient.CompleteAsync(new AgentModelRequest
            {
                Model = model,
                SystemPrompt = systemPrompt,
                Messages = fitted,
                Tools = Array.Empty<AgentToolDefinition>(),
                Temperature = Math.Min(0.35, _options.WorkerTemperature + 0.05),
                MaxOutputTokens = maxOut,
                EnableTools = false
            }, cancellationToken).ConfigureAwait(false);
        }
        catch (AgentModelBackendException ex) when (ex.IsContextLengthExceeded && fitted.Count > 2)
        {
            var trimmed = AgentPromptBudgetGuard.FitConversationTail(_options, systemPrompt, fitted, maxOut, keepTail: 2);
            return await _chatClient.CompleteAsync(new AgentModelRequest
            {
                Model = model,
                SystemPrompt = systemPrompt,
                Messages = trimmed,
                Tools = Array.Empty<AgentToolDefinition>(),
                Temperature = Math.Min(0.35, _options.WorkerTemperature + 0.05),
                MaxOutputTokens = maxOut,
                EnableTools = false
            }, cancellationToken).ConfigureAwait(false);
        }
    }

    private AgentConversationMemory MergeMemoryFromWorkerJson(AgentConversationMemory baseline, string? normalizedJsonObject)
    {
        if (string.IsNullOrWhiteSpace(normalizedJsonObject))
        {
            return baseline;
        }

        try
        {
            using var document = JsonDocument.Parse(normalizedJsonObject);
            var root = document.RootElement;

            var intent = ReadString(root, "currentUserIntent");
            var language = ReadString(root, "language");
            var rolling = ReadString(root, "rollingSummary");

            var pending = ReadStringArray(root, "pendingQuestions");
            var facts = ReadStringArray(root, "confirmedFacts");

            var highlights = new List<AgentToolEvidenceDigest>();
            if (root.TryGetProperty("toolHighlights", out var highlightArray) && highlightArray.ValueKind == JsonValueKind.Array)
            {
                foreach (var element in highlightArray.EnumerateArray().Take(12))
                {
                    var tool = ReadString(element, "tool");
                    var summary = ReadString(element, "summary");
                    if (!string.IsNullOrWhiteSpace(tool) || !string.IsNullOrWhiteSpace(summary))
                    {
                        highlights.Add(new AgentToolEvidenceDigest
                        {
                            ToolName = tool,
                            Digest = summary
                        });
                    }
                }
            }

            var next = baseline with
            {
                CurrentUserIntent = string.IsNullOrWhiteSpace(intent) ? baseline.CurrentUserIntent : intent,
                PendingQuestions = pending.Count == 0 ? baseline.PendingQuestions : pending,
                ConfirmedFacts = facts.Count == 0 ? baseline.ConfirmedFacts : facts,
                LanguagePreference = string.IsNullOrWhiteSpace(language) ? baseline.LanguagePreference : language.Trim(),
                LastUpdatedUtc = DateTimeOffset.UtcNow
            };

            if (!string.IsNullOrWhiteSpace(rolling))
            {
                var mergedRolling = string.IsNullOrWhiteSpace(next.RollingSummary)
                    ? rolling
                    : $"{next.RollingSummary}\n{rolling}";

                next = next.WithRollingSummary(mergedRolling, _options.MemorySummaryMaxLength);
            }

            if (highlights.Count > 0)
            {
                next = next.WithToolEvidence(
                    highlights,
                    _options.MemoryEvidenceDigestMaxChars,
                    _options.MemoryToolEvidenceMaxDigests);
            }

            if (facts.Count > 0)
            {
                next = next.WithMergedFacts(facts, _options.MemorySummaryMaxLength);
            }

            return next;
        }
        catch
        {
            return baseline;
        }
    }

    private static string ReadString(JsonElement element, string name)
    {
        if (!element.TryGetProperty(name, out var property))
        {
            return string.Empty;
        }

        return property.ValueKind switch
        {
            JsonValueKind.String => property.GetString() ?? string.Empty,
            _ => property.ToString() ?? string.Empty
        };
    }

    private static IReadOnlyList<string> ReadStringArray(JsonElement element, string name)
    {
        if (!element.TryGetProperty(name, out var property) || property.ValueKind != JsonValueKind.Array)
        {
            return Array.Empty<string>();
        }

        return property.EnumerateArray()
            .Select(value => value.GetString() ?? string.Empty)
            .Where(value => !string.IsNullOrWhiteSpace(value))
            .Take(12)
            .ToArray();
    }

    private static string HeuristicTranscriptSummary(IReadOnlyList<AgentConversationMessage> headMessages, AgentConversationMemory memory)
    {
        var builder = new StringBuilder();
        if (!string.IsNullOrWhiteSpace(memory.RollingSummary))
        {
            builder.AppendLine(memory.RollingSummary);
        }

        foreach (var message in headMessages.TakeLast(24))
        {
            if (message.Role is AgentConversationRole.User or AgentConversationRole.Assistant)
            {
                var text = (message.Content ?? string.Empty).Trim();
                if (text.Length == 0)
                {
                    continue;
                }

                var label = message.Role == AgentConversationRole.User ? "User" : "Assistant";
                builder.AppendLine($"{label}: {AgentPromptBudgetGuard.CompactPlain(text, 420)}");
            }
        }

        return AgentPromptBudgetGuard.CompactPlain(builder.ToString().Trim(), 2800);
    }

    private static string BuildToolDigest(string toolName, string resultJson, int maxLength)
    {
        if (string.IsNullOrWhiteSpace(resultJson))
        {
            return $"{toolName}: <empty>";
        }

        var trimmed = resultJson.Trim();
        if (trimmed.Length <= maxLength)
        {
            return trimmed;
        }

        return string.Concat(trimmed.AsSpan(0, maxLength), "…");
    }
}

internal static class LanguageHeuristics
{
    public static string DetectLanguage(string? latestUserInput, string? priorPreference)
    {
        if (!string.IsNullOrWhiteSpace(latestUserInput) && HasStrongPortugueseSignal(latestUserInput))
        {
            return "pt";
        }

        if (!string.IsNullOrWhiteSpace(latestUserInput) && HasStrongEnglishSignal(latestUserInput))
        {
            return "en";
        }

        if (!string.IsNullOrWhiteSpace(priorPreference))
        {
            return priorPreference.Trim();
        }

        if (string.IsNullOrWhiteSpace(latestUserInput))
        {
            return string.Empty;
        }

        return "en";
    }

    private static bool HasStrongPortugueseSignal(string latestUserInput)
    {
        var text = latestUserInput.ToLowerInvariant();
        var trimmed = text.TrimStart();
        var compact = text.Trim(' ', '\t', '\r', '\n', '.', ',', '!', '?', ';', ':');

        if (compact is "ola" or "oi" or "bom dia" or "boa tarde" or "boa noite" or "obrigado" or "obrigada" or "valeu")
        {
            return true;
        }

        if (trimmed.StartsWith("qual ", StringComparison.Ordinal)
            || trimmed.StartsWith("quais ", StringComparison.Ordinal)
            || trimmed.StartsWith("qual?", StringComparison.Ordinal)
            || trimmed.StartsWith("qual,", StringComparison.Ordinal)
            || trimmed == "qual"
            || text.Contains(" qual ", StringComparison.Ordinal))
        {
            return true;
        }

        if (text.Contains('ã', StringComparison.Ordinal)
            || text.Contains('õ', StringComparison.Ordinal)
            || text.Contains('ç', StringComparison.Ordinal))
        {
            return true;
        }

        var portugueseHints = new[]
        {
            "não", "nao", "voce", "você", "média", "media", "máquina", "maquina", "porque", "porquê",
            "faixa", "falhas", "falha", "temperatura", "quantos", "quantas", "como ", " onde ", "explique", "resumo",
            "agrup", "coluna", "dados", "análise", "analise"
        };

        foreach (var hint in portugueseHints)
        {
            if (text.Contains(hint, StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }

    private static bool HasStrongEnglishSignal(string latestUserInput)
    {
        var trimmed = latestUserInput.Trim().ToLowerInvariant();
        if (trimmed.StartsWith("what ", StringComparison.Ordinal)
            || trimmed.StartsWith("which ", StringComparison.Ordinal)
            || trimmed.StartsWith("how ", StringComparison.Ordinal)
            || trimmed.StartsWith("why ", StringComparison.Ordinal)
            || trimmed.StartsWith("show ", StringComparison.Ordinal)
            || trimmed.StartsWith("list ", StringComparison.Ordinal))
        {
            return true;
        }

        return false;
    }
}
