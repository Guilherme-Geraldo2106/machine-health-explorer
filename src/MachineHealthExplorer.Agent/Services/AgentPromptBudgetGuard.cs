using System.Text;
using System.Text.Json;
using MachineHealthExplorer.Agent.Models;
using MachineHealthExplorer.Agent.Serialization;

namespace MachineHealthExplorer.Agent.Services;

internal static class AgentPromptBudgetGuard
{
    public static int MaxWorkerPromptTokens(AgentOptions options, int maxOutputTokensForCall)
    {
        ArgumentNullException.ThrowIfNull(options);
        var host = AgentContextBudgetEstimator.GetEffectiveHostContextTokens(options);
        var reserve = Math.Max(96, options.WorkerPromptReserveTokens);
        var maxOut = Math.Max(32, maxOutputTokensForCall);
        return Math.Max(256, host - maxOut - reserve);
    }

    public static int EstimateWorkerPromptTokens(AgentOptions options, string systemPrompt, string userMessageBody)
    {
        ArgumentNullException.ThrowIfNull(options);
        var messages = new[]
        {
            new AgentConversationMessage
            {
                Role = AgentConversationRole.User,
                Content = userMessageBody
            }
        };

        return AgentContextBudgetEstimator.EstimatePromptTokens(
            options,
            systemPrompt,
            messages,
            Array.Empty<AgentToolDefinition>());
    }

    public static int EstimateConversationPromptTokens(
        AgentOptions options,
        string systemPrompt,
        IReadOnlyList<AgentConversationMessage> messages,
        IReadOnlyList<AgentToolDefinition> tools)
        => AgentContextBudgetEstimator.EstimatePromptTokens(options, systemPrompt, messages, tools);

    public static string BuildBoundedMemoryWorkerUserContent(
        AgentOptions options,
        string systemPrompt,
        string latestUserInput,
        IReadOnlyList<AgentConversationMessage> recentTranscript,
        IReadOnlyList<AgentToolExecutionRecord> toolExecutions,
        string compactMemoryJson,
        int maxOutputTokensForCall)
    {
        var budget = MaxWorkerPromptTokens(options, maxOutputTokensForCall);
        var perMsg = 900;
        var toolChars = 520;
        var memoryCap = compactMemoryJson.Length;

        for (var round = 0; round < 72; round++)
        {
            var mem = memoryCap >= compactMemoryJson.Length
                ? compactMemoryJson
                : string.Concat(compactMemoryJson.AsSpan(0, Math.Min(memoryCap, compactMemoryJson.Length)), "…");
            var user = $"""
Latest user message:
{CompactPlain(latestUserInput, 2000)}

Recent transcript:
{RenderTranscriptForWorker(recentTranscript, perMsg)}

Recent tool lines:
{FormatToolExecutionLines(toolExecutions, toolChars)}

Previous memory (compact JSON):
{mem}
""";

            if (EstimateWorkerPromptTokens(options, systemPrompt, user) <= budget)
            {
                return user;
            }

            if (perMsg > 220)
            {
                perMsg -= 140;
                continue;
            }

            if (toolChars > 160)
            {
                toolChars -= 120;
                continue;
            }

            if (memoryCap > 400)
            {
                memoryCap = Math.Max(200, memoryCap - 700);
                continue;
            }

            break;
        }

        var fallbackMem = compactMemoryJson.Length <= 200 ? compactMemoryJson : string.Concat(compactMemoryJson.AsSpan(0, 200), "…");
        return $"""
Latest user message:
{CompactPlain(latestUserInput, 800)}

Recent transcript:
{RenderTranscriptForWorker(recentTranscript, 180)}

Recent tool lines:
{FormatToolExecutionLines(toolExecutions, 120)}

Previous memory (compact JSON):
{fallbackMem}
""";
    }

    public static string BuildBoundedSummarizerUserContent(
        AgentOptions options,
        string systemPrompt,
        string memoryBlock,
        IReadOnlyList<AgentConversationMessage> headMessages,
        int maxOutputTokensForCall)
    {
        var budget = MaxWorkerPromptTokens(options, maxOutputTokensForCall);
        var perMsg = 900;
        var memCap = memoryBlock.Length;

        for (var round = 0; round < 48; round++)
        {
            var mem = memCap >= memoryBlock.Length
                ? memoryBlock
                : string.Concat(memoryBlock.AsSpan(0, Math.Min(memCap, memoryBlock.Length)), "…");
            var user = $"""
Existing memory (may be empty):
{mem}

Older conversation transcript to compress:
{RenderTranscriptForWorker(headMessages, perMsg)}
""";

            if (EstimateWorkerPromptTokens(options, systemPrompt, user) <= budget)
            {
                return user;
            }

            if (perMsg > 220)
            {
                perMsg -= 160;
                continue;
            }

            if (memCap > 400)
            {
                memCap = Math.Max(200, memCap - 600);
                continue;
            }

            break;
        }

        return $"""
Existing memory (may be empty):
{CompactPlain(memoryBlock, 400)}

Older conversation transcript to compress:
{RenderTranscriptForWorker(headMessages, 180)}
""";
    }

    /// <summary>
    /// Drops oldest chat messages (never the last <paramref name="keepTail"/>) until the prompt fits the budget.
    /// </summary>
    public static List<AgentConversationMessage> FitConversationTail(
        AgentOptions options,
        string systemPrompt,
        IReadOnlyList<AgentConversationMessage> messages,
        int maxOutputTokensForCall,
        int keepTail = 6)
    {
        ArgumentNullException.ThrowIfNull(options);
        var list = messages.ToList();
        var budget = MaxWorkerPromptTokens(options, maxOutputTokensForCall);
        var guard = 0;
        while (list.Count > keepTail && guard++ < 128)
        {
            var est = EstimateConversationPromptTokens(options, systemPrompt, list, Array.Empty<AgentToolDefinition>());
            if (est <= budget)
            {
                return list;
            }

            list.RemoveAt(0);
        }

        return list;
    }

    public static string CompactPlain(string value, int maxLength)
    {
        if (string.IsNullOrEmpty(value))
        {
            return string.Empty;
        }

        var normalized = value.ReplaceLineEndings(" ");
        if (normalized.Length <= maxLength)
        {
            return normalized;
        }

        return string.Concat(normalized.AsSpan(0, maxLength), "…");
    }

    public static string RenderTranscriptForWorker(IReadOnlyList<AgentConversationMessage> messages, int maxContentChars)
    {
        var builder = new StringBuilder();
        foreach (var message in messages)
        {
            if (message.Role == AgentConversationRole.Tool)
            {
                builder.AppendLine($"tool[{message.Name}]: {CompactPlain(message.Content ?? string.Empty, maxContentChars)}");
                continue;
            }

            if (message.Role == AgentConversationRole.Assistant && message.ToolCalls.Count > 0)
            {
                builder.AppendLine($"assistant[tool_calls:{message.ToolCalls.Count}]");
                continue;
            }

            builder.AppendLine($"{message.Role}: {CompactPlain(message.Content ?? string.Empty, maxContentChars)}");
        }

        return builder.ToString().Trim();
    }

    public static string FormatToolExecutionLines(IReadOnlyList<AgentToolExecutionRecord> toolExecutions, int maxPerResult)
    {
        if (toolExecutions.Count == 0)
        {
            return "(none)";
        }

        return string.Join(
            "\n",
            toolExecutions.Select(execution => $"{execution.ToolName} => {CompactPlain(execution.ResultJson ?? string.Empty, maxPerResult)}"));
    }

    public static string BuildCompactMemoryProjectionJson(AgentConversationMemory memory, AgentOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        var digestCap = Math.Max(32, options.MemoryEvidenceDigestMaxChars);
        var maxDigests = Math.Max(1, options.MemoryToolEvidenceMaxDigests);
        var digests = memory.ToolEvidenceDigests
            .TakeLast(maxDigests)
            .Select(d => new
            {
                tool = d.ToolName,
                digest = CompactPlain(d.Digest, Math.Min(digestCap, 200))
            })
            .ToArray();

        var projection = new
        {
            currentUserIntent = CompactPlain(memory.CurrentUserIntent, 800),
            languagePreference = memory.LanguagePreference,
            pendingQuestions = memory.PendingQuestions.Take(4).Select(q => CompactPlain(q, 200)).ToArray(),
            confirmedFacts = memory.ConfirmedFacts.Take(6).Select(f => CompactPlain(f, 240)).ToArray(),
            rollingSummary = CompactPlain(memory.RollingSummary, 1200),
            recentToolDigests = digests
        };

        return JsonSerializer.Serialize(projection, AgentJsonSerializer.Options);
    }
}
