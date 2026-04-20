using MachineHealthExplorer.Agent.Abstractions;
using MachineHealthExplorer.Agent.Models;
using MachineHealthExplorer.Agent.Services;
using Microsoft.Extensions.Logging;
using System.Text.Json;

namespace MachineHealthExplorer.Agent.MultiAgent;

internal sealed class CoordinatorAgent
{
    private readonly AgentOptions _options;
    private readonly IAgentChatClient _chatClient;
    private readonly ILogger _logger;

    public CoordinatorAgent(AgentOptions options, IAgentChatClient chatClient, ILogger logger)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _chatClient = chatClient ?? throw new ArgumentNullException(nameof(chatClient));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public Task<AgentCoordinatorPlanningResult> TryPlanAsync(
        string model,
        string userInput,
        AgentConversationMemory memory,
        IReadOnlyList<AgentConversationMessage> conversationTail,
        CancellationToken cancellationToken)
    {
        var multi = _options.MultiAgent;
        if (!multi.EnableCoordinatorLlmPlanning)
        {
            var heuristic = MultiAgentDispatchHeuristics.PostNormalizeDispatchPlan(
                userInput,
                MultiAgentDispatchHeuristics.Plan(userInput, multi),
                multi);
            LogPlan(heuristic, userInput);
            return Task.FromResult(new AgentCoordinatorPlanningResult(true, heuristic, null));
        }

        return TryPlanWithLlmAsync(model, userInput, memory, conversationTail, multi, cancellationToken);
    }

    private async Task<AgentCoordinatorPlanningResult> TryPlanWithLlmAsync(
        string model,
        string userInput,
        AgentConversationMemory memory,
        IReadOnlyList<AgentConversationMessage> conversationTail,
        MultiAgentOrchestrationOptions multi,
        CancellationToken cancellationToken)
    {
        string? lastError = null;
        var maxAttempts = Math.Max(1, multi.CoordinatorPlannerMaxRecoveryAttempts);

        for (var attempt = 0; attempt < maxAttempts; attempt++)
        {
            var isRecovery = attempt > 0;
            var maxOut = isRecovery
                ? Math.Min(
                    multi.CoordinatorPlannerMaxOutputTokens,
                    Math.Max(128, multi.CoordinatorPlannerRecoveryMaxOutputTokens))
                : multi.CoordinatorPlannerMaxOutputTokens;

            var systemPrompt = BuildCoordinatorSystemPrompt(multi, lastError);
            var userPrompt = isRecovery
                ? BuildShortPlannerUserPrompt(
                    userInput,
                    memory,
                    conversationTail,
                    Math.Clamp(multi.CoordinatorPlannerRecoveryMaxTailChars, 400, 24_000))
                : BuildPlannerUserPrompt(userInput, memory, conversationTail);

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
                    MaxOutputTokens = maxOut,
                    EnableTools = false
                }, cancellationToken).ConfigureAwait(false);
            }
            catch (AgentModelBackendException ex) when (ex.IsContextLengthExceeded)
            {
                lastError = $"Contexto excedido no backend do modelo: {ex.Message}";
                _logger.LogWarning(ex, "Coordinator attempt {Attempt} failed: context length.", attempt + 1);
                continue;
            }

            if (AgentFinishReason.IsTruncated(response.FinishReason))
            {
                lastError =
                    $"Saída do coordenador truncada (finish_reason={response.FinishReason}). Responda somente JSON compacto no formato exigido, sem markdown e sem texto fora do objeto.";
                _logger.LogWarning(
                    "Coordinator attempt {Attempt} unusable: truncated finish_reason={Reason}",
                    attempt + 1,
                    response.FinishReason);
                continue;
            }

            var surface = AgentToolScopePlanner.CombinePlannerSurface(response.Content, response.ReasoningContent);
            if (TryParsePlanDetailed(surface, multi, out var plan, out var parseError) && plan.Steps.Count > 0)
            {
                plan = MultiAgentDispatchHeuristics.PostNormalizeDispatchPlan(userInput, plan, multi);
                LogPlan(plan, userInput);
                return new AgentCoordinatorPlanningResult(true, plan with { UsedLlmPlanner = true }, null);
            }

            lastError = parseError ?? "Não foi possível interpretar o JSON do coordenador.";
            _logger.LogWarning(
                "Coordinator attempt {Attempt} produced unusable JSON: {Detail}",
                attempt + 1,
                lastError);
        }

        var userMessage =
            $"Não foi possível planejar os especialistas automaticamente após {maxAttempts} tentativas. Detalhe técnico: {lastError}";
        _logger.LogError("Coordinator planning exhausted retries ({Attempts}): {Error}", maxAttempts, lastError);
        return new AgentCoordinatorPlanningResult(
            false,
            new AgentDispatchPlan(Array.Empty<AgentDispatchStep>(), "coordinator_planning_failed", UsedLlmPlanner: true),
            userMessage);
    }

    private void LogPlan(AgentDispatchPlan plan, string userInput)
    {
        var specialists = string.Join(
            ", ",
            plan.Steps.Select(step => $"{step.SpecialistKind}(g={step.ParallelGroup}:{step.Reason})"));

        _logger.LogInformation(
            "Coordinator plan: llm={Llm} steps={StepCount} specialists=[{Specialists}] user_preview={Preview}",
            plan.UsedLlmPlanner,
            plan.Steps.Count,
            specialists,
            AgentPromptBudgetGuard.CompactPlain(userInput, 240));
    }

    private string BuildCoordinatorSystemPrompt(MultiAgentOrchestrationOptions multi, string? validationError)
    {
        var extension = string.IsNullOrWhiteSpace(multi.CoordinatorSystemPromptExtension)
            ? string.Empty
            : $"\n{multi.CoordinatorSystemPromptExtension.Trim()}";

        const string jsonShape = """
{"steps":[{"specialist":"Discovery|QueryAnalysis|FailureAnalysis|Reporting","reason":"short","parallel_group":0}],"notes":"optional"}
""";

        var recovery = string.IsNullOrWhiteSpace(validationError)
            ? string.Empty
            : $"""

The previous response failed validation:
{validationError.Trim()}

You MUST fix this now: output a single JSON object only (no markdown fences, no commentary before or after) in the exact shape:
{jsonShape.Trim()}
""";

        return $"""
You are the coordinator for a multi-agent dataset analysis system.
Your job is routing only: decide which specialist agents must run next.

Specialists (pick the minimum necessary set; specialists can run in parallel when independent):
- Discovery: structural context (schema/columns/types/distinct/profile).
- QueryAnalysis: quantitative queries (filters, aggregates, extrema, subset comparisons, failure rates by numeric bins via group_and_aggregate).
- FailureAnalysis: same generic tools as QueryAnalysis; do NOT schedule it together with QueryAnalysis for the same quantitative question (pick one — almost always QueryAnalysis).
- Reporting: internal structured reports / executive synthesis / reusable analysis examples.

Return ONLY JSON with this exact shape:
{jsonShape.Trim()}

Rules:
- Do not invent tool names here (routing only).
- Prefer Discovery before heavy quantitative work only when the user is exploring structure without a clear numeric question.
- Use parallel_group: steps that may run concurrently should share the same group id; dependent waves should use increasing ids.
- If the user asks multiple independent things, include multiple specialists and parallelize when safe.
- If a specialist is unnecessary, omit it.
- Keep the JSON small; avoid prose outside JSON.
{recovery}
{extension}
""";
    }

    private static string BuildPlannerUserPrompt(
        string userInput,
        AgentConversationMemory memory,
        IReadOnlyList<AgentConversationMessage> conversationTail)
    {
        var memoryBlock = AgentEphemeralWorkerRunner.FormatMemoryBlock(memory);
        var tail = RenderTail(conversationTail, maxChars: 6000);
        return $"""
User question:
{userInput}

Session memory (may be empty):
{memoryBlock}

Recent conversation tail:
{tail}
""";
    }

    private static string BuildShortPlannerUserPrompt(
        string userInput,
        AgentConversationMemory memory,
        IReadOnlyList<AgentConversationMessage> conversationTail,
        int maxTailChars)
    {
        var memoryBlock = AgentPromptBudgetGuard.CompactPlain(
            AgentEphemeralWorkerRunner.FormatMemoryBlock(memory),
            1200);
        var tail = RenderTail(conversationTail, maxChars: maxTailChars);
        return $"""
User question (authoritative):
{AgentPromptBudgetGuard.CompactPlain(userInput, 2000)}

Session memory (truncated):
{memoryBlock}

Recent conversation tail (truncated):
{tail}

Return ONLY the JSON dispatch plan for specialists.
""";
    }

    private bool TryParsePlanDetailed(
        string? jsonSurface,
        MultiAgentOrchestrationOptions multi,
        out AgentDispatchPlan plan,
        out string? failureDetail)
    {
        plan = new AgentDispatchPlan(Array.Empty<AgentDispatchStep>(), string.Empty, true);
        failureDetail = null;

        if (string.IsNullOrWhiteSpace(jsonSurface))
        {
            failureDetail = "Coordinator returned empty content.";
            return false;
        }

        try
        {
            var json = ExtractJsonObject(jsonSurface);
            using var document = JsonDocument.Parse(json);
            var root = document.RootElement;
            if (!root.TryGetProperty("steps", out var stepsElement))
            {
                failureDetail = "JSON object is missing required property 'steps'.";
                return false;
            }

            if (stepsElement.ValueKind != JsonValueKind.Array)
            {
                failureDetail = "Property 'steps' must be a JSON array.";
                return false;
            }

            var steps = new List<AgentDispatchStep>();
            var skipped = 0;
            foreach (var step in stepsElement.EnumerateArray())
            {
                var specialist = ReadString(step, "specialist");
                if (!TryMapSpecialist(specialist, out var kind))
                {
                    skipped++;
                    continue;
                }

                if (!IsSpecialistEnabled(kind, multi))
                {
                    skipped++;
                    continue;
                }

                var reason = ReadString(step, "reason");
                var group = ReadInt(step, "parallel_group") ?? 0;
                var expectsDatasetQueryEvidence = ReadNullableBool(step, "expects_dataset_query_evidence")
                    ?? kind != AgentSpecialistKind.Discovery;
                steps.Add(new AgentDispatchStep(
                    kind,
                    string.IsNullOrWhiteSpace(reason) ? "llm_dispatch" : reason.Trim(),
                    group,
                    expectsDatasetQueryEvidence));
            }

            if (steps.Count == 0)
            {
                failureDetail = skipped > 0
                    ? $"No enabled specialist steps parsed ({skipped} step entries were invalid or disabled)."
                    : "The 'steps' array was empty.";
                return false;
            }

            var notes = root.TryGetProperty("notes", out var notesElement) ? ReadString(notesElement) : string.Empty;
            plan = new AgentDispatchPlan(steps, notes, UsedLlmPlanner: true);
            return true;
        }
        catch (JsonException ex)
        {
            failureDetail = $"Invalid JSON: {ex.Message}";
            return false;
        }
    }

    private static bool IsSpecialistEnabled(AgentSpecialistKind kind, MultiAgentOrchestrationOptions multi)
        => kind switch
        {
            AgentSpecialistKind.Discovery => multi.Discovery.Enabled,
            AgentSpecialistKind.QueryAnalysis => multi.QueryAnalysis.Enabled,
            AgentSpecialistKind.FailureAnalysis => multi.FailureAnalysis.Enabled,
            AgentSpecialistKind.Reporting => multi.Reporting.Enabled,
            _ => false
        };

    private static bool TryMapSpecialist(string text, out AgentSpecialistKind kind)
    {
        kind = AgentSpecialistKind.Discovery;
        var trimmed = text.Trim();
        if (trimmed.Equals("Discovery", StringComparison.OrdinalIgnoreCase))
        {
            kind = AgentSpecialistKind.Discovery;
            return true;
        }

        if (trimmed.Equals("QueryAnalysis", StringComparison.OrdinalIgnoreCase))
        {
            kind = AgentSpecialistKind.QueryAnalysis;
            return true;
        }

        if (trimmed.Equals("FailureAnalysis", StringComparison.OrdinalIgnoreCase))
        {
            kind = AgentSpecialistKind.FailureAnalysis;
            return true;
        }

        if (trimmed.Equals("Reporting", StringComparison.OrdinalIgnoreCase))
        {
            kind = AgentSpecialistKind.Reporting;
            return true;
        }

        return false;
    }

    private static bool? ReadNullableBool(JsonElement element, string name)
    {
        if (!element.TryGetProperty(name, out var property))
        {
            return null;
        }

        return property.ValueKind switch
        {
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            JsonValueKind.String => bool.TryParse(property.GetString(), out var parsed) ? parsed : null,
            _ => null
        };
    }

    private static int? ReadInt(JsonElement element, string name)
    {
        if (!element.TryGetProperty(name, out var property))
        {
            return null;
        }

        return property.ValueKind switch
        {
            JsonValueKind.Number => property.TryGetInt32(out var value) ? value : null,
            JsonValueKind.String => int.TryParse(property.GetString(), out var parsed) ? parsed : null,
            _ => null
        };
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

    private static string ReadString(JsonElement element) => element.ValueKind switch
    {
        JsonValueKind.String => element.GetString() ?? string.Empty,
        _ => element.ToString() ?? string.Empty
    };

    private static string ExtractJsonObject(string content)
    {
        var trimmed = content.Trim();
        var start = trimmed.IndexOf('{');
        var end = trimmed.LastIndexOf('}');
        if (start >= 0 && end > start)
        {
            return trimmed[start..(end + 1)];
        }

        return trimmed;
    }

    private static string RenderTail(IReadOnlyList<AgentConversationMessage> tail, int maxChars)
    {
        var builder = new System.Text.StringBuilder();
        foreach (var message in tail.TakeLast(24))
        {
            builder.AppendLine($"{message.Role}: {message.Content}");
            if (builder.Length >= maxChars)
            {
                break;
            }
        }

        var text = builder.ToString();
        return text.Length <= maxChars ? text : string.Concat(text.AsSpan(0, maxChars), "…");
    }
}
