using System.Text.Json;
using System.Text.Json.Serialization;
using MachineHealthExplorer.Agent.Abstractions;
using MachineHealthExplorer.Agent.Models;
using MachineHealthExplorer.Agent.Serialization;
using MachineHealthExplorer.Agent.Services;
using Microsoft.Extensions.Logging;

namespace MachineHealthExplorer.Agent.MultiAgent;

internal enum SpecialistToolSelectionPlannerStatus
{
    Success,
    InvalidOrEmptyJson,
    Truncated
}

internal readonly record struct SpecialistToolSelectionPlannerOutcome(
    SpecialistToolSelectionPlannerStatus Status,
    bool NeedTools,
    IReadOnlyList<string> ToolNames,
    string? Reason,
    string? ValidationDetail);

/// <summary>
/// Model-driven selection of which generic dataset tools to expose on the next specialist tool-enabled turn.
/// Uses only tool names and descriptions in prompts; never full JSON schemas.
/// </summary>
internal sealed class SpecialistToolSelectionPlanner
{
    private readonly AgentOptions _options;
    private readonly IAgentChatClient _chatClient;
    private readonly ILogger _logger;

    public SpecialistToolSelectionPlanner(AgentOptions options, IAgentChatClient chatClient, ILogger logger)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _chatClient = chatClient ?? throw new ArgumentNullException(nameof(chatClient));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public async Task<SpecialistToolSelectionPlannerOutcome> SelectToolsForNextStepAsync(
        string model,
        AgentSpecialistKind specialistKind,
        string userQuestion,
        string dispatchReason,
        string coordinatorMemoryCompact,
        string executedToolsSummary,
        int iteration,
        bool hasExecutedAnyTool,
        IReadOnlyList<(string Name, string Description)> compactToolCatalog,
        CancellationToken cancellationToken)
    {
        if (compactToolCatalog.Count == 0)
        {
            return new SpecialistToolSelectionPlannerOutcome(
                SpecialistToolSelectionPlannerStatus.Success,
                NeedTools: false,
                ToolNames: Array.Empty<string>(),
                Reason: null,
                "empty_catalog");
        }

        var multi = _options.MultiAgent;
        var maxAttempts = Math.Max(1, multi.SpecialistToolSelectionPlannerMaxRecoveryAttempts);
        string? lastError = null;

        for (var attempt = 0; attempt < maxAttempts; attempt++)
        {
            var isRecovery = attempt > 0;
            var maxOut = isRecovery
                ? Math.Min(
                    Math.Max(96, multi.SpecialistToolSelectionPlannerMaxOutputTokens),
                    Math.Max(96, multi.SpecialistToolSelectionPlannerRecoveryMaxOutputTokens))
                : Math.Max(96, multi.SpecialistToolSelectionPlannerMaxOutputTokens);

            var systemPrompt = BuildSystemPrompt(lastError);
            var userPrompt = isRecovery
                ? BuildCompactRecoveryUserPrompt(
                    specialistKind,
                    userQuestion,
                    dispatchReason,
                    coordinatorMemoryCompact,
                    executedToolsSummary,
                    iteration,
                    hasExecutedAnyTool,
                    compactToolCatalog,
                    lastError)
                : BuildPrimaryUserPrompt(
                    specialistKind,
                    userQuestion,
                    dispatchReason,
                    coordinatorMemoryCompact,
                    executedToolsSummary,
                    iteration,
                    hasExecutedAnyTool,
                    compactToolCatalog);

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
                    EnableTools = false,
                    Temperature = Math.Min(0.1, _options.WorkerTemperature),
                    MaxOutputTokens = maxOut,
                    ResponseFormat = _options.EnableStructuredJsonOutputs
                        ? new AgentJsonSchemaResponseFormat
                        {
                            Type = "json_schema",
                            Name = AgentStructuredOutputJsonSchemas.SpecialistToolSelectionSchemaName,
                            Strict = _options.UseStrictJsonSchemaInResponseFormat,
                            SchemaJson = AgentStructuredOutputJsonSchemas.SpecialistToolSelection.Trim()
                        }
                        : null
                }, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                _logger.LogWarning(
                    ex,
                    "Specialist tool-selection planner call failed specialist={Specialist} iteration={Iteration} attempt={Attempt}",
                    specialistKind,
                    iteration,
                    attempt + 1);
                lastError = $"planner_backend_error:{ex.GetType().Name}";
                continue;
            }

            if (AgentFinishReason.IsTruncated(response.FinishReason))
            {
                lastError =
                    $"truncated_finish_reason={response.FinishReason}";
                _logger.LogWarning(
                    "Specialist tool-selection planner truncated specialist={Specialist} iteration={Iteration} attempt={Attempt}",
                    specialistKind,
                    iteration,
                    attempt + 1);
                continue;
            }

            var surface = AgentToolScopePlanner.CombinePlannerSurface(response.Content, response.ReasoningContent);
            if (!TryParseAndValidate(surface, compactToolCatalog, out var needTools, out var names, out var plannerReason, out var parseError))
            {
                lastError = parseError ?? "unparseable_json";
                _logger.LogWarning(
                    "Specialist tool-selection planner invalid JSON specialist={Specialist} iteration={Iteration} attempt={Attempt} detail={Detail}",
                    specialistKind,
                    iteration,
                    attempt + 1,
                    lastError);
                continue;
            }

            if (needTools && names.Count == 0)
            {
                lastError = "need_tools_true_but_empty_tools";
                _logger.LogWarning(
                    "Specialist tool-selection planner empty tools when required specialist={Specialist} iteration={Iteration} attempt={Attempt}",
                    specialistKind,
                    iteration,
                    attempt + 1);
                continue;
            }

            return new SpecialistToolSelectionPlannerOutcome(
                SpecialistToolSelectionPlannerStatus.Success,
                needTools,
                names,
                Reason: plannerReason,
                ValidationDetail: null);
        }

        return new SpecialistToolSelectionPlannerOutcome(
            SpecialistToolSelectionPlannerStatus.InvalidOrEmptyJson,
            NeedTools: true,
            ToolNames: Array.Empty<string>(),
            Reason: null,
            lastError);
    }

    private static string BuildSystemPrompt(string? validationError)
    {
        const string jsonShape = """
{"need_tools":true|false,"tools":["tool_name"],"reason":"short"}
""";

        var recovery = string.IsNullOrWhiteSpace(validationError)
            ? string.Empty
            : $"""

The previous response failed validation:
{validationError.Trim()}

Fix now: output a single JSON object only (no markdown fences, no commentary) exactly like:
{jsonShape.Trim()}
""";

        return $"""
You are a tool-routing planner for a specialist agent that queries a generic tabular dataset through a fixed catalog of database-like tools.

Return ONLY JSON with this exact shape:
{jsonShape.Trim()}

Rules:
- Choose tools only for the immediate next specialist step.
- Do not choose tool arguments, column names, filters, bins, thresholds, or interpretations.
- Do not invent tool names; every entry in tools[] must match a name from the provided catalog list.
- If no new tool call is needed for the next step, return "need_tools": false (tools may be []).
- If new dataset-backed calls are needed, return "need_tools": true with the smallest sufficient tools[] set (order is not important).
- Keep reason under 120 characters; factual routing notes only.
{recovery}
""";
    }

    private static string BuildPrimaryUserPrompt(
        AgentSpecialistKind specialistKind,
        string userQuestion,
        string dispatchReason,
        string coordinatorMemoryCompact,
        string executedToolsSummary,
        int iteration,
        bool hasExecutedAnyTool,
        IReadOnlyList<(string Name, string Description)> compactToolCatalog)
    {
        var catalogLines = string.Join(
            "\n",
            compactToolCatalog.Select(entry => $"- {entry.Name}: {AgentPromptBudgetGuard.CompactPlain(entry.Description, 360)}"));

        return $"""
specialist={specialistKind}
iteration={iteration}
round_state={(hasExecutedAnyTool ? "after_tool_execution" : "initial")}

primary_question:
{AgentPromptBudgetGuard.CompactPlain(userQuestion, 1200)}

dispatch_reason:
{AgentPromptBudgetGuard.CompactPlain(dispatchReason, 600)}

coordinator_memory_compact:
{AgentPromptBudgetGuard.CompactPlain(coordinatorMemoryCompact, 900)}

executed_tools_summary:
{AgentPromptBudgetGuard.CompactPlain(executedToolsSummary, 700)}

allowed_tool_catalog_name_description_only:
{catalogLines}

Output the JSON object now.
""";
    }

    private static string BuildCompactRecoveryUserPrompt(
        AgentSpecialistKind specialistKind,
        string userQuestion,
        string dispatchReason,
        string coordinatorMemoryCompact,
        string executedToolsSummary,
        int iteration,
        bool hasExecutedAnyTool,
        IReadOnlyList<(string Name, string Description)> compactToolCatalog,
        string? validationError)
    {
        var catalogLines = string.Join(
            "\n",
            compactToolCatalog.Select(entry => $"- {entry.Name}"));

        return $"""
specialist={specialistKind}
iteration={iteration}
round_state={(hasExecutedAnyTool ? "after_tool_execution" : "initial")}

validation_error:
{AgentPromptBudgetGuard.CompactPlain(validationError ?? "unknown", 400)}

executed_tools_summary:
{AgentPromptBudgetGuard.CompactPlain(executedToolsSummary, 500)}

primary_question:
{AgentPromptBudgetGuard.CompactPlain(userQuestion, 600)}

dispatch_reason:
{AgentPromptBudgetGuard.CompactPlain(dispatchReason, 400)}

coordinator_memory_compact:
{AgentPromptBudgetGuard.CompactPlain(coordinatorMemoryCompact, 400)}

allowed_tool_names:
{catalogLines}

Return ONLY one JSON object with keys need_tools (boolean), tools (string array), reason (string). No markdown, no extra text.
""";
    }

    private static bool TryParseAndValidate(
        string? surface,
        IReadOnlyList<(string Name, string Description)> catalog,
        out bool needTools,
        out IReadOnlyList<string> toolNames,
        out string? plannerReason,
        out string? error)
    {
        needTools = false;
        toolNames = Array.Empty<string>();
        plannerReason = null;
        error = null;

        if (string.IsNullOrWhiteSpace(surface))
        {
            error = "empty_surface";
            return false;
        }

        var jsonText = ExtractJsonObject(surface);
        if (jsonText is null)
        {
            error = "no_json_object";
            return false;
        }

        SpecialistToolSelectionPlanDto? dto;
        try
        {
            dto = JsonSerializer.Deserialize<SpecialistToolSelectionPlanDto>(
                jsonText,
                new JsonSerializerOptions { PropertyNameCaseInsensitive = true });
        }
        catch (JsonException ex)
        {
            error = $"json_exception:{ex.Message}";
            return false;
        }

        if (dto is null)
        {
            error = "null_dto";
            return false;
        }

        needTools = dto.NeedTools;
        plannerReason = string.IsNullOrWhiteSpace(dto.Reason) ? null : dto.Reason.Trim();
        var resolved = new List<string>();
        foreach (var raw in dto.Tools ?? Array.Empty<string>())
        {
            if (string.IsNullOrWhiteSpace(raw))
            {
                continue;
            }

            var hit = catalog.FirstOrDefault(c => c.Name.Equals(raw.Trim(), StringComparison.OrdinalIgnoreCase));
            if (string.IsNullOrEmpty(hit.Name))
            {
                continue;
            }

            if (!resolved.Exists(n => n.Equals(hit.Name, StringComparison.OrdinalIgnoreCase)))
            {
                resolved.Add(hit.Name);
            }
        }

        if (needTools && resolved.Count == 0)
        {
            error = "no_valid_tool_names_for_need_tools_true";
            return false;
        }

        toolNames = resolved;
        return true;
    }

    private static string? ExtractJsonObject(string surface)
    {
        var trimmed = surface.Trim();
        var start = trimmed.IndexOf('{');
        if (start < 0)
        {
            return null;
        }

        var depth = 0;
        for (var i = start; i < trimmed.Length; i++)
        {
            var c = trimmed[i];
            if (c == '{')
            {
                depth++;
            }
            else if (c == '}')
            {
                depth--;
                if (depth == 0)
                {
                    return trimmed.Substring(start, i - start + 1);
                }
            }
        }

        return null;
    }

    private sealed class SpecialistToolSelectionPlanDto
    {
        [JsonPropertyName("need_tools")]
        public bool NeedTools { get; set; }

        [JsonPropertyName("tools")]
        public string[]? Tools { get; set; }

        [JsonPropertyName("reason")]
        public string? Reason { get; set; }
    }
}
