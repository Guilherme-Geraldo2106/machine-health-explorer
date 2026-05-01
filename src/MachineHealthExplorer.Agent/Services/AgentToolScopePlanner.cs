using MachineHealthExplorer.Agent.Abstractions;
using MachineHealthExplorer.Agent.Models;
using System.Text.Json;

namespace MachineHealthExplorer.Agent.Services;

internal sealed class AgentToolScopePlanner
{
    private readonly AgentOptions _options;
    private readonly IAgentChatClient _chatClient;

    public AgentToolScopePlanner(AgentOptions options, IAgentChatClient chatClient)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _chatClient = chatClient ?? throw new ArgumentNullException(nameof(chatClient));
    }

    public async Task<IReadOnlyList<AgentToolDefinition>> SelectToolsAsync(
        string model,
        IReadOnlyList<AgentConversationMessage> conversationTail,
        AgentConversationMemory memory,
        IReadOnlyList<AgentToolDefinition> allTools,
        CancellationToken cancellationToken)
    {
        if (!_options.EnableDynamicToolScoping || allTools.Count == 0)
        {
            return allTools.ToArray();
        }

        if (!_options.EnableToolPlannerPass)
        {
            return HeuristicSelection(memory, conversationTail, allTools);
        }

        var catalog = string.Join(
            "\n",
            allTools.Take(_options.ToolPlannerMaxNamedTools).Select(tool => $"- {tool.Name}: {tool.Description}"));

        var systemPrompt = """
You are a routing planner for a dataset assistant.
The dataset is already loaded in the host process and is queryable exclusively through the tool catalog below. Never assume the dataset is missing or unloaded.

Pick the smallest set of tools needed for the NEXT model step, or none if the next step should be a normal assistant answer using existing evidence.

Return ONLY JSON with this exact shape:
{"need_tools":true|false,"tools":["tool_name"],"reason":"short"}
Rules:
- Do not invent tool names.
- If the user is asking for interpretation/meaning and the transcript already includes tool evidence, prefer need_tools=false.
- If the user needs new factual dataset values (counts, min/max/mean, filters, column discovery, labels/events, etc.), set need_tools=true and include only necessary tools.
- In "reason", name a specific dataset column only if the user or transcript already identified it; do not guess between similarly named columns when multiple exist.
- When the transcript shows multiple plausible numeric columns for the same concept (often after describe_dataset), keep need_tools=true and prefer tools that let the assistant compute extrema per column (e.g. query_rows with sortRules and pageSize=1) without assuming which column applies.
""";

        var userPrompt = $"""
Tool catalog:
{catalog}

Session memory (may be empty):
{AgentEphemeralWorkerRunner.FormatMemoryBlock(memory)}

Recent conversation tail:
{RenderTail(conversationTail, maxChars: 6000)}
""";

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
                MaxOutputTokens = _options.ToolPlannerMaxOutputTokens,
                EnableTools = false
            }, cancellationToken).ConfigureAwait(false);
        }
        catch (AgentModelBackendException ex) when (ex.IsContextLengthExceeded)
        {
            return HeuristicSelection(memory, conversationTail, allTools);
        }

        var plannerSurface = CombinePlannerSurface(response.Content, response.ReasoningContent);
        var surfaceEmpty = string.IsNullOrWhiteSpace(plannerSurface);
        var truncatedPlanner = AgentFinishReason.IsTruncated(response.FinishReason);
        var parseOk = TryParsePlannerSelection(plannerSurface, allTools, out var needTools, out var selected);

        if (truncatedPlanner || surfaceEmpty || !parseOk)
        {
            return HeuristicSelection(memory, conversationTail, allTools);
        }

        if (needTools)
        {
            if (selected.Count > 0)
            {
                return selected;
            }

            return LooksNumericOrAggregationHeavy(GetLastUserText(conversationTail))
                ? MinimalDataToolkit(allTools)
                : Array.Empty<AgentToolDefinition>();
        }

        var lastUser = GetLastUserText(conversationTail);
        if (!LooksInterpretive(lastUser)
            && LooksNumericOrAggregationHeavy(lastUser)
            && !HasNumericDatasetEvidence(memory, conversationTail))
        {
            return MinimalDataToolkit(allTools);
        }

        return Array.Empty<AgentToolDefinition>();
    }

    private IReadOnlyList<AgentToolDefinition> HeuristicSelection(
        AgentConversationMemory memory,
        IReadOnlyList<AgentConversationMessage> tail,
        IReadOnlyList<AgentToolDefinition> allTools)
    {
        var lastUser = tail.LastOrDefault(message => message.Role == AgentConversationRole.User)?.Content ?? string.Empty;
        if (LooksInterpretive(lastUser) && HasToolEvidence(memory, tail))
        {
            return Array.Empty<AgentToolDefinition>();
        }

        if (LooksDataHeavy(lastUser))
        {
            return MinimalDataToolkit(allTools);
        }

        return allTools.ToArray();
    }

    private static string GetLastUserText(IReadOnlyList<AgentConversationMessage> tail)
        => tail.LastOrDefault(message => message.Role == AgentConversationRole.User)?.Content ?? string.Empty;

    internal static string CombinePlannerSurface(string? content, string? reasoningContent)
    {
        if (!string.IsNullOrWhiteSpace(content))
        {
            return content!;
        }

        return reasoningContent?.Trim() ?? string.Empty;
    }

    private static readonly string[] NumericDatasetEvidenceTools =
    [
        "group_and_aggregate",
        "query_rows",
        "profile_columns",
        "get_distinct_values"
    ];

    private static bool HasNumericDatasetEvidence(AgentConversationMemory memory, IReadOnlyList<AgentConversationMessage> tail)
    {
        foreach (var digest in memory.ToolEvidenceDigests)
        {
            if (NumericDatasetEvidenceTools.Any(tool =>
                    digest.ToolName.Equals(tool, StringComparison.OrdinalIgnoreCase)))
            {
                return true;
            }
        }

        return tail.Any(message =>
            message.Role == AgentConversationRole.Tool
            && NumericDatasetEvidenceTools.Any(tool =>
                string.Equals(message.Name, tool, StringComparison.OrdinalIgnoreCase)));
    }

    private static bool LooksNumericOrAggregationHeavy(string text)
    {
        if (string.IsNullOrWhiteSpace(text))
        {
            return false;
        }

        var lower = text.ToLowerInvariant();
        return lower.Contains("máximo", StringComparison.Ordinal)
               || lower.Contains("maximo", StringComparison.Ordinal)
               || lower.Contains("mínimo", StringComparison.Ordinal)
               || lower.Contains("minimo", StringComparison.Ordinal)
               || lower.Contains("maior", StringComparison.Ordinal)
               || lower.Contains("menor", StringComparison.Ordinal)
               || lower.Contains("momento", StringComparison.Ordinal)
               || lower.Contains("celsius", StringComparison.Ordinal)
               || lower.Contains("kelvin", StringComparison.Ordinal)
               || lower.Contains("count", StringComparison.Ordinal)
               || lower.Contains("quantos", StringComparison.Ordinal)
               || lower.Contains("quantas", StringComparison.Ordinal)
               || lower.Contains("média", StringComparison.Ordinal)
               || lower.Contains(" mean ", StringComparison.Ordinal)
               || lower.StartsWith("mean ", StringComparison.Ordinal)
               || lower.Contains("mean?", StringComparison.Ordinal)
               || lower.Contains("median", StringComparison.Ordinal)
               || lower.Contains("soma", StringComparison.Ordinal)
               || lower.Contains("agreg", StringComparison.Ordinal)
               || lower.Contains("aggregate", StringComparison.Ordinal)
               || lower.Contains("histogram", StringComparison.Ordinal)
               || lower.Contains("distrib", StringComparison.Ordinal)
               || lower.Contains("temperatura", StringComparison.Ordinal)
               || lower.Contains("temperature", StringComparison.Ordinal)
               || lower.Contains("torque", StringComparison.Ordinal)
               || lower.Contains(" max ", StringComparison.Ordinal)
               || lower.StartsWith("max ", StringComparison.Ordinal)
               || lower.Contains(" max?", StringComparison.Ordinal)
               || lower.Contains(" min ", StringComparison.Ordinal)
               || lower.StartsWith("min ", StringComparison.Ordinal)
               || lower.Contains(" min?", StringComparison.Ordinal)
               || lower.Contains("max:", StringComparison.Ordinal)
               || lower.Contains("min:", StringComparison.Ordinal);
    }

    private static bool HasToolEvidence(AgentConversationMemory memory, IReadOnlyList<AgentConversationMessage> tail)
    {
        if (memory.ToolEvidenceDigests.Count > 0 || memory.ConfirmedFacts.Count > 0)
        {
            return true;
        }

        return tail.Any(message => message.Role == AgentConversationRole.Tool);
    }

    private static bool LooksInterpretive(string text)
    {
        if (string.IsNullOrWhiteSpace(text))
        {
            return false;
        }

        var lower = text.ToLowerInvariant();
        return lower.Contains("significa", StringComparison.Ordinal)
               || lower.Contains("meaning", StringComparison.Ordinal)
               || lower.Contains("explain", StringComparison.Ordinal)
               || lower.Contains("por que", StringComparison.Ordinal)
               || lower.Contains("porque", StringComparison.Ordinal)
               || lower.Contains("o que isso", StringComparison.Ordinal);
    }

    private static bool LooksDataHeavy(string text)
    {
        if (string.IsNullOrWhiteSpace(text))
        {
            return false;
        }

        var lower = text.ToLowerInvariant();
        return lower.Contains("schema", StringComparison.Ordinal)
               || lower.Contains("count", StringComparison.Ordinal)
               || lower.Contains("quantos", StringComparison.Ordinal)
               || lower.Contains("quantas", StringComparison.Ordinal)
               || lower.Contains("rows", StringComparison.Ordinal)
               || lower.Contains("filter", StringComparison.Ordinal)
               || lower.Contains("average", StringComparison.Ordinal)
               || lower.Contains("mean", StringComparison.Ordinal)
               || lower.Contains("median", StringComparison.Ordinal)
               || lower.Contains("media", StringComparison.Ordinal)
               || lower.Contains("média", StringComparison.Ordinal)
               || lower.Contains("máximo", StringComparison.Ordinal)
               || lower.Contains("maximo", StringComparison.Ordinal)
               || lower.Contains("mínimo", StringComparison.Ordinal)
               || lower.Contains("minimo", StringComparison.Ordinal)
               || lower.Contains(" max ", StringComparison.Ordinal)
               || lower.StartsWith("max ", StringComparison.Ordinal)
               || lower.Contains(" max?", StringComparison.Ordinal)
               || lower.Contains(" min ", StringComparison.Ordinal)
               || lower.StartsWith("min ", StringComparison.Ordinal)
               || lower.Contains(" min?", StringComparison.Ordinal)
               || lower.Contains("max:", StringComparison.Ordinal)
               || lower.Contains("min:", StringComparison.Ordinal)
               || lower.Contains("maximum", StringComparison.Ordinal)
               || lower.Contains("minimum", StringComparison.Ordinal)
               || lower.Contains("temperatura", StringComparison.Ordinal)
               || lower.Contains("temperature", StringComparison.Ordinal)
               || lower.Contains("torque", StringComparison.Ordinal)
               || lower.Contains("falha", StringComparison.Ordinal)
               || lower.Contains("failure", StringComparison.Ordinal)
               || lower.Contains("failures", StringComparison.Ordinal)
               || lower.Contains("taxa", StringComparison.Ordinal)
               || lower.Contains("rate", StringComparison.Ordinal)
               || lower.Contains("histogram", StringComparison.Ordinal)
               || lower.Contains("distrib", StringComparison.Ordinal)
               || lower.Contains("aggregate", StringComparison.Ordinal)
               || lower.Contains("agrup", StringComparison.Ordinal)
               || lower.Contains("sum", StringComparison.Ordinal)
               || lower.Contains("total", StringComparison.Ordinal)
               || lower.Contains("dataset", StringComparison.Ordinal)
               || lower.Contains("column", StringComparison.Ordinal)
               || lower.Contains("coluna", StringComparison.Ordinal)
               || lower.Contains("consulta", StringComparison.Ordinal)
               || lower.Contains("dados", StringComparison.Ordinal);
    }

    private static IReadOnlyList<AgentToolDefinition> MinimalDataToolkit(IReadOnlyList<AgentToolDefinition> allTools)
    {
        var preferred = new[]
        {
            "describe_dataset",
            "get_schema",
            "get_distinct_values",
            "group_and_aggregate",
            "profile_columns",
            "query_rows",
            "search_columns"
        };

        var selected = new List<AgentToolDefinition>();
        foreach (var name in preferred)
        {
            var match = allTools.FirstOrDefault(tool => tool.Name.Equals(name, StringComparison.OrdinalIgnoreCase));
            if (match is not null && !string.IsNullOrWhiteSpace(match.Name))
            {
                selected.Add(match);
            }
        }

        return selected.Count > 0 ? selected : allTools.Take(Math.Min(4, allTools.Count)).ToArray();
    }

    private static bool TryParsePlannerSelection(
        string? plannerJson,
        IReadOnlyList<AgentToolDefinition> allTools,
        out bool needTools,
        out IReadOnlyList<AgentToolDefinition> selected)
    {
        needTools = false;
        selected = Array.Empty<AgentToolDefinition>();

        if (string.IsNullOrWhiteSpace(plannerJson))
        {
            return false;
        }

        try
        {
            var json = ExtractJsonObject(plannerJson);
            using var document = JsonDocument.Parse(json);
            var root = document.RootElement;
            if (!root.TryGetProperty("need_tools", out var needToolsElement))
            {
                return false;
            }

            if (needToolsElement.ValueKind != JsonValueKind.True && needToolsElement.ValueKind != JsonValueKind.False)
            {
                return false;
            }

            needTools = needToolsElement.ValueKind == JsonValueKind.True;
            if (!needTools)
            {
                return true;
            }

            if (!root.TryGetProperty("tools", out var toolsElement) || toolsElement.ValueKind != JsonValueKind.Array)
            {
                return false;
            }

            var catalogNames = allTools.Select(tool => tool.Name).ToArray();
            var list = new List<AgentToolDefinition>();
            foreach (var element in toolsElement.EnumerateArray())
            {
                var name = element.GetString();
                if (string.IsNullOrWhiteSpace(name))
                {
                    continue;
                }

                var resolved = AgentToolInvocationCanonicalizer.TryResolveRegisteredName(name.Trim(), catalogNames);
                var match = resolved is null
                    ? null
                    : allTools.FirstOrDefault(tool => tool.Name.Equals(resolved, StringComparison.OrdinalIgnoreCase));
                if (match is not null && !string.IsNullOrWhiteSpace(match.Name))
                {
                    list.Add(match);
                }
            }

            selected = list.DistinctBy(tool => tool.Name, StringComparer.OrdinalIgnoreCase).ToArray();
            return true;
        }
        catch
        {
            needTools = false;
            selected = Array.Empty<AgentToolDefinition>();
            return false;
        }
    }

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
