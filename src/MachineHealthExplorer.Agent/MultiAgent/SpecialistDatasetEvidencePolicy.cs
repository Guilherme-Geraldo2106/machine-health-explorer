using System.Text.Json;
using MachineHealthExplorer.Agent.Models;

namespace MachineHealthExplorer.Agent.MultiAgent;

internal static class SpecialistDatasetEvidencePolicy
{
    private static readonly AgentEvidenceKind[] AllGenericTabularKinds =
    [
        AgentEvidenceKind.Profile,
        AgentEvidenceKind.Aggregate,
        AgentEvidenceKind.DistinctValues,
        AgentEvidenceKind.RowSample
    ];

    /// <summary>
    /// Tools that can contribute structural evidence when the schema is still unknown.
    /// </summary>
    internal static readonly HashSet<string> StructuralDiscoveryToolNames = new(StringComparer.OrdinalIgnoreCase)
    {
        "describe_dataset",
        "get_schema",
        "search_columns"
    };

    private static readonly HashSet<string> LegacyDatasetQueryEvidenceTools = new(StringComparer.OrdinalIgnoreCase)
    {
        "group_and_aggregate",
        "query_rows",
        "profile_columns",
        "get_distinct_values"
    };

    public static bool AllowlistOffersDatasetQueryEvidence(IReadOnlyList<AgentToolDefinition> allowedTools)
    {
        foreach (var tool in allowedTools)
        {
            if (LegacyDatasetQueryEvidenceTools.Contains(tool.Name))
            {
                return true;
            }
        }

        return false;
    }

    public static bool HasSuccessfulLegacyDatasetQueryEvidence(IReadOnlyList<AgentToolExecutionRecord> executions)
    {
        foreach (var execution in executions)
        {
            if (execution.IsError)
            {
                continue;
            }

            if (LegacyDatasetQueryEvidenceTools.Contains(execution.ToolName))
            {
                return true;
            }
        }

        return false;
    }

    public static HashSet<AgentEvidenceKind> GetSatisfiedEvidenceKinds(IReadOnlyList<AgentToolExecutionRecord> executions)
    {
        var set = new HashSet<AgentEvidenceKind>();
        foreach (var execution in executions)
        {
            if (execution.IsError)
            {
                continue;
            }

            if (string.Equals(execution.ToolName, "search_columns", StringComparison.OrdinalIgnoreCase))
            {
                if (SearchColumnsHasUsefulStructuralMatches(execution.ResultJson))
                {
                    set.Add(AgentEvidenceKind.StructuralSchema);
                }

                continue;
            }

            if (string.Equals(execution.ToolName, "get_schema", StringComparison.OrdinalIgnoreCase)
                || string.Equals(execution.ToolName, "describe_dataset", StringComparison.OrdinalIgnoreCase))
            {
                set.Add(AgentEvidenceKind.StructuralSchema);
                continue;
            }

            if (string.Equals(execution.ToolName, "profile_columns", StringComparison.OrdinalIgnoreCase))
            {
                set.Add(AgentEvidenceKind.Profile);
            }

            if (string.Equals(execution.ToolName, "get_distinct_values", StringComparison.OrdinalIgnoreCase))
            {
                set.Add(AgentEvidenceKind.DistinctValues);
            }

            if (string.Equals(execution.ToolName, "query_rows", StringComparison.OrdinalIgnoreCase))
            {
                set.Add(AgentEvidenceKind.RowSample);
            }

            if (string.Equals(execution.ToolName, "group_and_aggregate", StringComparison.OrdinalIgnoreCase))
            {
                set.Add(AgentEvidenceKind.Aggregate);
            }
        }

        return set;
    }

    public static IReadOnlyList<AgentEvidenceKind> GetMissingRequiredEvidenceKinds(
        AgentTaskRequest request,
        IReadOnlyList<AgentToolExecutionRecord> executions)
    {
        var satisfied = GetSatisfiedEvidenceKinds(executions);
        if (request.RequiredEvidenceKinds is { Count: > 0 } required)
        {
            return required.Where(kind => !satisfied.Contains(kind)).Distinct().ToArray();
        }

        if (!request.ExpectsDatasetQueryEvidence)
        {
            return Array.Empty<AgentEvidenceKind>();
        }

        if (!AllowlistOffersDatasetQueryEvidence(request.AllowedTools))
        {
            return Array.Empty<AgentEvidenceKind>();
        }

        if (HasSuccessfulLegacyDatasetQueryEvidence(executions))
        {
            return Array.Empty<AgentEvidenceKind>();
        }

        return AllGenericTabularKinds;
    }

    public static bool ShouldPromptForMoreDatasetQueryEvidence(
        AgentTaskRequest request,
        IReadOnlyList<AgentToolExecutionRecord> executions)
        => GetMissingRequiredEvidenceKinds(request, executions).Count > 0;

    public static IReadOnlyList<AgentToolDefinition> FilterToolsSatisfyingAnyKind(
        IReadOnlyList<AgentToolDefinition> catalog,
        IReadOnlyList<AgentEvidenceKind> kinds,
        IReadOnlyList<AgentToolExecutionRecord>? executions = null)
    {
        if (kinds.Count == 0)
        {
            return catalog;
        }

        var wantStructural = kinds.Contains(AgentEvidenceKind.StructuralSchema);
        var wantProfile = kinds.Contains(AgentEvidenceKind.Profile);
        var wantDistinct = kinds.Contains(AgentEvidenceKind.DistinctValues);
        var wantRows = kinds.Contains(AgentEvidenceKind.RowSample);
        var wantAgg = kinds.Contains(AgentEvidenceKind.Aggregate);

        var list = new List<AgentToolDefinition>();
        foreach (var tool in catalog)
        {
            if (wantStructural && StructuralDiscoveryToolNames.Contains(tool.Name))
            {
                list.Add(tool);
                continue;
            }

            if (wantProfile && string.Equals(tool.Name, "profile_columns", StringComparison.OrdinalIgnoreCase))
            {
                list.Add(tool);
                continue;
            }

            if (wantDistinct && string.Equals(tool.Name, "get_distinct_values", StringComparison.OrdinalIgnoreCase))
            {
                list.Add(tool);
                continue;
            }

            if (wantRows && string.Equals(tool.Name, "query_rows", StringComparison.OrdinalIgnoreCase))
            {
                list.Add(tool);
                continue;
            }

            if (wantAgg && string.Equals(tool.Name, "group_and_aggregate", StringComparison.OrdinalIgnoreCase))
            {
                list.Add(tool);
            }
        }

        if (executions is not null
            && wantAgg
            && !GetSatisfiedEvidenceKinds(executions).Contains(AgentEvidenceKind.StructuralSchema))
        {
            var names = new HashSet<string>(list.Select(t => t.Name), StringComparer.OrdinalIgnoreCase);
            foreach (var tool in catalog)
            {
                if (StructuralDiscoveryToolNames.Contains(tool.Name) && names.Add(tool.Name))
                {
                    list.Add(tool);
                }
            }
        }

        return list.Count > 0 ? list : catalog;
    }

    /// <summary>
    /// When the planner narrows the exposed tool list to aggregate-only but schema/columns are not yet evidenced,
    /// merge structural discovery tools from the specialist allowlist so the model can run get_schema/search_columns first.
    /// </summary>
    public static IReadOnlyList<AgentToolDefinition> EnsureStructuralSurfaceWhenSchemaUnsatisfied(
        IReadOnlyList<AgentToolDefinition> scopedCatalog,
        IReadOnlyList<AgentToolDefinition> chosenForTurn,
        IReadOnlyList<AgentEvidenceKind> missingKinds,
        IReadOnlyList<AgentToolExecutionRecord> executions)
    {
        if (GetSatisfiedEvidenceKinds(executions).Contains(AgentEvidenceKind.StructuralSchema))
        {
            return chosenForTurn;
        }

        var mustExposeStructural =
            missingKinds.Contains(AgentEvidenceKind.StructuralSchema)
            || missingKinds.Contains(AgentEvidenceKind.Aggregate);
        if (!mustExposeStructural)
        {
            return chosenForTurn;
        }

        var names = new HashSet<string>(chosenForTurn.Select(t => t.Name), StringComparer.OrdinalIgnoreCase);
        var merged = chosenForTurn.ToList();
        foreach (var tool in scopedCatalog)
        {
            if (StructuralDiscoveryToolNames.Contains(tool.Name) && names.Add(tool.Name))
            {
                merged.Add(tool);
            }
        }

        return merged;
    }

    private static bool SearchColumnsHasUsefulStructuralMatches(string? resultJson)
    {
        if (string.IsNullOrWhiteSpace(resultJson))
        {
            return false;
        }

        try
        {
            using var document = JsonDocument.Parse(resultJson);
            if (!document.RootElement.TryGetProperty("matches", out var matches)
                || matches.ValueKind != JsonValueKind.Array)
            {
                return false;
            }

            return matches.GetArrayLength() > 0;
        }
        catch
        {
            return false;
        }
    }

    /// <summary>
    /// Generic, model-visible gap phrases derived from tool names/JSON shapes only (no user-text heuristics).
    /// </summary>
    public static IReadOnlyList<string> BuildTechnicalEvidenceGapMessages(
        AgentTaskRequest request,
        IReadOnlyList<AgentToolExecutionRecord> executions,
        bool includeStructuralSupplementalHints)
    {
        var gaps = new List<string>();
        foreach (var missing in GetMissingRequiredEvidenceKinds(request, executions))
        {
            gaps.Add(missing switch
            {
                AgentEvidenceKind.Aggregate => "missing aggregate evidence",
                AgentEvidenceKind.Profile => "missing numeric distribution/profile evidence",
                AgentEvidenceKind.DistinctValues => "missing distinct-value cardinality evidence",
                AgentEvidenceKind.RowSample => "missing row-sample evidence",
                AgentEvidenceKind.StructuralSchema => "missing schema/column listing evidence",
                _ => $"missing evidence kind:{missing}"
            });
        }

        if (!includeStructuralSupplementalHints
            || !request.ExpectsDatasetQueryEvidence
            || request.SpecialistKind == AgentSpecialistKind.Discovery)
        {
            return gaps.Distinct(StringComparer.Ordinal).ToArray();
        }

        if (HasSuccessfulGroupAndAggregateExecution(executions))
        {
            if (!AnyGroupAndAggregateArgumentsIncludeDerivedMetrics(executions))
            {
                gaps.Add("missing derived rate evidence");
            }

            if (!AnyGroupAndAggregateArgumentsSuggestMultiKeyGrouping(executions))
            {
                gaps.Add("missing multi-factor grouping evidence");
            }
        }

        return gaps.Distinct(StringComparer.Ordinal).ToArray();
    }

    private static bool HasSuccessfulGroupAndAggregateExecution(IReadOnlyList<AgentToolExecutionRecord> executions)
    {
        foreach (var execution in executions)
        {
            if (!execution.IsError
                && string.Equals(execution.ToolName, "group_and_aggregate", StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }
        }

        return false;
    }

    private static bool AnyGroupAndAggregateArgumentsIncludeDerivedMetrics(IReadOnlyList<AgentToolExecutionRecord> executions)
    {
        foreach (var execution in executions)
        {
            if (execution.IsError
                || !string.Equals(execution.ToolName, "group_and_aggregate", StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            if (GroupAndAggregateArgumentsIncludeDerivedMetrics(execution.ArgumentsJson))
            {
                return true;
            }
        }

        return false;
    }

    private static bool GroupAndAggregateArgumentsIncludeDerivedMetrics(string? argumentsJson)
    {
        if (string.IsNullOrWhiteSpace(argumentsJson))
        {
            return false;
        }

        try
        {
            using var document = JsonDocument.Parse(argumentsJson);
            if (!TryGetPropertyInsensitive(document.RootElement, "derivedMetrics", out var derived)
                || derived.ValueKind != JsonValueKind.Array)
            {
                return false;
            }

            return derived.GetArrayLength() > 0;
        }
        catch (JsonException)
        {
            return false;
        }
    }

    private static bool AnyGroupAndAggregateArgumentsSuggestMultiKeyGrouping(IReadOnlyList<AgentToolExecutionRecord> executions)
    {
        foreach (var execution in executions)
        {
            if (execution.IsError
                || !string.Equals(execution.ToolName, "group_and_aggregate", StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            if (GroupAndAggregateArgumentsSuggestMultiKeyGrouping(execution.ArgumentsJson))
            {
                return true;
            }
        }

        return false;
    }

    private static bool GroupAndAggregateArgumentsSuggestMultiKeyGrouping(string? argumentsJson)
    {
        if (string.IsNullOrWhiteSpace(argumentsJson))
        {
            return false;
        }

        try
        {
            using var document = JsonDocument.Parse(argumentsJson);
            var root = document.RootElement;
            var dimensions = 0;
            if (TryGetPropertyInsensitive(root, "groupByColumns", out var columns)
                && columns.ValueKind == JsonValueKind.Array)
            {
                dimensions += columns.GetArrayLength();
            }

            if (TryGetPropertyInsensitive(root, "groupByBins", out var bins)
                && bins.ValueKind == JsonValueKind.Array)
            {
                dimensions += bins.GetArrayLength();
            }

            if (TryGetPropertyInsensitive(root, "groupByAutoBins", out var autoBins)
                && autoBins.ValueKind == JsonValueKind.Array)
            {
                dimensions += autoBins.GetArrayLength();
            }

            return dimensions >= 2;
        }
        catch (JsonException)
        {
            return false;
        }
    }

    private static bool TryGetPropertyInsensitive(JsonElement obj, string name, out JsonElement value)
    {
        foreach (var property in obj.EnumerateObject())
        {
            if (string.Equals(property.Name, name, StringComparison.OrdinalIgnoreCase))
            {
                value = property.Value;
                return true;
            }
        }

        value = default;
        return false;
    }
}
