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

    private static readonly HashSet<string> StructuralTools = new(StringComparer.OrdinalIgnoreCase)
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

            if (StructuralTools.Contains(execution.ToolName))
            {
                set.Add(AgentEvidenceKind.StructuralSchema);
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
        IReadOnlyList<AgentEvidenceKind> kinds)
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
            if (wantStructural && StructuralTools.Contains(tool.Name))
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

        return list.Count > 0 ? list : catalog;
    }
}
