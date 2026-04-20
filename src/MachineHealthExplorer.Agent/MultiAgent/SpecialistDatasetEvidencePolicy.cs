using MachineHealthExplorer.Agent.Models;

namespace MachineHealthExplorer.Agent.MultiAgent;

internal static class SpecialistDatasetEvidencePolicy
{
    private static readonly HashSet<string> DatasetQueryEvidenceTools = new(StringComparer.OrdinalIgnoreCase)
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
            if (DatasetQueryEvidenceTools.Contains(tool.Name))
            {
                return true;
            }
        }

        return false;
    }

    public static bool HasSuccessfulDatasetQueryEvidence(IReadOnlyList<AgentToolExecutionRecord> executions)
    {
        foreach (var execution in executions)
        {
            if (execution.IsError)
            {
                continue;
            }

            if (DatasetQueryEvidenceTools.Contains(execution.ToolName))
            {
                return true;
            }
        }

        return false;
    }

    public static bool ShouldPromptForMoreDatasetQueryEvidence(
        AgentTaskRequest request,
        IReadOnlyList<AgentToolExecutionRecord> executions)
    {
        if (!request.ExpectsDatasetQueryEvidence)
        {
            return false;
        }

        if (!AllowlistOffersDatasetQueryEvidence(request.AllowedTools))
        {
            return false;
        }

        if (HasSuccessfulDatasetQueryEvidence(executions))
        {
            return false;
        }

        return true;
    }
}
