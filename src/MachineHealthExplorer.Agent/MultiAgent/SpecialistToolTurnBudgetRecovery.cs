using MachineHealthExplorer.Agent.Models;
using MachineHealthExplorer.Agent.Services;

namespace MachineHealthExplorer.Agent.MultiAgent;

internal static class SpecialistToolTurnBudgetRecovery
{
    /// <summary>
    /// Generic tools that can continue a partially explored dataset analysis without domain-specific branching.
    /// </summary>
    public static readonly HashSet<string> EvidenceContinuationToolNames = new(StringComparer.OrdinalIgnoreCase)
    {
        "get_schema",
        "search_columns",
        "group_and_aggregate",
        "profile_columns",
        "query_rows",
        "get_distinct_values"
    };

    public static bool ShouldNarrowToEvidenceContinuationSurface(
        AgentTaskRequest request,
        IReadOnlyList<AgentToolExecutionRecord> executedTools)
        => request.ExpectsDatasetQueryEvidence
           && SpecialistDatasetEvidencePolicy.AllowlistOffersDatasetQueryEvidence(request.AllowedTools)
           && !SpecialistDatasetEvidencePolicy.HasSuccessfulDatasetQueryEvidence(executedTools);

    public static string[] IntersectAllowedWithEvidenceContinuation(IEnumerable<string> allowedNames)
    {
        var narrowed = allowedNames
            .Where(name => EvidenceContinuationToolNames.Contains(name))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray();
        return narrowed.Length > 0 ? narrowed : allowedNames.Distinct(StringComparer.OrdinalIgnoreCase).ToArray();
    }

    public static string BuildExecutedToolsSummary(IReadOnlyList<AgentToolExecutionRecord> executedTools)
    {
        if (executedTools.Count == 0)
        {
            return "(none)";
        }

        return string.Join(
            "; ",
            executedTools.Select(execution =>
                $"{execution.ToolName}: {(execution.IsError ? "error" : "ok")}"));
    }

    public static List<AgentConversationMessage> BuildRecoveryScratchConversation(
        string specialistPrimaryUserContent,
        IReadOnlyList<AgentToolExecutionRecord> executedTools,
        bool useMinimalToolSchemas)
    {
        var list = new List<AgentConversationMessage>
        {
            new()
            {
                Role = AgentConversationRole.User,
                Content = specialistPrimaryUserContent
            }
        };

        if (!useMinimalToolSchemas)
        {
            list.Add(new AgentConversationMessage
            {
                Role = AgentConversationRole.User,
                Content = MultiAgentPromptBuilder.BuildMinimalToolParametersContractHint()
            });
        }

        list.Add(new AgentConversationMessage
        {
            Role = AgentConversationRole.User,
            Content = $"""
Context recovery (scratch transcript; do not assume any hidden reasoning survived).

Executed tools so far (compact): {BuildExecutedToolsSummary(executedTools)}

Instruction: emit exactly one valid tool call using the exposed tools, or reply with exactly this single line and nothing else:
DONE_NO_MORE_TOOLS
"""
        });

        var recapBudget = 1400;
        foreach (var execution in executedTools)
        {
            var recapBody = execution.IsError
                ? AgentPromptBudgetGuard.CompactPlain(execution.ResultJson ?? "{}", recapBudget)
                : AgentToolEvidenceCompressor.BuildToolMessageContent(
                    execution.ToolName,
                    execution.ResultJson ?? "{}",
                    recapBudget);

            list.Add(new AgentConversationMessage
            {
                Role = AgentConversationRole.User,
                Content = $"Evidence recap ({execution.ToolName}):\n{recapBody}"
            });
        }

        return list;
    }
}
