using MachineHealthExplorer.Agent.Models;
using MachineHealthExplorer.Agent.MultiAgent;

namespace MachineHealthExplorer.Tests;

public sealed class SpecialistDatasetEvidenceGapTests
{
    [Fact]
    public void BuildTechnicalEvidenceGapMessages_IncludesMissingKinds()
    {
        var request = new AgentTaskRequest(
            AgentSpecialistKind.QueryAnalysis,
            "q",
            "r",
            new AgentConversationMemory(),
            Array.Empty<AgentConversationMessage>(),
            Array.Empty<AgentToolDefinition>(),
            "m",
            "sys",
            ExpectsDatasetQueryEvidence: true,
            RequiredEvidenceKinds: [AgentEvidenceKind.Aggregate, AgentEvidenceKind.Profile]);

        var gaps = SpecialistDatasetEvidencePolicy.BuildTechnicalEvidenceGapMessages(
            request,
            Array.Empty<AgentToolExecutionRecord>(),
            includeStructuralSupplementalHints: true);

        Assert.Contains("missing aggregate evidence", gaps, StringComparer.Ordinal);
        Assert.Contains("missing numeric distribution/profile evidence", gaps, StringComparer.Ordinal);
    }

    [Fact]
    public void BuildTechnicalEvidenceGapMessages_SupplementalWhenAggregateShallow()
    {
        var request = new AgentTaskRequest(
            AgentSpecialistKind.QueryAnalysis,
            "q",
            "r",
            new AgentConversationMemory(),
            Array.Empty<AgentConversationMessage>(),
            Array.Empty<AgentToolDefinition>(),
            "m",
            "sys",
            ExpectsDatasetQueryEvidence: true);

        var shallowAgg = new AgentToolExecutionRecord
        {
            ToolName = "group_and_aggregate",
            IsError = false,
            ArgumentsJson = """{"groupByColumns":["dim_a"],"aggregations":[{"alias":"row_count","function":"Count"}]}""",
            ResultJson = "{}"
        };

        var gaps = SpecialistDatasetEvidencePolicy.BuildTechnicalEvidenceGapMessages(
            request,
            [shallowAgg],
            includeStructuralSupplementalHints: true);

        Assert.Contains("missing derived rate evidence", gaps, StringComparer.Ordinal);
        Assert.Contains("missing multi-factor grouping evidence", gaps, StringComparer.Ordinal);
    }
}
