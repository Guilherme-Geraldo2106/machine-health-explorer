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
    public void BuildTechnicalEvidenceGapMessages_SupplementalWhenDispatchImpliesRatesAndCombination_AndAggregateShallow()
    {
        var request = new AgentTaskRequest(
            AgentSpecialistKind.QueryAnalysis,
            "ranking by event rate across factor combinations",
            "Requires grouped rates and multi-variable combinations from aggregation.",
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
            ArgumentsJson = """{"groupByColumns":["dim_a","dim_b"],"aggregations":[{"alias":"row_count","function":"Count"}]}""",
            ResultJson = "{}"
        };

        var gaps = SpecialistDatasetEvidencePolicy.BuildTechnicalEvidenceGapMessages(
            request,
            [shallowAgg],
            includeStructuralSupplementalHints: true);

        Assert.Contains("missing derived rate evidence", gaps, StringComparer.Ordinal);
        Assert.Contains("existing aggregate only has absolute counts", gaps, StringComparer.Ordinal);
        Assert.DoesNotContain("missing multi-factor grouping evidence", gaps, StringComparer.Ordinal);
    }

    [Fact]
    public void EnsureAggregateRefinementToolOnSurface_AddsGroupWhenOnlyDistinctWasFilteredButShallowAggregateNeedsRefinement()
    {
        var getDistinct = new AgentToolDefinition { Name = "get_distinct_values", Description = "d", ParametersJsonSchema = "{}" };
        var group = new AgentToolDefinition { Name = "group_and_aggregate", Description = "g", ParametersJsonSchema = "{}" };
        var catalog = new[] { getDistinct, group };

        var request = new AgentTaskRequest(
            AgentSpecialistKind.QueryAnalysis,
            "compare proportions and combinations",
            "Needs rates across combined dimensions.",
            new AgentConversationMemory(),
            Array.Empty<AgentConversationMessage>(),
            Array.Empty<AgentToolDefinition>(),
            "m",
            "sys",
            ExpectsDatasetQueryEvidence: true,
            RequiredEvidenceKinds: [AgentEvidenceKind.DistinctValues]);

        var shallowAgg = new AgentToolExecutionRecord
        {
            ToolName = "group_and_aggregate",
            IsError = false,
            ArgumentsJson = """{"groupByColumns":["c1"],"aggregations":[{"alias":"n","function":"Count"}]}""",
            ResultJson = "{}"
        };

        var merged = SpecialistDatasetEvidencePolicy.EnsureAggregateRefinementToolOnSurface(
            catalog,
            new[] { getDistinct },
            request,
            [shallowAgg]);

        Assert.Contains(merged, t => t.Name.Equals("group_and_aggregate", StringComparison.OrdinalIgnoreCase));
    }
}
