using MachineHealthExplorer.Agent.Abstractions;
using MachineHealthExplorer.Agent.Models;
using MachineHealthExplorer.Agent.MultiAgent;
using MachineHealthExplorer.Agent.Serialization;
using MachineHealthExplorer.Agent.Services;
using Microsoft.Extensions.Logging.Abstractions;

namespace MachineHealthExplorer.Tests;

public sealed class EvidencePolicyAndCoordinatorTests
{
    [Fact]
    public void SpecialistDatasetEvidencePolicy_ProfileDoesNotSatisfyAggregateRequirement()
    {
        var executions = new[]
        {
            new AgentToolExecutionRecord
            {
                ToolName = "profile_columns",
                ArgumentsJson = "{}",
                ResultJson = "{}",
                IsError = false
            }
        };

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
            RequiredEvidenceKinds: [AgentEvidenceKind.Aggregate]);

        var missing = SpecialistDatasetEvidencePolicy.GetMissingRequiredEvidenceKinds(request, executions);
        Assert.Single(missing);
        Assert.Equal(AgentEvidenceKind.Aggregate, missing[0]);
    }

    [Fact]
    public void CoordinatorDispatchJsonSchema_ContainsRequiredEvidenceArray()
    {
        Assert.Contains("required_evidence", AgentStructuredOutputJsonSchemas.CoordinatorDispatch, StringComparison.Ordinal);
    }

    [Fact]
    public void CoordinatorPrompt_ContainsGenericRankingComparisonRule()
    {
        var rules = MultiAgentPromptBuilder.BuildCoordinatorEvidenceRoutingRules();
        Assert.Contains("ranking", rules, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("comparison", rules, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("QueryAnalysis", rules, StringComparison.Ordinal);
        Assert.DoesNotContain("falha", rules, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("temperature", rules, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Coordinator_TryPlanAsync_ParsesRequiredEvidence_OnDispatchStep()
    {
        var options = new AgentOptions
        {
            Model = "m",
            MultiAgent = new MultiAgentOrchestrationOptions { EnableCoordinatorLlmPlanning = true }
        };

        var chat = new CoordinatorQueuingChatClient(
            new AgentModelResponse
            {
                Model = "m",
                Content =
                    """{"steps":[{"specialist":"QueryAnalysis","reason":"x","parallel_group":0,"required_evidence":["Aggregate","Profile"]}],"notes":"t"}""",
                FinishReason = "stop"
            });

        var coordinator = new CoordinatorAgent(options, chat, NullLogger.Instance);
        var result = await coordinator.TryPlanAsync(
                "m",
                "any",
                new AgentConversationMemory(),
                Array.Empty<AgentConversationMessage>(),
                CancellationToken.None);

        Assert.True(result.Success);
        Assert.Single(result.Plan.Steps);
        var kinds = result.Plan.Steps[0].RequiredEvidenceKinds;
        Assert.NotNull(kinds);
        Assert.Contains(AgentEvidenceKind.Aggregate, kinds);
        Assert.Contains(AgentEvidenceKind.Profile, kinds);
    }

    [Fact]
    public void FinalComposer_ReasoningAwareBudget_AllowsVisibleFloorPlusReasoningWhenContextFits()
    {
        var options = new AgentOptions
        {
            HostContextTokens = 24_000,
            ContextSlotTokens = 24_000,
            ContextSafetyMarginTokens = 384,
            MaxOutputTokens = 4096,
            MinAssistantCompletionTokens = 448,
            ReasoningReserveTokens = 768,
            MultiAgent = new MultiAgentOrchestrationOptions { FinalComposerReasoningReserveTokens = 900 }
        };

        var estimatedPrompt = 1200;
        var floor = AgentContextBudgetEstimator.GetAssistantCompletionFloorTokens(options);
        var maxOut = AgentContextBudgetEstimator.ComputeEffectiveMaxOutputTokens(
            options,
            estimatedPrompt,
            reasoningPressureSteps: 0,
            lastUsage: null,
            continuationAssistantPass: false,
            visibleCompletionFloorOverride: floor);

        Assert.True(maxOut >= floor + 700, $"expected headroom for reasoning+visible, got {maxOut}");
    }

    [Fact]
    public void SpecialistWorker_WhenRequiredAggregateMissingAndPlannerSaysDone_RecoveryPromptListsAggregate()
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
            RequiredEvidenceKinds: [AgentEvidenceKind.Aggregate]);

        var executed = new[]
        {
            new AgentToolExecutionRecord
            {
                ToolName = "profile_columns",
                ArgumentsJson = "{}",
                ResultJson = "{}",
                IsError = false
            }
        };

        var msg = SpecialistToolAgentWorker.BuildEvidenceKindsModelDrivenRecoveryUserContent(
            request,
            executed,
            ["group_and_aggregate", "profile_columns"],
            useFullToolSchemas: true);

        Assert.Contains("Aggregate", msg, StringComparison.Ordinal);
        Assert.Contains("Profile", msg, StringComparison.Ordinal);
        Assert.Contains("group_and_aggregate", msg, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void SpecialistDatasetEvidencePolicy_SearchColumnsEmpty_DoesNotSatisfyStructuralSchema()
    {
        var executions = new[]
        {
            new AgentToolExecutionRecord
            {
                ToolName = "search_columns",
                ArgumentsJson = """{"keyword":"x"}""",
                ResultJson = """{"keyword":"x","matches":[]}""",
                IsError = false
            }
        };

        var kinds = SpecialistDatasetEvidencePolicy.GetSatisfiedEvidenceKinds(executions);
        Assert.DoesNotContain(AgentEvidenceKind.StructuralSchema, kinds);
    }

    [Fact]
    public void SpecialistDatasetEvidencePolicy_SearchColumnsWithMatches_SatisfiesStructuralSchema()
    {
        var executions = new[]
        {
            new AgentToolExecutionRecord
            {
                ToolName = "search_columns",
                ArgumentsJson = """{"keyword":"x"}""",
                ResultJson = """{"keyword":"x","matches":[{"columnName":"c1","matchReason":"n"}]}""",
                IsError = false
            }
        };

        var kinds = SpecialistDatasetEvidencePolicy.GetSatisfiedEvidenceKinds(executions);
        Assert.Contains(AgentEvidenceKind.StructuralSchema, kinds);
    }

    [Fact]
    public void SpecialistDatasetEvidencePolicy_MissingStructuralAndAggregate_ImpliesStructuralDiscoveryToolsInFilteredCatalog()
    {
        var catalog = new AgentToolDefinition[]
        {
            new() { Name = "get_schema", Description = "s", ParametersJsonSchema = "{}" },
            new() { Name = "search_columns", Description = "s", ParametersJsonSchema = "{}" },
            new() { Name = "group_and_aggregate", Description = "s", ParametersJsonSchema = "{}" }
        };

        var executions = new[]
        {
            new AgentToolExecutionRecord
            {
                ToolName = "search_columns",
                ResultJson = """{"matches":[]}""",
                IsError = false
            }
        };

        var request = new AgentTaskRequest(
            AgentSpecialistKind.QueryAnalysis,
            "q",
            "r",
            new AgentConversationMemory(),
            Array.Empty<AgentConversationMessage>(),
            Array.Empty<AgentToolDefinition>(),
            "m",
            "sys",
            RequiredEvidenceKinds:
            [
                AgentEvidenceKind.StructuralSchema,
                AgentEvidenceKind.Aggregate
            ]);

        var filtered = SpecialistDatasetEvidencePolicy.FilterToolsSatisfyingAnyKind(
            catalog,
            SpecialistDatasetEvidencePolicy.GetMissingRequiredEvidenceKinds(request, executions),
            executions);

        Assert.Contains(filtered, t => t.Name.Equals("get_schema", StringComparison.OrdinalIgnoreCase));
        Assert.Contains(filtered, t => t.Name.Equals("group_and_aggregate", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void SpecialistDatasetEvidencePolicy_EnsureStructuralSurface_MergesWhenSchemaUnsatisfied()
    {
        var catalog = new List<AgentToolDefinition>
        {
            new() { Name = "get_schema", Description = "s", ParametersJsonSchema = "{}" },
            new() { Name = "search_columns", Description = "s", ParametersJsonSchema = "{}" },
            new() { Name = "group_and_aggregate", Description = "s", ParametersJsonSchema = "{}" }
        };

        var chosen = new List<AgentToolDefinition> { catalog[2] };
        var merged = SpecialistDatasetEvidencePolicy.EnsureStructuralSurfaceWhenSchemaUnsatisfied(
            catalog,
            chosen,
            [AgentEvidenceKind.Aggregate],
            Array.Empty<AgentToolExecutionRecord>());

        Assert.Contains(merged, t => t.Name.Equals("get_schema", StringComparison.OrdinalIgnoreCase));
        Assert.Contains(merged, t => t.Name.Equals("search_columns", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void ProductionFinalComposerPrompt_AvoidsDomainSpecificExamples()
    {
        var prompt = MultiAgentPromptBuilder.BuildFinalComposerSystemPrompt(new AgentOptions());
        Assert.DoesNotContain("temperature", prompt, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("torque", prompt, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void SpecialistWorker_DoesNotHardcodeColumnNamesInRecoveryPrompt()
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
            RequiredEvidenceKinds: [AgentEvidenceKind.Aggregate]);

        var msg = SpecialistToolAgentWorker.BuildEvidenceKindsModelDrivenRecoveryUserContent(
            request,
            Array.Empty<AgentToolExecutionRecord>(),
            ["group_and_aggregate"],
            useFullToolSchemas: true);

        Assert.DoesNotContain("Machine failure", msg, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("temperature", msg, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("desgaste", msg, StringComparison.OrdinalIgnoreCase);
    }

    private sealed class CoordinatorQueuingChatClient : IAgentChatClient
    {
        private readonly Queue<AgentModelResponse> _queue;

        public CoordinatorQueuingChatClient(params AgentModelResponse[] responses)
        {
            _queue = new Queue<AgentModelResponse>(responses);
        }

        public Task<AgentModelResponse> CompleteAsync(AgentModelRequest request, CancellationToken cancellationToken = default)
        {
            if (_queue.Count == 0)
            {
                throw new InvalidOperationException("empty queue");
            }

            return Task.FromResult(_queue.Dequeue());
        }

        public Task<IReadOnlyList<string>> GetAvailableModelsAsync(CancellationToken cancellationToken = default)
            => Task.FromResult<IReadOnlyList<string>>(["m"]);
    }
}
