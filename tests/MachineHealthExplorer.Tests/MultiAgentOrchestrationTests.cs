using MachineHealthExplorer.Agent.Abstractions;
using MachineHealthExplorer.Agent.Models;
using MachineHealthExplorer.Agent.MultiAgent;
using MachineHealthExplorer.Agent.Services;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace MachineHealthExplorer.Tests;

public sealed class MultiAgentOrchestrationTests
{
    [Fact]
    public void HeuristicPlan_SchemaQuestion_UsesDiscoveryOnly()
    {
        var options = new MultiAgentOrchestrationOptions();
        var plan = MultiAgentDispatchHeuristics.Plan("List the dataset schema columns.", options);
        Assert.Single(plan.Steps);
        Assert.Equal(AgentSpecialistKind.Discovery, plan.Steps[0].SpecialistKind);
    }

    [Fact]
    public void HeuristicPlan_AggregationQuestion_IncludesQueryAnalysis()
    {
        var options = new MultiAgentOrchestrationOptions();
        var plan = MultiAgentDispatchHeuristics.Plan("What is the mean torque grouped by machine type?", options);
        Assert.Contains(plan.Steps, step => step.SpecialistKind == AgentSpecialistKind.QueryAnalysis);
    }

    [Fact]
    public void HeuristicPlan_FailureQuestion_WithoutQueryConflict_StillAllowsFailureAnalysis()
    {
        var options = new MultiAgentOrchestrationOptions();
        var plan = MultiAgentDispatchHeuristics.Plan(
            "Summarize maintenance patterns observed in historical failure cases.",
            options);
        Assert.Contains(plan.Steps, step => step.SpecialistKind == AgentSpecialistKind.FailureAnalysis);
    }

    [Fact]
    public void HeuristicPlan_CompareFailureRateVsHealthy_DropsFailureWhenQueryAnalysisPresent()
    {
        var options = new MultiAgentOrchestrationOptions();
        var plan = MultiAgentDispatchHeuristics.Plan("Compare failure rate vs healthy cohort.", options);
        Assert.Contains(plan.Steps, step => step.SpecialistKind == AgentSpecialistKind.QueryAnalysis);
        Assert.DoesNotContain(plan.Steps, step => step.SpecialistKind == AgentSpecialistKind.FailureAnalysis);
    }

    [Fact]
    public void HeuristicPlan_TemperatureFailureChances_Portuguese_IsSingleQueryAnalysis()
    {
        var options = new MultiAgentOrchestrationOptions();
        var plan = MultiAgentDispatchHeuristics.Plan(
            "a partir de qual temperatura as chances de falham aumentam consideravelmente?",
            options);

        Assert.Single(plan.Steps);
        Assert.Equal(AgentSpecialistKind.QueryAnalysis, plan.Steps[0].SpecialistKind);
        Assert.Equal(0, plan.Steps[0].ParallelGroup);
    }

    [Fact]
    public void PostNormalizeDispatchPlan_CollapsesCoordinatorOverreach_ForTemperatureFailureBins()
    {
        var options = new MultiAgentOrchestrationOptions();
        var plan = new AgentDispatchPlan(
            [
                new AgentDispatchStep(AgentSpecialistKind.Discovery, "llm", ParallelGroup: 0),
                new AgentDispatchStep(AgentSpecialistKind.QueryAnalysis, "llm", ParallelGroup: 1),
                new AgentDispatchStep(AgentSpecialistKind.FailureAnalysis, "llm", ParallelGroup: 1)
            ],
            string.Empty,
            UsedLlmPlanner: true);

        var normalized = MultiAgentDispatchHeuristics.PostNormalizeDispatchPlan(
            "a partir de qual temperatura as chances de falhas aumentam?",
            plan,
            options);

        Assert.Single(normalized.Steps);
        Assert.Equal(AgentSpecialistKind.QueryAnalysis, normalized.Steps[0].SpecialistKind);
    }

    [Fact]
    public void HeuristicPlan_ExecutiveReport_IncludesReporting()
    {
        var options = new MultiAgentOrchestrationOptions();
        var plan = MultiAgentDispatchHeuristics.Plan("Write an executive summary report for management.", options);
        Assert.Contains(plan.Steps, step => step.SpecialistKind == AgentSpecialistKind.Reporting);
    }

    [Fact]
    public void HeuristicPlan_FailureTemperatureBandQuestion_Portuguese_IncludesQueryAnalysis()
    {
        var options = new MultiAgentOrchestrationOptions();
        var plan = MultiAgentDispatchHeuristics.Plan(
            "qual a faixa de temperatura em que ocorre mais falhas ?",
            options);

        Assert.Contains(plan.Steps, step => step.SpecialistKind == AgentSpecialistKind.QueryAnalysis);
    }

    [Fact]
    public void DetectLanguage_PortugueseQuestion_OverridesEnglishPreference()
    {
        Assert.Equal(
            "pt",
            LanguageHeuristics.DetectLanguage("qual a faixa de temperatura em que ocorre mais falhas ?", "en"));
    }

    [Fact]
    public async Task FilteredAgentToolRuntime_RejectsToolsOutsideSpecialistScope()
    {
        var inner = new StubInnerRuntime();
        var filtered = new FilteredAgentToolRuntime(inner, ["get_schema"], "test");

        var bad = await filtered.ExecuteAsync("query_rows", "{}", CancellationToken.None);
        Assert.True(bad.IsError);
        Assert.Contains("not allowed", bad.ResultJson, StringComparison.OrdinalIgnoreCase);

        var ok = await filtered.ExecuteAsync("get_schema", "{}", CancellationToken.None);
        Assert.False(ok.IsError);
        Assert.Equal("get_schema", ok.ToolName);
    }

    [Fact]
    public async Task FinalComposer_UsesNoToolsInModelRequest()
    {
        var recording = new RecordingChatClient(
            new AgentModelResponse { Model = "m", Content = "ok", FinishReason = "stop" });

        var composer = new FinalComposerAgent(
            new AgentOptions { Model = "m" },
            recording,
            NullLogger.Instance);

        var input = new FinalComposerInput(
            OriginalUserQuestion: "test",
            DetectedLanguage: "en",
            ConversationRollingSummary: null,
            SpecialistResults: Array.Empty<AgentTaskResult>(),
            RecentUserAssistantTail: Array.Empty<AgentConversationMessage>());

        await composer.ComposeFirstResponseAsync(
            input,
            "m",
            new AgentConversationMemory(),
            CancellationToken.None);

        var request = Assert.Single(recording.Requests);
        Assert.False(request.EnableTools);
        Assert.Empty(request.Tools);
    }

    private sealed class StubInnerRuntime : IAgentToolRuntime
    {
        public IReadOnlyList<AgentToolDefinition> GetTools()
            =>
            [
                new AgentToolDefinition { Name = "get_schema", Description = "schema" },
                new AgentToolDefinition { Name = "query_rows", Description = "rows" }
            ];

        public Task<AgentToolExecutionRecord> ExecuteAsync(string toolName, string argumentsJson, CancellationToken cancellationToken = default)
            => Task.FromResult(new AgentToolExecutionRecord
            {
                ToolName = toolName,
                ArgumentsJson = argumentsJson,
                ResultJson = """{"ok":true}"""
            });
    }

    private sealed class RecordingChatClient : IAgentChatClient
    {
        private readonly Queue<AgentModelResponse> _responses;
        public List<AgentModelRequest> Requests { get; } = [];

        public RecordingChatClient(params AgentModelResponse[] responses)
        {
            _responses = new Queue<AgentModelResponse>(responses);
        }

        public Task<AgentModelResponse> CompleteAsync(AgentModelRequest request, CancellationToken cancellationToken = default)
        {
            Requests.Add(request);
            return Task.FromResult(_responses.Dequeue());
        }

        public Task<IReadOnlyList<string>> GetAvailableModelsAsync(CancellationToken cancellationToken = default)
            => Task.FromResult<IReadOnlyList<string>>(["m"]);
    }

    [Fact]
    public async Task SpecialistToolAgentWorker_ContextExceededAfterToolExecution_PreservesToolExecutions()
    {
        var options = new AgentOptions
        {
            Model = "m",
            MultiAgent = new MultiAgentOrchestrationOptions
            {
                EnableSpecialistToolSelectionPlanning = false,
                SpecialistMaxToolIterations = 6
            },
            MaxToolEvidenceContentChars = 8000
        };

        var runtime = new GroupAggregateToolStubRuntime();
        var chat = new ContextExceededAfterSecondCompletionClient();
        var worker = new SpecialistToolAgentWorker(options, runtime, chat, NullLogger.Instance);

        var request = new AgentTaskRequest(
            AgentSpecialistKind.QueryAnalysis,
            "qual a faixa de temperatura em que ocorre mais falhas ?",
            "test",
            new AgentConversationMemory(),
            Array.Empty<AgentConversationMessage>(),
            runtime.GetTools(),
            "m",
            "You are QueryAnalysis.");

        var result = await worker.ExecuteAsync(request, CancellationToken.None);

        Assert.True(result.ToolExecutions.Count > 0);
        Assert.Contains(
            result.ToolExecutions,
            execution =>
                execution.ToolName.Equals("group_and_aggregate", StringComparison.OrdinalIgnoreCase)
                && !execution.IsError);
        Assert.NotEmpty(result.StructuredOutput.Evidences);
    }

    private sealed class GroupAggregateToolStubRuntime : IAgentToolRuntime
    {
        public IReadOnlyList<AgentToolDefinition> GetTools()
            =>
            [
                new AgentToolDefinition
                {
                    Name = "group_and_aggregate",
                    Description = "aggregate",
                    ParametersJsonSchema = """{"type":"object","additionalProperties":false}"""
                },
                new AgentToolDefinition
                {
                    Name = "query_rows",
                    Description = "rows",
                    ParametersJsonSchema = """{"type":"object","additionalProperties":false}"""
                }
            ];

        public Task<AgentToolExecutionRecord> ExecuteAsync(string toolName, string argumentsJson, CancellationToken cancellationToken = default)
        {
            if (toolName.Equals("group_and_aggregate", StringComparison.OrdinalIgnoreCase))
            {
                return Task.FromResult(
                    new AgentToolExecutionRecord
                    {
                        ToolName = toolName,
                        ArgumentsJson = argumentsJson,
                        ResultJson =
                            """{"columns":["proc_temp_bin","row_count","failure_count"],"rows":[{"values":{"proc_temp_bin":310,"row_count":2456,"failure_count":120}}],"totalGroups":1,"scopedRowCount":10000}"""
                    });
            }

            return Task.FromResult(
                new AgentToolExecutionRecord
                {
                    ToolName = toolName,
                    ArgumentsJson = argumentsJson,
                    ResultJson = "{}",
                    IsError = true
                });
        }
    }

    private sealed class ContextExceededAfterSecondCompletionClient : IAgentChatClient
    {
        private int _calls;

        public Task<AgentModelResponse> CompleteAsync(AgentModelRequest request, CancellationToken cancellationToken = default)
        {
            _calls++;
            if (_calls == 1)
            {
                return Task.FromResult(
                    new AgentModelResponse
                    {
                        Model = "m",
                        Content = string.Empty,
                        FinishReason = "tool_calls",
                        ToolCalls =
                        [
                            new AgentToolCall
                            {
                                Id = "1",
                                Name = "group_and_aggregate",
                                ArgumentsJson = "{}"
                            }
                        ]
                    });
            }

            throw new AgentModelBackendException(
                "Context size has been exceeded",
                400,
                "Context size has been exceeded",
                isContextLengthExceeded: true);
        }

        public Task<IReadOnlyList<string>> GetAvailableModelsAsync(CancellationToken cancellationToken = default)
            => Task.FromResult<IReadOnlyList<string>>(["m"]);
    }
}
