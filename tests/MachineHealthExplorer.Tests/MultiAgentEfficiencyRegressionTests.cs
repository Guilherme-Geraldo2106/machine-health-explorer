using MachineHealthExplorer.Agent.Abstractions;
using MachineHealthExplorer.Agent.Models;
using MachineHealthExplorer.Agent.MultiAgent;
using MachineHealthExplorer.Agent.Services;
using Microsoft.Extensions.Logging.Abstractions;
namespace MachineHealthExplorer.Tests;

/// <summary>
/// Regressions for multi-agent efficiency: max_tokens floors, adaptive planner skip, structured outputs, memory worker placement.
/// </summary>
public sealed class MultiAgentEfficiencyRegressionTests
{
    private const string MinimalSpecialistSynthesisJson =
        """{"relevantColumns":[],"ambiguities":[],"evidences":[],"keyMetrics":{},"objectiveObservations":[],"hypothesesOrCaveats":[],"reportSections":[],"analystNotes":"ok"}""";

    private sealed class RecordingChatClient : IAgentChatClient
    {
        private readonly Queue<AgentModelResponse> _queue = new();
        public List<AgentModelRequest> Requests { get; } = new();

        public RecordingChatClient()
        {
        }

        public RecordingChatClient(params AgentModelResponse[] responses)
        {
            foreach (var r in responses)
            {
                _queue.Enqueue(r);
            }
        }

        public Task<AgentModelResponse> CompleteAsync(AgentModelRequest request, CancellationToken cancellationToken = default)
        {
            Requests.Add(request);
            if (_queue.Count == 0)
            {
                throw new InvalidOperationException("RecordingChatClient received an unexpected LLM call.");
            }

            return Task.FromResult(_queue.Dequeue());
        }

        public Task<IReadOnlyList<string>> GetAvailableModelsAsync(CancellationToken cancellationToken = default)
            => Task.FromResult<IReadOnlyList<string>>(["m"]);
    }

    private sealed class TwoToolRuntime : IAgentToolRuntime
    {
        public IReadOnlyList<AgentToolDefinition> GetTools() =>
        [
            new AgentToolDefinition
            {
                Name = "get_schema",
                Description = "Schema",
                ParametersJsonSchema = """{"type":"object","additionalProperties":false}"""
            },
            new AgentToolDefinition
            {
                Name = "group_and_aggregate",
                Description = "Aggregate",
                ParametersJsonSchema = """{"type":"object","additionalProperties":false}"""
            }
        ];

        public Task<AgentToolExecutionRecord> ExecuteAsync(string toolName, string argumentsJson, CancellationToken cancellationToken = default)
            => Task.FromResult(
                new AgentToolExecutionRecord
                {
                    ToolName = toolName,
                    ArgumentsJson = argumentsJson,
                    ResultJson = """{"ok":true}"""
                });
    }

    [Fact]
    public void ApplyHeuristicMemoryFromToolExecutions_DoesNotInvokeChatClient()
    {
        var options = new AgentOptions();
        var chat = new RecordingChatClient();
        var runner = new AgentEphemeralWorkerRunner(options, chat);
        var executions = new[]
        {
            new AgentToolExecutionRecord
            {
                ToolName = "get_schema",
                ArgumentsJson = "{}",
                ResultJson = "{}"
            }
        };

        var memory = runner.ApplyHeuristicMemoryFromToolExecutions("pergunta", executions, new AgentConversationMemory());

        Assert.Contains("get_schema", memory.RollingSummary, StringComparison.OrdinalIgnoreCase);
        Assert.Empty(chat.Requests);
    }

    [Fact]
    public void ComputeEffectiveMaxOutputTokens_InsufficientBudget_IsZeroNotOne()
    {
        var options = new AgentOptions
        {
            ContextSlotTokens = 4096,
            ContextSafetyMarginTokens = 384,
            ReasoningReserveTokens = 768,
            MaxOutputTokens = 4096,
            MinAssistantCompletionTokens = 448
        };

        var maxOut = AgentContextBudgetEstimator.ComputeEffectiveMaxOutputTokens(
            options,
            estimatedPromptTokens: 3600,
            reasoningPressureSteps: 0,
            lastUsage: null);

        Assert.Equal(0, maxOut);
        Assert.NotEqual(1, maxOut);
    }

    [Fact]
    public async Task SpecialistWorker_SmallToolCatalog_SkipsPlannerLlm_WhenBudgetAllows()
    {
        var options = new AgentOptions
        {
            Model = "m",
            HostContextTokens = 12000,
            ContextSafetyMarginTokens = 200,
            ContextBudgetCharsPerToken = 4,
            MultiAgent = new MultiAgentOrchestrationOptions
            {
                EnableSpecialistToolSelectionPlanning = true,
                SpecialistToolPlannerSkipWhenCatalogSizeAtMost = 8,
                SpecialistMaxToolIterations = 6,
                SpecialistToolCallMaxOutputTokens = 900,
                SpecialistRecoveryPreferToolChoiceRequired = false,
                SpecialistProviderSupportsToolChoiceRequired = true
            }
        };

        var chat = new RecordingChatClient(
            new AgentModelResponse
            {
                Model = "m",
                FinishReason = "tool_calls",
                ToolCalls = [new AgentToolCall { Id = "1", Name = "get_schema", ArgumentsJson = "{}" }]
            },
            new AgentModelResponse
            {
                Model = "m",
                FinishReason = "tool_calls",
                ToolCalls = [new AgentToolCall { Id = "2", Name = "group_and_aggregate", ArgumentsJson = "{}" }]
            },
            new AgentModelResponse { Model = "m", Content = string.Empty, FinishReason = "stop" },
            new AgentModelResponse { Model = "m", Content = MinimalSpecialistSynthesisJson, FinishReason = "stop" });

        var worker = new SpecialistToolAgentWorker(options, new TwoToolRuntime(), chat, NullLogger.Instance);
        var request = new AgentTaskRequest(
            AgentSpecialistKind.QueryAnalysis,
            "Q",
            "dispatch",
            new AgentConversationMemory(),
            Array.Empty<AgentConversationMessage>(),
            new TwoToolRuntime().GetTools(),
            "m",
            "sys",
            UseFullToolSchemas: true,
            ExpectsDatasetQueryEvidence: false);

        _ = await worker.ExecuteAsync(request, CancellationToken.None);

        var plannerCalls = chat.Requests.Count(r =>
            !r.EnableTools
            && r.SystemPrompt.Contains("tool-routing planner", StringComparison.Ordinal));
        Assert.Equal(0, plannerCalls);
        Assert.All(chat.Requests, r => Assert.NotEqual(1, r.MaxOutputTokens));
        Assert.All(
            chat.Requests.Where(r => !r.EnableTools && r.ResponseFormat is not null),
            r => Assert.Equal("json_schema", r.ResponseFormat!.Type));
    }

    [Fact]
    public async Task CoordinatorPlanner_WhenStructuredOutputsEnabled_SendsJsonSchemaResponseFormat()
    {
        var options = new AgentOptions
        {
            Model = "m",
            EnableStructuredJsonOutputs = true,
            MultiAgent = new MultiAgentOrchestrationOptions { EnableCoordinatorLlmPlanning = true }
        };

        var chat = new RecordingChatClient(
            new AgentModelResponse
            {
                Model = "m",
                Content = """{"steps":[{"specialist":"Discovery","reason":"x","parallel_group":0}],"notes":""}""",
                FinishReason = "stop"
            });

        var coordinator = new CoordinatorAgent(options, chat, NullLogger.Instance);
        var result = await coordinator.TryPlanAsync(
            "m",
            "hello",
            new AgentConversationMemory(),
            Array.Empty<AgentConversationMessage>(),
            CancellationToken.None);

        Assert.True(result.Success);
        Assert.Single(chat.Requests);
        Assert.NotNull(chat.Requests[0].ResponseFormat);
        Assert.Contains("coordinator_dispatch_plan", chat.Requests[0].ResponseFormat!.Name, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void AppendAssistantTurnFromModel_DoesNotPersistReasoning_InConversation()
    {
        var conversation = new List<AgentConversationMessage>();
        var response = new AgentModelResponse
        {
            Content = "visible",
            ReasoningContent = "SECRET_REASONING_JSON_SHOULD_NOT_APPEAR",
            FinishReason = "stop"
        };

        SpecialistToolAgentWorker.AppendAssistantTurnFromModel(conversation, response);
        Assert.Single(conversation);
        Assert.DoesNotContain("SECRET", conversation[0].Content ?? string.Empty, StringComparison.Ordinal);
        Assert.Equal("visible", conversation[0].Content);
    }

    [Fact]
    public async Task FinalComposer_SuggestQuestion_StubResponse_StaysConcisePortuguese()
    {
        const string shortPt =
            "Três perguntas: (1) … (2) … (3) … Recomendo a (2) porque …";
        var options = new AgentOptions { Model = "m", MultiAgent = new MultiAgentOrchestrationOptions() };
        var chat = new RecordingChatClient(
            new AgentModelResponse { Model = "m", Content = shortPt, FinishReason = "stop" });

        var composer = new FinalComposerAgent(options, chat, NullLogger.Instance);
        var input = new FinalComposerInput(
            OriginalUserQuestion: "Sugira uma pergunta de follow-up",
            DetectedLanguage: "pt",
            ConversationRollingSummary: string.Empty,
            SpecialistResults:
            [
                new AgentTaskResult(
                    AgentSpecialistKind.Discovery,
                    Success: true,
                    null,
                    Array.Empty<AgentToolExecutionRecord>(),
                    new AgentStructuredSpecialistOutput(
                        AgentSpecialistKind.Discovery,
                        RelevantColumns: ["A"],
                        Ambiguities: [],
                        Evidences: [],
                        KeyMetrics: new Dictionary<string, decimal>(),
                        ObjectiveObservations: [],
                        HypothesesOrCaveats: [],
                        ReportSections: [],
                        AnalystNotes: "schema only"),
                    Array.Empty<AgentConversationMessage>())
            ],
            RecentUserAssistantTail: Array.Empty<AgentConversationMessage>(),
            SchemaColumnNamesFromTools: ["A"]);

        var turn = await composer.ComposeFirstResponseAsync(input, "m", new AgentConversationMemory(), CancellationToken.None);
        Assert.Contains("Recomendo", turn.FirstResponse.Content ?? string.Empty, StringComparison.OrdinalIgnoreCase);
        Assert.True((turn.FirstResponse.Content ?? string.Empty).Length < 600);
    }
}
