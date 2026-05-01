using System.Text.Json;
using MachineHealthExplorer.Agent.Abstractions;
using MachineHealthExplorer.Agent.Models;
using MachineHealthExplorer.Agent.MultiAgent;
using Microsoft.Extensions.Logging.Abstractions;

namespace MachineHealthExplorer.Tests;

public sealed class SpecialistToolSelectionPlannerIntegrationTests
{
    private const string FullSchemaSentinel = "__FULL_SCHEMA_SENTINEL__FOR_TESTS__";

    [Fact]
    public async Task SpecialistToolPlanner_RequestHasNoToolsAndNoSchemaSentinel_ToolTurnExposesOnlyChosenFullSchema()
    {
        const string plannerOkGetSchema = """{"need_tools":true,"tools":["get_schema"],"reason":"discover"}""";
        const string plannerOkGroup = """{"need_tools":true,"tools":["group_and_aggregate"],"reason":"aggregate"}""";
        const string plannerDone = """{"need_tools":false,"tools":[],"reason":"enough"}""";

        var options = new AgentOptions
        {
            Model = "m",
            HostContextTokens = 64_000,
            ContextSlotTokens = 64_000,
            ContextSafetyMarginTokens = 256,
            EnableStructuredJsonOutputs = false,
            MultiAgent = new MultiAgentOrchestrationOptions
            {
                SpecialistToolPlannerSkipWhenCatalogSizeAtMost = 0,
                EnableSpecialistToolSelectionPlanning = true,
                SpecialistMaxToolIterations = 6,
                SpecialistToolCallMaxOutputTokens = 900,
                ToolTurnMinOutputTokens = 256,
                SpecialistRecoveryPreferToolChoiceRequired = false,
                SpecialistProviderSupportsToolChoiceRequired = true
            }
        };

        var runtime = new TwoToolRuntimeWithSentinelSchema();
        var chat = new RecordingPlannerQueueChatClient(
            new AgentModelResponse { Model = "m", Content = plannerOkGetSchema, FinishReason = "stop" },
            new AgentModelResponse
            {
                Model = "m",
                FinishReason = "tool_calls",
                ToolCalls = [new AgentToolCall { Id = "1", Name = "get_schema", ArgumentsJson = "{}" }]
            },
            new AgentModelResponse { Model = "m", Content = plannerOkGroup, FinishReason = "stop" },
            new AgentModelResponse
            {
                Model = "m",
                FinishReason = "tool_calls",
                ToolCalls = [new AgentToolCall { Id = "2", Name = "group_and_aggregate", ArgumentsJson = "{}" }]
            },
            new AgentModelResponse { Model = "m", Content = plannerDone, FinishReason = "stop" },
            new AgentModelResponse
            {
                Model = "m",
                Content =
                    """{"relevantColumns":[],"ambiguities":[],"evidences":[{"sourceTool":"group_and_aggregate","summary":"ok","supportingJsonFragment":null}],"keyMetrics":{},"objectiveObservations":[],"hypothesesOrCaveats":[],"reportSections":[],"analystNotes":"ok"}""",
                FinishReason = "stop"
            });

        var worker = new SpecialistToolAgentWorker(options, runtime, chat, NullLogger.Instance);
        var request = new AgentTaskRequest(
            AgentSpecialistKind.QueryAnalysis,
            "Quantitative question",
            "dispatch",
            new AgentConversationMemory(),
            Array.Empty<AgentConversationMessage>(),
            runtime.GetTools(),
            "m",
            "You are QueryAnalysis.",
            UseFullToolSchemas: true,
            ExpectsDatasetQueryEvidence: true);

        _ = await worker.ExecuteAsync(request, CancellationToken.None);

        var plannerRequests = chat.Requests.Where(r => !r.EnableTools && r.SystemPrompt.Contains("tool-routing planner", StringComparison.Ordinal)).ToArray();
        Assert.Equal(3, plannerRequests.Length);
        foreach (var plannerRequest in plannerRequests)
        {
            Assert.False(plannerRequest.EnableTools);
            Assert.Empty(plannerRequest.Tools);
            var serialized = JsonSerializer.Serialize(plannerRequest);
            Assert.DoesNotContain(FullSchemaSentinel, serialized, StringComparison.Ordinal);
        }

        var toolTurns = chat.Requests.Where(r => r.EnableTools && r.Tools.Count > 0).ToArray();
        Assert.True(toolTurns.Length >= 2);
        Assert.Single(toolTurns[0].Tools);
        Assert.Equal("get_schema", toolTurns[0].Tools[0].Name, StringComparer.OrdinalIgnoreCase);
        Assert.False(toolTurns[0].UseMinimalToolSchemas);
        Assert.Contains(FullSchemaSentinel, toolTurns[0].Tools[0].ParametersJsonSchema, StringComparison.Ordinal);

        Assert.Single(toolTurns[1].Tools);
        Assert.Equal("group_and_aggregate", toolTurns[1].Tools[0].Name, StringComparer.OrdinalIgnoreCase);
        Assert.Contains(FullSchemaSentinel, toolTurns[1].Tools[0].ParametersJsonSchema, StringComparison.Ordinal);
    }

    [Fact]
    public async Task SpecialistToolPlanner_InvalidTwiceThenFallback_UsesMinimalSchemasForFullAllowlistWithoutKeywordHeuristic()
    {
        var options = new AgentOptions
        {
            Model = "m",
            MultiAgent = new MultiAgentOrchestrationOptions
            {
                SpecialistToolPlannerSkipWhenCatalogSizeAtMost = 0,
                EnableSpecialistToolSelectionPlanning = true,
                SpecialistToolSelectionPlannerMaxRecoveryAttempts = 2,
                SpecialistMaxToolIterations = 5,
                SpecialistToolCallMaxOutputTokens = 900,
                SpecialistRecoveryPreferToolChoiceRequired = false
            }
        };

        var runtime = new TwoToolRuntimeWithSentinelSchema();
        var chat = new RecordingPlannerQueueChatClient(
            new AgentModelResponse { Model = "m", Content = string.Empty, FinishReason = "stop" },
            new AgentModelResponse { Model = "m", Content = "not-json", FinishReason = "length" },
            new AgentModelResponse
            {
                Model = "m",
                FinishReason = "tool_calls",
                ToolCalls = [new AgentToolCall { Id = "1", Name = "get_schema", ArgumentsJson = "{}" }]
            },
            new AgentModelResponse { Model = "m", Content = """{"need_tools":false,"tools":[],"reason":"x"}""", FinishReason = "stop" },
            new AgentModelResponse
            {
                Model = "m",
                Content =
                    """{"relevantColumns":[],"ambiguities":[],"evidences":[],"keyMetrics":{},"objectiveObservations":[],"hypothesesOrCaveats":[],"reportSections":[],"analystNotes":"ok"}""",
                FinishReason = "stop"
            });

        var worker = new SpecialistToolAgentWorker(options, runtime, chat, NullLogger.Instance);
        var request = new AgentTaskRequest(
            AgentSpecialistKind.QueryAnalysis,
            "Question",
            "dispatch",
            new AgentConversationMemory(),
            Array.Empty<AgentConversationMessage>(),
            runtime.GetTools(),
            "m",
            "sys",
            UseFullToolSchemas: true,
            ExpectsDatasetQueryEvidence: false);

        _ = await worker.ExecuteAsync(request, CancellationToken.None);

        var fallbackToolRequest = chat.Requests.First(r => r.EnableTools && r.Tools.Count == 2);
        Assert.True(fallbackToolRequest.UseMinimalToolSchemas);
        Assert.Equal(2, fallbackToolRequest.Tools.Count);
    }

    [Fact]
    public async Task SpecialistToolPlanner_WithLargeAllowlist_SelectsOneTool_ToolTurnRespectsSafeFloor()
    {
        var options = new AgentOptions
        {
            Model = "m",
            HostContextTokens = 9000,
            ContextSafetyMarginTokens = 200,
            ContextBudgetCharsPerToken = 2,
            MultiAgent = new MultiAgentOrchestrationOptions
            {
                SpecialistToolPlannerSkipWhenCatalogSizeAtMost = 0,
                EnableSpecialistToolSelectionPlanning = true,
                SpecialistMaxToolIterations = 4,
                SpecialistToolCallMaxOutputTokens = 900,
                ToolTurnMinOutputTokens = 512,
                ToolTurnReasoningReserveTokens = 900,
                ToolTurnSafeMinMaxOutputTokens = 128,
                SpecialistContextBudgetRecoveryMaxPasses = 4,
                SpecialistRecoveryPreferToolChoiceRequired = false
            }
        };

        var runtime = new ManyLargeToolsPlusSmallGetSchemaRuntime(FullSchemaSentinel);
        var plannerJson = """{"need_tools":true,"tools":["get_schema"],"reason":"schema"}""";
        const string plannerNoMoreTools = """{"need_tools":false,"tools":[],"reason":"done"}""";
        var chat = new RecordingPlannerQueueChatClient(
            new AgentModelResponse { Model = "m", Content = plannerJson, FinishReason = "stop" },
            new AgentModelResponse
            {
                Model = "m",
                FinishReason = "tool_calls",
                ToolCalls = [new AgentToolCall { Id = "1", Name = "get_schema", ArgumentsJson = "{}" }]
            },
            new AgentModelResponse { Model = "m", Content = plannerNoMoreTools, FinishReason = "stop" },
            new AgentModelResponse
            {
                Model = "m",
                Content =
                    """{"relevantColumns":[],"ambiguities":[],"evidences":[],"keyMetrics":{},"objectiveObservations":[],"hypothesesOrCaveats":[],"reportSections":[],"analystNotes":"ok"}""",
                FinishReason = "stop"
            });

        var guarded = new BudgetFloorGuardChatClient(chat, 128);
        var worker = new SpecialistToolAgentWorker(options, runtime, guarded, NullLogger.Instance);
        var request = new AgentTaskRequest(
            AgentSpecialistKind.QueryAnalysis,
            "Question",
            "dispatch",
            new AgentConversationMemory(),
            Array.Empty<AgentConversationMessage>(),
            runtime.GetTools(),
            "m",
            new string('s', 1200),
            UseFullToolSchemas: true,
            ExpectsDatasetQueryEvidence: false);

        var result = await worker.ExecuteAsync(request, CancellationToken.None);

        Assert.True(result.Success);
        var toolEnabled = chat.Requests.Where(r => r.EnableTools && r.Tools.Count > 0).ToArray();
        Assert.NotEmpty(toolEnabled);
        Assert.All(toolEnabled, r => Assert.True(r.MaxOutputTokens >= 128));
        Assert.Contains(toolEnabled, r => r.Tools.Count == 1 && r.Tools[0].Name.Equals("get_schema", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public async Task SpecialistToolPlanner_SingleToolAfterTruncation_UsesRequireToolCallWhenConfigured()
    {
        var options = new AgentOptions
        {
            Model = "m",
            MultiAgent = new MultiAgentOrchestrationOptions
            {
                SpecialistToolPlannerSkipWhenCatalogSizeAtMost = 0,
                EnableSpecialistToolSelectionPlanning = true,
                SpecialistMaxToolIterations = 7,
                SpecialistToolCallMaxOutputTokens = 900,
                SpecialistRecoveryPreferToolChoiceRequired = true,
                SpecialistProviderSupportsToolChoiceRequired = true
            }
        };

        var runtime = new TwoToolRuntimeWithSentinelSchema();
        var chat = new RecordingPlannerQueueChatClient(
            new AgentModelResponse { Model = "m", Content = """{"need_tools":true,"tools":["get_schema"],"reason":"a"}""", FinishReason = "stop" },
            new AgentModelResponse
            {
                Model = "m",
                FinishReason = "tool_calls",
                ToolCalls = [new AgentToolCall { Id = "1", Name = "get_schema", ArgumentsJson = "{}" }]
            },
            new AgentModelResponse { Model = "m", Content = """{"need_tools":true,"tools":["group_and_aggregate"],"reason":"b"}""", FinishReason = "stop" },
            new AgentModelResponse
            {
                Model = "m",
                Content = "x",
                ReasoningContent = new string('r', 40),
                FinishReason = "length",
                ToolCalls = []
            },
            new AgentModelResponse
            {
                Model = "m",
                FinishReason = "tool_calls",
                ToolCalls = [new AgentToolCall { Id = "2", Name = "group_and_aggregate", ArgumentsJson = "{}" }]
            },
            new AgentModelResponse { Model = "m", Content = """{"need_tools":false,"tools":[],"reason":"done"}""", FinishReason = "stop" },
            new AgentModelResponse
            {
                Model = "m",
                Content =
                    """{"relevantColumns":[],"ambiguities":[],"evidences":[{"sourceTool":"group_and_aggregate","summary":"ok","supportingJsonFragment":null}],"keyMetrics":{},"objectiveObservations":[],"hypothesesOrCaveats":[],"reportSections":[],"analystNotes":"ok"}""",
                FinishReason = "stop"
            });

        var worker = new SpecialistToolAgentWorker(options, runtime, chat, NullLogger.Instance);
        var request = new AgentTaskRequest(
            AgentSpecialistKind.QueryAnalysis,
            "Question",
            "dispatch",
            new AgentConversationMemory(),
            Array.Empty<AgentConversationMessage>(),
            runtime.GetTools(),
            "m",
            "sys",
            UseFullToolSchemas: true,
            ExpectsDatasetQueryEvidence: true);

        _ = await worker.ExecuteAsync(request, CancellationToken.None);

        var requiredTurn = chat.Requests.FirstOrDefault(r =>
            r.EnableTools
            && r.RequireToolCall
            && r.Tools.Any(t => t.Name.Equals("group_and_aggregate", StringComparison.OrdinalIgnoreCase)));
        Assert.NotNull(requiredTurn);

        var multiSurface = chat.Requests.FirstOrDefault(r =>
            r.EnableTools
            && r.Tools.Count > 1
            && r.RequireToolCall);
        Assert.Null(multiSurface);
    }

    [Fact]
    public async Task SpecialistToolPlanner_SelectsTwoTools_DoesNotSetRequireToolCall()
    {
        var options = new AgentOptions
        {
            Model = "m",
            MultiAgent = new MultiAgentOrchestrationOptions
            {
                SpecialistToolPlannerSkipWhenCatalogSizeAtMost = 0,
                EnableSpecialistToolSelectionPlanning = true,
                SpecialistMaxToolIterations = 4,
                SpecialistToolCallMaxOutputTokens = 900,
                SpecialistRecoveryPreferToolChoiceRequired = true,
                SpecialistProviderSupportsToolChoiceRequired = true
            }
        };

        var runtime = new TwoToolRuntimeWithSentinelSchema();
        var chat = new RecordingPlannerQueueChatClient(
            new AgentModelResponse
            {
                Model = "m",
                Content = """{"need_tools":true,"tools":["get_schema","group_and_aggregate"],"reason":"both"}""",
                FinishReason = "stop"
            },
            new AgentModelResponse
            {
                Model = "m",
                FinishReason = "tool_calls",
                ToolCalls =
                [
                    new AgentToolCall { Id = "1", Name = "get_schema", ArgumentsJson = "{}" }
                ]
            },
            new AgentModelResponse { Model = "m", Content = """{"need_tools":false,"tools":[],"reason":"done"}""", FinishReason = "stop" },
            new AgentModelResponse
            {
                Model = "m",
                Content =
                    """{"relevantColumns":[],"ambiguities":[],"evidences":[],"keyMetrics":{},"objectiveObservations":[],"hypothesesOrCaveats":[],"reportSections":[],"analystNotes":"ok"}""",
                FinishReason = "stop"
            });

        var worker = new SpecialistToolAgentWorker(options, runtime, chat, NullLogger.Instance);
        var request = new AgentTaskRequest(
            AgentSpecialistKind.QueryAnalysis,
            "Question",
            "dispatch",
            new AgentConversationMemory(),
            Array.Empty<AgentConversationMessage>(),
            runtime.GetTools(),
            "m",
            "sys",
            UseFullToolSchemas: true,
            ExpectsDatasetQueryEvidence: false);

        _ = await worker.ExecuteAsync(request, CancellationToken.None);

        var twoToolRequest = chat.Requests.First(r => r.EnableTools && r.Tools.Count == 2);
        Assert.False(twoToolRequest.RequireToolCall);
    }

    private sealed class RecordingPlannerQueueChatClient : IAgentChatClient
    {
        private readonly Queue<AgentModelResponse> _queue;
        public List<AgentModelRequest> Requests { get; } = [];

        public RecordingPlannerQueueChatClient(params AgentModelResponse[] responses)
        {
            _queue = new Queue<AgentModelResponse>(responses);
        }

        public Task<AgentModelResponse> CompleteAsync(AgentModelRequest request, CancellationToken cancellationToken = default)
        {
            Requests.Add(request);
            return Task.FromResult(_queue.Dequeue());
        }

        public Task<IReadOnlyList<string>> GetAvailableModelsAsync(CancellationToken cancellationToken = default)
            => Task.FromResult<IReadOnlyList<string>>(["m"]);
    }

    private sealed class BudgetFloorGuardChatClient : IAgentChatClient
    {
        private readonly RecordingPlannerQueueChatClient _inner;
        private readonly int _floor;

        public BudgetFloorGuardChatClient(RecordingPlannerQueueChatClient inner, int floor)
        {
            _inner = inner;
            _floor = floor;
        }

        public Task<AgentModelResponse> CompleteAsync(AgentModelRequest request, CancellationToken cancellationToken = default)
        {
            if (request.EnableTools && request.Tools.Count > 0 && request.MaxOutputTokens < _floor)
            {
                throw new InvalidOperationException($"Below floor: {request.MaxOutputTokens}");
            }

            return _inner.CompleteAsync(request, cancellationToken);
        }

        public Task<IReadOnlyList<string>> GetAvailableModelsAsync(CancellationToken cancellationToken = default)
            => _inner.GetAvailableModelsAsync(cancellationToken);
    }

    private sealed class TwoToolRuntimeWithSentinelSchema : IAgentToolRuntime
    {
        public IReadOnlyList<AgentToolDefinition> GetTools()
            =>
            [
                new AgentToolDefinition
                {
                    Name = "get_schema",
                    Description = "Return dataset schema.",
                    ParametersJsonSchema =
                        "{\"type\":\"object\",\"description\":\"" + FullSchemaSentinel + "\",\"additionalProperties\":false}"
                },
                new AgentToolDefinition
                {
                    Name = "group_and_aggregate",
                    Description = "Aggregate rows.",
                    ParametersJsonSchema =
                        "{\"type\":\"object\",\"description\":\"" + FullSchemaSentinel + "\",\"additionalProperties\":false}"
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

    private sealed class ManyLargeToolsPlusSmallGetSchemaRuntime : IAgentToolRuntime
    {
        private readonly IReadOnlyList<AgentToolDefinition> _tools;

        public ManyLargeToolsPlusSmallGetSchemaRuntime(string sentinel)
        {
            var filler = new string('p', 1800);
            var largeSchema =
                "{\"type\":\"object\",\"description\":\"" + filler + "\",\"properties\":{\"x\":{\"type\":\"string\"}},\"additionalProperties\":false}";
            var list = Enumerable.Range(0, 18)
                .Select(index => new AgentToolDefinition
                {
                    Name = $"tool_{index}",
                    Description = "d",
                    ParametersJsonSchema = largeSchema
                })
                .ToList();
            list.Add(
                new AgentToolDefinition
                {
                    Name = "get_schema",
                    Description = "schema",
                    ParametersJsonSchema =
                        "{\"type\":\"object\",\"description\":\"" + sentinel + "\",\"additionalProperties\":false}"
                });
            _tools = list;
        }

        public IReadOnlyList<AgentToolDefinition> GetTools() => _tools;

        public Task<AgentToolExecutionRecord> ExecuteAsync(string toolName, string argumentsJson, CancellationToken cancellationToken = default)
            => Task.FromResult(
                new AgentToolExecutionRecord
                {
                    ToolName = toolName,
                    ArgumentsJson = argumentsJson,
                    ResultJson = "{}"
                });
    }
}
