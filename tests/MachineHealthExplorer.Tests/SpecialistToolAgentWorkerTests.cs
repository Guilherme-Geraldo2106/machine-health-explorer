using System.Text.Json;
using MachineHealthExplorer.Agent.Abstractions;
using MachineHealthExplorer.Agent.Models;
using MachineHealthExplorer.Agent.MultiAgent;
using MachineHealthExplorer.Agent.Services;
using Microsoft.Extensions.Logging.Abstractions;

namespace MachineHealthExplorer.Tests;

public sealed class SpecialistToolAgentWorkerTests
{
    [Fact]
    public async Task ExecuteAsync_TruncationWithoutTools_DoesNotInjectRawReasoningIntoNextPrompt()
    {
        const string reasoningMarker = "REASONING_JUNK_MARKER_XYZ";
        var options = new AgentOptions
        {
            Model = "m",
            HostContextTokens = 16_000,
            ContextSlotTokens = 16_000,
            MultiAgent = new MultiAgentOrchestrationOptions
            {
                EnableSpecialistToolSelectionPlanning = false,
                SpecialistMaxToolIterations = 5,
                SpecialistToolCallMaxOutputTokens = 900,
                ToolTurnMinOutputTokens = 512,
                ToolTurnReasoningReserveTokens = 128,
                SpecialistRecoveryPreferToolChoiceRequired = false
            }
        };

        var runtime = new SchemaThenAggregateRuntime();
        var inner = new QueuingChatClient(
            new AgentModelResponse
            {
                Model = "m",
                FinishReason = "tool_calls",
                ToolCalls =
                [
                    new AgentToolCall { Id = "1", Name = "get_schema", ArgumentsJson = "{}" }
                ]
            },
            new AgentModelResponse
            {
                Model = "m",
                Content = string.Empty,
                ReasoningContent = reasoningMarker + new string('r', 4000),
                FinishReason = "length",
                ToolCalls = []
            },
            new AgentModelResponse
            {
                Model = "m",
                FinishReason = "tool_calls",
                ToolCalls =
                [
                    new AgentToolCall { Id = "2", Name = "group_and_aggregate", ArgumentsJson = "{}" }
                ]
            },
            new AgentModelResponse { Model = "m", Content = string.Empty, FinishReason = "stop", ToolCalls = [] },
            new AgentModelResponse
            {
                Model = "m",
                Content =
                    """
                    {"relevantColumns":[],"ambiguities":[],"evidences":[{"sourceTool":"group_and_aggregate","summary":"ok","supportingJsonFragment":null}],"keyMetrics":{},"objectiveObservations":[],"hypothesesOrCaveats":[],"reportSections":[],"analystNotes":"ok"}
                    """,
                FinishReason = "stop"
            });

        var chat = new BudgetFloorGuardChatClient(inner, minToolTurnMaxOutputTokensWhenToolsEnabled: 96);
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
            ExpectsDatasetQueryEvidence: true);

        _ = await worker.ExecuteAsync(request, CancellationToken.None);

        var afterTruncation = inner.Requests.FirstOrDefault(r => r.EnableTools && r.Messages.Any(m => (m.Content ?? string.Empty).Contains("truncated", StringComparison.OrdinalIgnoreCase)));
        Assert.NotNull(afterTruncation);
        var serializedAfterTruncation = JsonSerializer.Serialize(afterTruncation.Messages);
        Assert.DoesNotContain(reasoningMarker, serializedAfterTruncation, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ExecuteAsync_NeverIssuesToolEnabledRequestBelowSafeMaxOutputFloor()
    {
        var options = new AgentOptions
        {
            Model = "m",
            HostContextTokens = 1800,
            ContextSafetyMarginTokens = 200,
            ContextBudgetCharsPerToken = 2,
            MultiAgent = new MultiAgentOrchestrationOptions
            {
                EnableSpecialistToolSelectionPlanning = false,
                SpecialistMaxToolIterations = 4,
                SpecialistToolCallMaxOutputTokens = 900,
                ToolTurnMinOutputTokens = 512,
                ToolTurnReasoningReserveTokens = 900,
                ToolTurnSafeMinMaxOutputTokens = 128,
                SpecialistContextBudgetRecoveryMaxPasses = 2,
                SpecialistRecoveryPreferToolChoiceRequired = false
            }
        };

        var runtime = new ManyLargeSchemaToolsRuntime(toolCount: 24, schemaChars: 2200);
        var inner = new QueuingChatClient(
            new AgentModelResponse
            {
                Model = "m",
                FinishReason = "tool_calls",
                ToolCalls =
                [
                    new AgentToolCall { Id = "1", Name = "get_schema", ArgumentsJson = "{}" }
                ]
            });
        var chat = new BudgetFloorGuardChatClient(inner, minToolTurnMaxOutputTokensWhenToolsEnabled: 128);
        var worker = new SpecialistToolAgentWorker(options, runtime, chat, NullLogger.Instance);
        var request = new AgentTaskRequest(
            AgentSpecialistKind.QueryAnalysis,
            "Question",
            "dispatch",
            new AgentConversationMemory(),
            Array.Empty<AgentConversationMessage>(),
            runtime.GetTools(),
            "m",
            new string('s', 4000),
            ExpectsDatasetQueryEvidence: true);

        var result = await worker.ExecuteAsync(request, CancellationToken.None);

        Assert.False(result.Success);
        Assert.Contains("context budget exhausted", result.FailureMessage ?? string.Empty, StringComparison.OrdinalIgnoreCase);
        Assert.Empty(inner.Requests);
    }

    [Fact]
    public async Task ExecuteAsync_StructuralEvidenceRecovery_PreservesGenericGroupAggregateContract()
    {
        var options = new AgentOptions
        {
            Model = "m",
            MultiAgent = new MultiAgentOrchestrationOptions
            {
                EnableSpecialistToolSelectionPlanning = false,
                SpecialistMaxToolIterations = 5,
                SpecialistMaxStructuralEvidenceRecoveryUserTurns = 1,
                SpecialistRecoveryPreferToolChoiceRequired = false
            }
        };

        var runtime = new GenericTwoToolRuntime();
        var inner = new QueuingChatClient(
            new AgentModelResponse
            {
                Model = "m",
                FinishReason = "tool_calls",
                ToolCalls =
                [
                    new AgentToolCall { Id = "1", Name = "get_schema", ArgumentsJson = "{}" }
                ]
            },
            new AgentModelResponse { Model = "m", Content = string.Empty, FinishReason = "stop", ToolCalls = [] },
            new AgentModelResponse
            {
                Model = "m",
                FinishReason = "tool_calls",
                ToolCalls =
                [
                    new AgentToolCall { Id = "2", Name = "group_and_aggregate", ArgumentsJson = "{}" }
                ]
            },
            new AgentModelResponse { Model = "m", Content = string.Empty, FinishReason = "stop", ToolCalls = [] },
            new AgentModelResponse
            {
                Model = "m",
                Content =
                    """
                    {"relevantColumns":[],"ambiguities":[],"evidences":[{"sourceTool":"group_and_aggregate","summary":"ok","supportingJsonFragment":null}],"keyMetrics":{},"objectiveObservations":[],"hypothesesOrCaveats":[],"reportSections":[],"analystNotes":"ok"}
                    """,
                FinishReason = "stop"
            });

        var chat = new BudgetFloorGuardChatClient(inner, minToolTurnMaxOutputTokensWhenToolsEnabled: 96);
        var worker = new SpecialistToolAgentWorker(options, runtime, chat, NullLogger.Instance);
        var request = new AgentTaskRequest(
            AgentSpecialistKind.QueryAnalysis,
            "Need bins",
            "dispatch",
            new AgentConversationMemory(),
            Array.Empty<AgentConversationMessage>(),
            runtime.GetTools(),
            "m",
            "sys",
            ExpectsDatasetQueryEvidence: true,
            UseFullToolSchemas: true);

        _ = await worker.ExecuteAsync(request, CancellationToken.None);

        var recoverySerialized = string.Join('|', inner.Requests.Select(r => JsonSerializer.Serialize(r.Messages)));
        Assert.Contains("groupByBins", recoverySerialized, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("aggregations", recoverySerialized, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("Machine failure", recoverySerialized, StringComparison.Ordinal);
        Assert.DoesNotContain("Air temperature", recoverySerialized, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ExecuteAsync_AfterGetSchema_TruncatedLengthWithoutTools_ContinuesToolLoopThenAggregates()
    {
        var options = new AgentOptions
        {
            Model = "m",
            HostContextTokens = 16_000,
            ContextSlotTokens = 16_000,
            MultiAgent = new MultiAgentOrchestrationOptions
            {
                EnableSpecialistToolSelectionPlanning = false,
                SpecialistMaxToolIterations = 6,
                SpecialistToolCallMaxOutputTokens = 900,
                ToolTurnMinOutputTokens = 512,
                ToolTurnReasoningReserveTokens = 128,
                SpecialistMaxStructuralEvidenceRecoveryUserTurns = 2
            }
        };

        var runtime = new SchemaThenAggregateRuntime();
        var chat = new QueuingChatClient(
            new AgentModelResponse
            {
                Model = "m",
                Content = string.Empty,
                FinishReason = "tool_calls",
                ToolCalls =
                [
                    new AgentToolCall { Id = "1", Name = "get_schema", ArgumentsJson = "{}" }
                ]
            },
            new AgentModelResponse
            {
                Model = "m",
                Content = "reasoning-only",
                ReasoningContent = new string('r', 80),
                FinishReason = "length",
                ToolCalls = []
            },
            new AgentModelResponse
            {
                Model = "m",
                Content = string.Empty,
                FinishReason = "tool_calls",
                ToolCalls =
                [
                    new AgentToolCall { Id = "2", Name = "group_and_aggregate", ArgumentsJson = "{}" }
                ]
            },
            new AgentModelResponse { Model = "m", Content = string.Empty, FinishReason = "stop", ToolCalls = [] },
            new AgentModelResponse
            {
                Model = "m",
                Content =
                    """
                    {"relevantColumns":[],"ambiguities":[],"evidences":[{"sourceTool":"group_and_aggregate","summary":"ok","supportingJsonFragment":null}],"keyMetrics":{},"objectiveObservations":[],"hypothesesOrCaveats":[],"reportSections":[],"analystNotes":"ok"}
                    """,
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
            ExpectsDatasetQueryEvidence: true);

        var result = await worker.ExecuteAsync(request, CancellationToken.None);

        Assert.Contains(
            result.ToolExecutions,
            e => e.ToolName.Equals("get_schema", StringComparison.OrdinalIgnoreCase));
        Assert.Contains(
            result.ToolExecutions,
            e => e.ToolName.Equals("group_and_aggregate", StringComparison.OrdinalIgnoreCase));

        var toolTurnRequests = chat.Requests.Where(r => r.EnableTools).ToArray();
        Assert.True(toolTurnRequests.Length >= 3, "truncation recovery must keep tools enabled for another turn before synthesis");

        var synthesisRequest = Assert.Single(chat.Requests, r => !r.EnableTools);
        Assert.False(synthesisRequest.EnableTools);
        Assert.NotEmpty(result.StructuredOutput.Evidences);
    }

    [Fact]
    public async Task ExecuteAsync_SynthesisPseudoToolCall_ReentersToolLoop()
    {
        var options = new AgentOptions
        {
            Model = "m",
            MultiAgent = new MultiAgentOrchestrationOptions
            {
                EnableSpecialistToolSelectionPlanning = false,
                SpecialistMaxToolIterations = 6,
                SpecialistToolCallMaxOutputTokens = 900
            }
        };

        var runtime = new SchemaThenAggregateRuntime();
        var chat = new QueuingChatClient(
            new AgentModelResponse
            {
                Model = "m",
                FinishReason = "tool_calls",
                ToolCalls =
                [
                    new AgentToolCall { Id = "1", Name = "get_schema", ArgumentsJson = "{}" }
                ]
            },
            new AgentModelResponse { Model = "m", Content = string.Empty, FinishReason = "stop", ToolCalls = [] },
            new AgentModelResponse
            {
                Model = "m",
                Content = """<|tool_call|>call:group_and_aggregate{"a":1}<|tool_call|>""",
                FinishReason = "stop"
            },
            new AgentModelResponse
            {
                Model = "m",
                FinishReason = "tool_calls",
                ToolCalls =
                [
                    new AgentToolCall { Id = "2", Name = "group_and_aggregate", ArgumentsJson = "{}" }
                ]
            },
            new AgentModelResponse { Model = "m", Content = string.Empty, FinishReason = "stop", ToolCalls = [] },
            new AgentModelResponse
            {
                Model = "m",
                Content =
                    """
                    {"relevantColumns":[],"ambiguities":[],"evidences":[{"sourceTool":"group_and_aggregate","summary":"ok","supportingJsonFragment":null}],"keyMetrics":{},"objectiveObservations":[],"hypothesesOrCaveats":[],"reportSections":[],"analystNotes":"ok"}
                    """,
                FinishReason = "stop"
            });

        var worker = new SpecialistToolAgentWorker(options, runtime, chat, NullLogger.Instance);
        var request = new AgentTaskRequest(
            AgentSpecialistKind.QueryAnalysis,
            "q",
            "d",
            new AgentConversationMemory(),
            Array.Empty<AgentConversationMessage>(),
            runtime.GetTools(),
            "m",
            "sys",
            ExpectsDatasetQueryEvidence: false);

        _ = await worker.ExecuteAsync(request, CancellationToken.None);

        var synthesisRequests = chat.Requests.Where(r => !r.EnableTools).ToArray();
        Assert.Equal(2, synthesisRequests.Length);
        var lastToolBeforeSecondSynthesis = chat.Requests[^2];
        Assert.True(lastToolBeforeSecondSynthesis.EnableTools);
    }

    [Fact]
    public async Task ExecuteAsync_QueryAnalysis_OnlyGetSchemaThenStop_TriggersDatasetEvidenceRecoveryPrompt()
    {
        var options = new AgentOptions
        {
            Model = "m",
            MultiAgent = new MultiAgentOrchestrationOptions
            {
                EnableSpecialistToolSelectionPlanning = false,
                SpecialistMaxToolIterations = 5,
                SpecialistMaxStructuralEvidenceRecoveryUserTurns = 1
            }
        };

        var runtime = new SchemaThenAggregateRuntime();
        var chat = new QueuingChatClient(
            new AgentModelResponse
            {
                Model = "m",
                FinishReason = "tool_calls",
                ToolCalls =
                [
                    new AgentToolCall { Id = "1", Name = "get_schema", ArgumentsJson = "{}" }
                ]
            },
            new AgentModelResponse { Model = "m", Content = string.Empty, FinishReason = "stop", ToolCalls = [] },
            new AgentModelResponse
            {
                Model = "m",
                FinishReason = "tool_calls",
                ToolCalls =
                [
                    new AgentToolCall { Id = "2", Name = "group_and_aggregate", ArgumentsJson = "{}" }
                ]
            },
            new AgentModelResponse { Model = "m", Content = string.Empty, FinishReason = "stop", ToolCalls = [] },
            new AgentModelResponse
            {
                Model = "m",
                Content =
                    """
                    {"relevantColumns":[],"ambiguities":[],"evidences":[{"sourceTool":"group_and_aggregate","summary":"ok","supportingJsonFragment":null}],"keyMetrics":{},"objectiveObservations":[],"hypothesesOrCaveats":[],"reportSections":[],"analystNotes":"ok"}
                    """,
                FinishReason = "stop"
            });

        var worker = new SpecialistToolAgentWorker(options, runtime, chat, NullLogger.Instance);
        var request = new AgentTaskRequest(
            AgentSpecialistKind.QueryAnalysis,
            "Aggregate question",
            "dispatch",
            new AgentConversationMemory(),
            Array.Empty<AgentConversationMessage>(),
            runtime.GetTools(),
            "m",
            "sys",
            ExpectsDatasetQueryEvidence: true);

        _ = await worker.ExecuteAsync(request, CancellationToken.None);

        var serialized = string.Join('|', chat.Requests.Select(r => JsonSerializer.Serialize(r.Messages)));
        Assert.Contains("Evidence recovery", serialized, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task ExecuteAsync_DiscoveryStructuralOnly_DoesNotForceDatasetEvidenceTurn()
    {
        var options = new AgentOptions
        {
            Model = "m",
            HostContextTokens = 16_000,
            ContextSlotTokens = 16_000,
            MultiAgent = new MultiAgentOrchestrationOptions
            {
                EnableSpecialistToolSelectionPlanning = false,
                SpecialistMaxToolIterations = 4,
                SpecialistMaxStructuralEvidenceRecoveryUserTurns = 2
            }
        };

        var runtime = new SchemaThenAggregateRuntime();
        var chat = new QueuingChatClient(
            new AgentModelResponse
            {
                Model = "m",
                FinishReason = "tool_calls",
                ToolCalls =
                [
                    new AgentToolCall { Id = "1", Name = "get_schema", ArgumentsJson = "{}" }
                ]
            },
            new AgentModelResponse { Model = "m", Content = string.Empty, FinishReason = "stop", ToolCalls = [] },
            new AgentModelResponse
            {
                Model = "m",
                Content =
                    """
                    {"relevantColumns":[],"ambiguities":[],"evidences":[],"keyMetrics":{},"objectiveObservations":[],"hypothesesOrCaveats":[],"reportSections":[],"analystNotes":"schema only"}
                    """,
                FinishReason = "stop"
            });

        var worker = new SpecialistToolAgentWorker(options, runtime, chat, NullLogger.Instance);
        var request = new AgentTaskRequest(
            AgentSpecialistKind.Discovery,
            "List columns",
            "structural",
            new AgentConversationMemory(),
            Array.Empty<AgentConversationMessage>(),
            runtime.GetTools(),
            "m",
            "sys",
            ExpectsDatasetQueryEvidence: false);

        _ = await worker.ExecuteAsync(request, CancellationToken.None);

        var toolTurns = chat.Requests.Count(r => r.EnableTools);
        Assert.Equal(2, toolTurns);
    }

    private sealed class BudgetFloorGuardChatClient : IAgentChatClient
    {
        private readonly QueuingChatClient _inner;
        private readonly int _minToolTurnMaxOutputTokensWhenToolsEnabled;

        public BudgetFloorGuardChatClient(QueuingChatClient inner, int minToolTurnMaxOutputTokensWhenToolsEnabled)
        {
            _inner = inner;
            _minToolTurnMaxOutputTokensWhenToolsEnabled = minToolTurnMaxOutputTokensWhenToolsEnabled;
        }

        public Task<AgentModelResponse> CompleteAsync(AgentModelRequest request, CancellationToken cancellationToken = default)
        {
            if (request.EnableTools
                && request.Tools.Count > 0
                && request.MaxOutputTokens < _minToolTurnMaxOutputTokensWhenToolsEnabled)
            {
                throw new InvalidOperationException(
                    $"Tool-enabled max_tokens {_minToolTurnMaxOutputTokensWhenToolsEnabled} violated: {request.MaxOutputTokens}");
            }

            return _inner.CompleteAsync(request, cancellationToken);
        }

        public Task<IReadOnlyList<string>> GetAvailableModelsAsync(CancellationToken cancellationToken = default)
            => _inner.GetAvailableModelsAsync(cancellationToken);
    }

    private sealed class ManyLargeSchemaToolsRuntime : IAgentToolRuntime
    {
        private readonly IReadOnlyList<AgentToolDefinition> _tools;

        public ManyLargeSchemaToolsRuntime(int toolCount, int schemaChars)
        {
            var filler = new string('p', Math.Max(64, schemaChars));
            var schema =
                "{\"type\":\"object\",\"description\":\"" + filler + "\",\"properties\":{\"x\":{\"type\":\"string\"}},\"additionalProperties\":false}";
            _tools = Enumerable.Range(0, toolCount)
                .Select(index => new AgentToolDefinition
                {
                    Name = $"tool_{index}",
                    Description = "d",
                    ParametersJsonSchema = schema
                })
                .Concat(new[]
                {
                    new AgentToolDefinition
                    {
                        Name = "get_schema",
                        Description = "schema",
                        ParametersJsonSchema = """{"type":"object","additionalProperties":false}"""
                    }
                })
                .ToArray();
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

    private sealed class GenericTwoToolRuntime : IAgentToolRuntime
    {
        public IReadOnlyList<AgentToolDefinition> GetTools()
            =>
            [
                new AgentToolDefinition
                {
                    Name = "get_schema",
                    Description = "schema",
                    ParametersJsonSchema = """{"type":"object","additionalProperties":false}"""
                },
                new AgentToolDefinition
                {
                    Name = "group_and_aggregate",
                    Description = "agg",
                    ParametersJsonSchema = """{"type":"object","additionalProperties":false}"""
                }
            ];

        public Task<AgentToolExecutionRecord> ExecuteAsync(string toolName, string argumentsJson, CancellationToken cancellationToken = default)
        {
            if (toolName.Equals("get_schema", StringComparison.OrdinalIgnoreCase))
            {
                return Task.FromResult(
                    new AgentToolExecutionRecord
                    {
                        ToolName = toolName,
                        ArgumentsJson = argumentsJson,
                        ResultJson =
                            """{"datasetName":"D","rowCount":3,"generatedAtUtc":"2020-01-01T00:00:00Z","columns":[{"name":"metric_a","dataType":"Decimal","isNullable":false,"isNumeric":true,"isCategorical":false},{"name":"metric_b","dataType":"Decimal","isNullable":false,"isNumeric":true,"isCategorical":false},{"name":"flag_c","dataType":"Boolean","isNullable":false,"isNumeric":false,"isCategorical":true}]}"""
                    });
            }

            return Task.FromResult(
                new AgentToolExecutionRecord
                {
                    ToolName = toolName,
                    ArgumentsJson = argumentsJson,
                    ResultJson = """{"rows":[{"values":{"bin":1}}]}"""
                });
        }
    }

    private sealed class QueuingChatClient : IAgentChatClient
    {
        private readonly Queue<AgentModelResponse> _queue;
        public List<AgentModelRequest> Requests { get; } = [];

        public QueuingChatClient(params AgentModelResponse[] responses)
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

    [Fact]
    public async Task Regression_PortugueseCombinationStub_SearchThenSchemaThenAggregate()
    {
        var options = new AgentOptions
        {
            Model = "m",
            HostContextTokens = 16_000,
            ContextSlotTokens = 16_000,
            EnableStructuredJsonOutputs = true,
            UseStrictJsonSchemaInResponseFormat = false,
            MultiAgent = new MultiAgentOrchestrationOptions
            {
                EnableSpecialistToolSelectionPlanning = false,
                SpecialistMaxToolIterations = 10,
                SpecialistMaxStructuralEvidenceRecoveryUserTurns = 2,
                SpecialistRecoveryPreferToolChoiceRequired = false
            }
        };

        var runtime = new SearchSchemaAggregateRuntime();
        var chat = new QueuingChatClient(
            new AgentModelResponse
            {
                Model = "m",
                FinishReason = "tool_calls",
                ToolCalls =
                [
                    new AgentToolCall
                    {
                        Id = "a",
                        Name = "search_columns",
                        ArgumentsJson = """{"keyword":"stub"}"""
                    }
                ]
            },
            new AgentModelResponse
            {
                Model = "m",
                FinishReason = "tool_calls",
                ToolCalls = [new AgentToolCall { Id = "b", Name = "get_schema", ArgumentsJson = "{}" }]
            },
            new AgentModelResponse
            {
                Model = "m",
                FinishReason = "tool_calls",
                ToolCalls =
                [
                    new AgentToolCall
                    {
                        Id = "c",
                        Name = "group_and_aggregate",
                        ArgumentsJson =
                            """{"aggregations":[{"alias":"row_count","function":"Count"}],"pageSize":50}"""
                    }
                ]
            },
            new AgentModelResponse { Model = "m", Content = string.Empty, FinishReason = "stop", ToolCalls = [] },
            new AgentModelResponse
            {
                Model = "m",
                Content =
                    """
                    {"relevantColumns":["c1"],"ambiguities":[],"evidences":[{"sourceTool":"group_and_aggregate","summary":"ok","supportingJsonFragment":null}],"keyMetrics":{"row_count":1},"objectiveObservations":[],"hypothesesOrCaveats":[],"reportSections":[],"analystNotes":"ok"}
                    """,
                FinishReason = "stop"
            });

        var worker = new SpecialistToolAgentWorker(options, runtime, chat, NullLogger.Instance);
        var request = new AgentTaskRequest(
            AgentSpecialistKind.QueryAnalysis,
            "qual a combinação de fator que voce considera maior causador de falhas e desgaste na maquina ?",
            "dispatch",
            new AgentConversationMemory(),
            Array.Empty<AgentConversationMessage>(),
            runtime.GetTools(),
            "m",
            MultiAgentPromptBuilder.BuildSpecialistSystemPrompt(AgentSpecialistKind.QueryAnalysis, options),
            RequiredEvidenceKinds:
            [
                AgentEvidenceKind.StructuralSchema,
                AgentEvidenceKind.Aggregate
            ]);

        var result = await worker.ExecuteAsync(request, CancellationToken.None);

        Assert.Contains(
            result.ToolExecutions,
            e => e.ToolName.Equals("search_columns", StringComparison.OrdinalIgnoreCase));
        Assert.Contains(
            result.ToolExecutions,
            e => e.ToolName.Equals("get_schema", StringComparison.OrdinalIgnoreCase));
        Assert.Contains(
            result.ToolExecutions,
            e => e.ToolName.Equals("group_and_aggregate", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void OutOfSurfaceToolResultJson_IncludesValidationPayload()
    {
        var exposed = new List<AgentToolDefinition>
        {
            new()
            {
                Name = "group_and_aggregate",
                ParametersJsonSchema = """{"type":"object"}"""
            }
        };
        var scoped = new List<AgentToolDefinition>
        {
            new()
            {
                Name = "get_schema",
                ParametersJsonSchema = """{"type":"object","additionalProperties":false}"""
            },
            exposed[0]
        };

        var json = SpecialistToolSurfaceValidation.BuildOutOfSurfaceToolResultJson(
            "get_schema",
            exposed,
            scoped,
            useFullToolSchemas: true);

        Assert.Contains("tool_not_on_exposed_surface", json, StringComparison.Ordinal);
        Assert.Contains("group_and_aggregate", json, StringComparison.OrdinalIgnoreCase);
    }

    private sealed class SearchSchemaAggregateRuntime : IAgentToolRuntime
    {
        public IReadOnlyList<AgentToolDefinition> GetTools()
            =>
            [
                new AgentToolDefinition
                {
                    Name = "search_columns",
                    Description = "search",
                    ParametersJsonSchema =
                        """{"type":"object","properties":{"keyword":{"type":"string"}},"required":["keyword"],"additionalProperties":false}"""
                },
                new AgentToolDefinition
                {
                    Name = "get_schema",
                    Description = "schema",
                    ParametersJsonSchema = """{"type":"object","additionalProperties":false}"""
                },
                new AgentToolDefinition
                {
                    Name = "group_and_aggregate",
                    Description = "agg",
                    ParametersJsonSchema = """{"type":"object","additionalProperties":false}"""
                }
            ];

        public Task<AgentToolExecutionRecord> ExecuteAsync(string toolName, string argumentsJson, CancellationToken cancellationToken = default)
        {
            if (toolName.Equals("search_columns", StringComparison.OrdinalIgnoreCase))
            {
                return Task.FromResult(
                    new AgentToolExecutionRecord
                    {
                        ToolName = toolName,
                        ArgumentsJson = argumentsJson,
                        ResultJson = """{"keyword":"stub","matches":[]}"""
                    });
            }

            if (toolName.Equals("get_schema", StringComparison.OrdinalIgnoreCase))
            {
                return Task.FromResult(
                    new AgentToolExecutionRecord
                    {
                        ToolName = toolName,
                        ArgumentsJson = argumentsJson,
                        ResultJson =
                            """{"datasetName":"stub","rowCount":100,"generatedAtUtc":"2020-01-01T00:00:00Z","columns":[{"name":"col_a","dataType":"Double","isNullable":false,"isNumeric":true,"isCategorical":false},{"name":"flag_b","dataType":"Boolean","isNullable":false,"isNumeric":false,"isCategorical":true}]}"""
                    });
            }

            return Task.FromResult(
                new AgentToolExecutionRecord
                {
                    ToolName = toolName,
                    ArgumentsJson = argumentsJson,
                    ResultJson = """{"rows":[{"values":{"row_count":10}}],"aggregations":[]}"""
                });
        }
    }

    private sealed class SchemaThenAggregateRuntime : IAgentToolRuntime
    {
        public IReadOnlyList<AgentToolDefinition> GetTools()
            =>
            [
                new AgentToolDefinition
                {
                    Name = "get_schema",
                    Description = "schema",
                    ParametersJsonSchema = """{"type":"object","additionalProperties":false}"""
                },
                new AgentToolDefinition
                {
                    Name = "group_and_aggregate",
                    Description = "agg",
                    ParametersJsonSchema = """{"type":"object","additionalProperties":false}"""
                }
            ];

        public Task<AgentToolExecutionRecord> ExecuteAsync(string toolName, string argumentsJson, CancellationToken cancellationToken = default)
        {
            if (toolName.Equals("get_schema", StringComparison.OrdinalIgnoreCase))
            {
                return Task.FromResult(
                    new AgentToolExecutionRecord
                    {
                        ToolName = toolName,
                        ArgumentsJson = argumentsJson,
                        ResultJson =
                            """{"datasetName":"AI4I","rowCount":10000,"generatedAtUtc":"2020-01-01T00:00:00Z","columns":[{"name":"Air temperature [K]","dataType":"Decimal","isNullable":false,"isNumeric":true,"isCategorical":false},{"name":"Process temperature [K]","dataType":"Decimal","isNullable":false,"isNumeric":true,"isCategorical":false},{"name":"Machine failure","dataType":"Boolean","isNullable":false,"isNumeric":false,"isCategorical":true}]}"""
                    });
            }

            return Task.FromResult(
                new AgentToolExecutionRecord
                {
                    ToolName = toolName,
                    ArgumentsJson = argumentsJson,
                    ResultJson = """{"rows":[{"values":{"bin":1}}]}"""
                });
        }
    }
}
