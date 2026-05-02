using MachineHealthExplorer.Agent.Abstractions;
using MachineHealthExplorer.Agent.Models;
using MachineHealthExplorer.Agent.MultiAgent;
using MachineHealthExplorer.Agent.Services;
using MachineHealthExplorer.Domain.Abstractions;
using MachineHealthExplorer.Domain.Models;
using MachineHealthExplorer.Tools.Services;
using Microsoft.Extensions.Logging.Abstractions;
using System.Text.Json;

namespace MachineHealthExplorer.Tests;

public sealed class MultiAgentModelDrivenTests
{
    [Fact]
    public async Task Coordinator_TryPlanAsync_WhenLlmReturnsInvalidAndTruncated_RetriesUntilValidPlan()
    {
        var options = new AgentOptions
        {
            Model = "m",
            MultiAgent = new MultiAgentOrchestrationOptions
            {
                EnableCoordinatorLlmPlanning = true,
                CoordinatorPlannerMaxRecoveryAttempts = 3,
                CoordinatorPlannerMaxOutputTokens = 512,
                CoordinatorPlannerRecoveryMaxOutputTokens = 256
            }
        };

        var chat = new ModelDrivenQueuingChatClient(
            new AgentModelResponse { Model = "m", Content = "not-json", FinishReason = "stop" },
            new AgentModelResponse { Model = "m", Content = """{"notes":"x"}""", FinishReason = "length" },
            new AgentModelResponse
            {
                Model = "m",
                Content = """{"steps":[{"specialist":"QueryAnalysis","reason":"quant","parallel_group":0}]}""",
                FinishReason = "stop"
            });

        var coordinator = new CoordinatorAgent(options, chat, NullLogger.Instance);
        var result = await coordinator.TryPlanAsync(
                "m",
                "qual a faixa de temperatura em que ocorre mais falhas ?",
                new AgentConversationMemory(),
                Array.Empty<AgentConversationMessage>(),
                CancellationToken.None);

        Assert.True(result.Success);
        Assert.Single(result.Plan.Steps);
        Assert.Equal(AgentSpecialistKind.QueryAnalysis, result.Plan.Steps[0].SpecialistKind);
        Assert.Null(result.UserVisibleFailureMessage);
        Assert.Equal(3, chat.Requests.Count);

        for (var i = 1; i < chat.Requests.Count; i++)
        {
            Assert.Contains(
                "previous response failed validation",
                chat.Requests[i].SystemPrompt,
                StringComparison.OrdinalIgnoreCase);
        }
    }

    [Fact]
    public async Task Coordinator_TryPlanAsync_AcceptsEmptyPlan_ForNonAnalyticalTurn()
    {
        var options = new AgentOptions
        {
            Model = "m",
            MultiAgent = new MultiAgentOrchestrationOptions
            {
                EnableCoordinatorLlmPlanning = true
            }
        };

        var chat = new ModelDrivenQueuingChatClient(
            new AgentModelResponse
            {
                Model = "m",
                Content = """{"steps":[],"notes":"greeting only"}""",
                FinishReason = "stop"
            });

        var coordinator = new CoordinatorAgent(options, chat, NullLogger.Instance);
        var result = await coordinator.TryPlanAsync(
                "m",
                "ola",
                new AgentConversationMemory(),
                Array.Empty<AgentConversationMessage>(),
                CancellationToken.None);

        Assert.True(result.Success);
        Assert.Empty(result.Plan.Steps);
        Assert.Null(result.UserVisibleFailureMessage);
        Assert.Single(chat.Requests);
    }

    [Fact]
    public async Task MultiAgentSessionEngine_Greeting_BypassesModelCalls()
    {
        var chat = new ModelDrivenRecordingChatClient();
        var engine = new MultiAgentSessionEngine(
            new AgentOptions
            {
                MultiAgent = new MultiAgentOrchestrationOptions
                {
                    EnableCoordinatorLlmPlanning = true
                }
            },
            chat,
            new NoopToolRuntime(),
            NullLogger.Instance);

        var result = await engine.RunAsync(
            new AgentExecutionContext { UserInput = "ola" },
            CancellationToken.None);

        Assert.Empty(chat.Requests);
        Assert.Empty(result.ToolExecutions);
        Assert.Equal("pt", result.UpdatedConversationMemory?.LanguagePreference);
        Assert.Contains("Como posso ajudar", result.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Equal("direct_casual_turn", result.MultiAgentTrace?.Plan.CoordinatorNotes);
    }

    [Fact]
    public void DatasetAgentToolRuntime_SearchColumnsAndGroupAggregateSchemas_ExposeContractFields()
    {
        var engine = new MinimalStubAnalyticsEngine();
        var toolService = new DatasetToolService(engine);
        var runtime = new DatasetAgentToolRuntime(new DatasetToolCatalog(), toolService);
        var tools = runtime.GetTools().ToDictionary(tool => tool.Name, StringComparer.OrdinalIgnoreCase);

        using (var searchDoc = JsonDocument.Parse(tools["search_columns"].ParametersJsonSchema))
        {
            var props = searchDoc.RootElement.GetProperty("properties");
            Assert.True(props.TryGetProperty("keyword", out _));
            var required = searchDoc.RootElement.GetProperty("required");
            Assert.Contains("keyword", required.EnumerateArray().Select(e => e.GetString()));
        }

        using (var groupDoc = JsonDocument.Parse(tools["group_and_aggregate"].ParametersJsonSchema))
        {
            var props = groupDoc.RootElement.GetProperty("properties");
            var bins = props.GetProperty("groupByBins");
            Assert.Equal("array", bins.GetProperty("type").GetString());
            var items = bins.GetProperty("items");
            var req = items.GetProperty("required");
            var names = req.EnumerateArray().Select(e => e.GetString()).OrderBy(s => s).ToArray();
            Assert.Equal(new[] { "alias", "binWidth", "columnName" }, names);

            var ag = props.GetProperty("aggregations");
            Assert.Equal("array", ag.GetProperty("type").GetString());
            var agItem = ag.GetProperty("items");
            Assert.Contains(
                "function",
                agItem.GetProperty("required").EnumerateArray().Select(e => e.GetString()));

            var sort = props.GetProperty("sortRules");
            Assert.Equal("array", sort.GetProperty("type").GetString());
            var sortItem = sort.GetProperty("items");
            Assert.True(sortItem.GetProperty("properties").TryGetProperty("columnName", out _));
            Assert.True(props.TryGetProperty("derivedMetrics", out _));
            Assert.True(props.TryGetProperty("groupByAutoBins", out _));
        }
    }

    [Fact]
    public async Task SpecialistToolAgentWorker_OnToolArgumentError_IncludesSchemaAndErrorInNextModelRequest()
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

        var runtime = new KeywordMistakeSearchColumnsRuntime();
        var chat = new ModelDrivenQueuingChatClient(
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
                        Name = "search_columns",
                        ArgumentsJson = """{"keywords":"temperature"}"""
                    }
                ]
            },
            new AgentModelResponse { Model = "m", Content = string.Empty, FinishReason = "stop", ToolCalls = [] },
            new AgentModelResponse
            {
                Model = "m",
                Content =
                    """
                    {"relevantColumns":["Air temperature [K]"],"ambiguities":[],"evidences":[],"keyMetrics":{},"objectiveObservations":[],"hypothesesOrCaveats":[],"reportSections":[],"analystNotes":"ok"}
                    """,
                FinishReason = "stop"
            });

        var worker = new SpecialistToolAgentWorker(options, runtime, chat, NullLogger.Instance);
        var request = new AgentTaskRequest(
            AgentSpecialistKind.QueryAnalysis,
            "pergunta",
            "test",
            new AgentConversationMemory(),
            Array.Empty<AgentConversationMessage>(),
            runtime.GetTools(),
            "m",
            "You are QueryAnalysis.",
            UseFullToolSchemas: true,
            ExpectsDatasetQueryEvidence: false);

        _ = await worker.ExecuteAsync(request, CancellationToken.None);

        Assert.True(chat.Requests.Count >= 2);
        var secondSpecialistRequest = chat.Requests[1];
        var serialized = JsonSerializer.Serialize(secondSpecialistRequest.Messages);
        Assert.Contains("tool_error", serialized, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("expected_parameters_json_schema", serialized, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("keyword", serialized, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void SchemaColumnNamesFromToolExecutions_IncludesNamesFromGetSchema()
    {
        var executions = new[]
        {
            new AgentToolExecutionRecord
            {
                ToolName = "get_schema",
                ArgumentsJson = "{}",
                ResultJson =
                    """{"datasetName":"AI4I","rowCount":1,"generatedAtUtc":"2020-01-01T00:00:00Z","columns":[{"name":"Air temperature [K]","dataType":"Double","isNullable":false,"isNumeric":true,"isCategorical":false},{"name":"Machine failure","dataType":"Boolean","isNullable":false,"isNumeric":false,"isCategorical":true}]}"""
            }
        };

        var names = SchemaColumnNamesFromToolExecutions.Extract(executions);
        Assert.Contains("Air temperature [K]", names);
        Assert.Contains("Machine failure", names);
    }

    [Fact]
    public async Task FinalComposer_payload_includes_schema_column_hints()
    {
        var recording = new ModelDrivenRecordingChatClient(
            new AgentModelResponse { Model = "m", Content = "Resposta.", FinishReason = "stop" });

        var composer = new FinalComposerAgent(
            new AgentOptions { Model = "m" },
            recording,
            NullLogger.Instance);

        await composer.ComposeFirstResponseAsync(
                new FinalComposerInput(
                    OriginalUserQuestion: "qual a faixa de temperatura em que ocorre mais falhas ?",
                    DetectedLanguage: "pt",
                    ConversationRollingSummary: null,
                    SpecialistResults: Array.Empty<AgentTaskResult>(),
                    RecentUserAssistantTail: Array.Empty<AgentConversationMessage>(),
                    SchemaColumnNamesFromTools: ["Air temperature [K]", "Process temperature [K]"]),
                "m",
                new AgentConversationMemory(),
                CancellationToken.None);

        var userPayload = recording.Requests[^1].Messages[^1].Content ?? string.Empty;
        Assert.Contains("schemaColumnNamesFromTools", userPayload, StringComparison.Ordinal);
        Assert.Contains("Air temperature [K]", userPayload, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Regression_portuguese_temperature_bins_composer_payload_includes_schema_hints_and_tool_errors()
    {
        var getSchema = new AgentToolExecutionRecord
        {
            ToolName = "get_schema",
            ArgumentsJson = "{}",
            ResultJson =
                """{"datasetName":"AI4I","rowCount":10000,"generatedAtUtc":"2020-01-01T00:00:00Z","columns":[{"name":"Air temperature [K]","dataType":"Double","isNullable":false,"isNumeric":true,"isCategorical":false},{"name":"Machine failure","dataType":"Boolean","isNullable":false,"isNumeric":false,"isCategorical":true}]}"""
        };
        var badGroup = new AgentToolExecutionRecord
        {
            ToolName = "group_and_aggregate",
            ArgumentsJson = """{"groupByBins":{}}""",
            ResultJson = """{"error":"group_and_aggregate: invalid tool arguments JSON (groupByBins must be array)"}""",
            IsError = true
        };

        var structured = SpecialistStructuredOutputParser.FromToolFallback(
            AgentSpecialistKind.QueryAnalysis,
            [getSchema, badGroup],
            toolEvidenceMaxChars: 4000);

        var schemaHints = SchemaColumnNamesFromToolExecutions.Extract([getSchema, badGroup]);

        var recording = new ModelDrivenRecordingChatClient(
            new AgentModelResponse { Model = "m", Content = "ok", FinishReason = "stop" });

        var composer = new FinalComposerAgent(
            new AgentOptions { Model = "m" },
            recording,
            NullLogger.Instance);

        await composer.ComposeFirstResponseAsync(
                new FinalComposerInput(
                    OriginalUserQuestion: "em qual faixa de temperatura ocorre mais falhas?",
                    DetectedLanguage: "pt",
                    ConversationRollingSummary: null,
                    SpecialistResults:
                    [
                        new AgentTaskResult(
                            AgentSpecialistKind.QueryAnalysis,
                            Success: true,
                            FailureMessage: null,
                            ToolExecutions: [getSchema, badGroup],
                            StructuredOutput: structured,
                            SpecialistScratchTranscript: Array.Empty<AgentConversationMessage>())
                    ],
                    RecentUserAssistantTail: Array.Empty<AgentConversationMessage>(),
                    SchemaColumnNamesFromTools: schemaHints),
                "m",
                new AgentConversationMemory(),
                CancellationToken.None);

        var payload = recording.Requests[0].Messages[^1].Content ?? string.Empty;
        Assert.Contains("Air temperature [K]", payload, StringComparison.Ordinal);
        Assert.Contains("group_and_aggregate", payload, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("invalid tool arguments", payload, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("schemaColumnNamesFromTools", payload, StringComparison.Ordinal);
    }

    [Fact]
    public async Task MultiAgentSessionEngine_FakeCombinationQuestion_RunsAggregateBeforeFinalComposer()
    {
        const string userQuestion =
            "qual a combinação de fator que voce considera maior causador de falhas e desgaste na maquina ?";

        var options = new AgentOptions
        {
            Model = "m",
            HostContextTokens = 24_000,
            ContextSlotTokens = 24_000,
            MultiAgent = new MultiAgentOrchestrationOptions
            {
                EnableCoordinatorLlmPlanning = false,
                EnableSpecialistToolSelectionPlanning = false,
                SpecialistMaxToolIterations = 6,
                SpecialistMaxStructuralEvidenceRecoveryUserTurns = 2,
                FinalComposerReasoningReserveTokens = 900
            }
        };

        var chat = new ModelDrivenQueuingChatClient(
            new AgentModelResponse
            {
                Model = "m",
                Content = string.Empty,
                FinishReason = "tool_calls",
                ToolCalls =
                [
                    new AgentToolCall
                    {
                        Id = "t1",
                        Name = "group_and_aggregate",
                        ArgumentsJson =
                            """{"groupByColumns":[],"aggregations":[{"alias":"n","function":"Count"}],"pageSize":50}"""
                    }
                ]
            },
            new AgentModelResponse { Model = "m", Content = "DONE_NO_MORE_TOOLS", FinishReason = "stop" },
            new AgentModelResponse
            {
                Model = "m",
                Content =
                    """{"relevantColumns":[],"ambiguities":[],"evidences":[],"keyMetrics":{},"objectiveObservations":[],"hypothesesOrCaveats":[],"reportSections":[],"analystNotes":"ok"}""",
                FinishReason = "stop"
            },
            new AgentModelResponse
            {
                Model = "m",
                Content =
                    "Com base nas contagens agregadas, o padrão observado é associação entre categorias, não causalidade forte.",
                FinishReason = "stop"
            });

        var engine = new MinimalStubAnalyticsEngine();
        var toolService = new DatasetToolService(engine);
        var runtime = new DatasetAgentToolRuntime(new DatasetToolCatalog(), toolService);

        var session = new MultiAgentSessionEngine(options, chat, runtime, NullLogger.Instance);
        var result = await session.RunAsync(new AgentExecutionContext { UserInput = userQuestion }, CancellationToken.None);

        Assert.Contains(
            result.ToolExecutions,
            execution =>
                execution.ToolName.Equals("group_and_aggregate", StringComparison.OrdinalIgnoreCase)
                && !execution.IsError);

        Assert.Contains("associação", result.Message, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("Encerramento técnico", result.Message, StringComparison.OrdinalIgnoreCase);
        Assert.True(chat.Requests.Count <= 7, $"expected few LLM calls, got {chat.Requests.Count}");
    }

    [Fact]
    public void FinalComposer_system_prompt_forbids_false_absence_claims_when_schema_hints_exist()
    {
        var prompt = MultiAgentPromptBuilder.BuildFinalComposerSystemPrompt(new AgentOptions());
        Assert.Contains("schemaColumnNamesFromTools", prompt, StringComparison.Ordinal);
        Assert.Contains("tool_error", prompt, StringComparison.OrdinalIgnoreCase);
    }

    private sealed class MinimalStubAnalyticsEngine : IDatasetAnalyticsEngine
    {
        public Task<DatasetSchema> GetSchemaAsync(CancellationToken cancellationToken = default)
            => Task.FromResult(new DatasetSchema { DatasetName = "stub" });

        public Task<ColumnProfile> GetColumnProfileAsync(string columnName, CancellationToken cancellationToken = default)
            => Task.FromResult(new ColumnProfile { ColumnName = columnName });

        public Task<SearchColumnsResult> SearchColumnsAsync(string keyword, CancellationToken cancellationToken = default)
            => Task.FromResult(new SearchColumnsResult { Keyword = keyword });

        public Task<QueryResult> QueryRowsAsync(QueryRequest request, CancellationToken cancellationToken = default)
            => Task.FromResult(new QueryResult());

        public Task<ColumnExtremaResult> FindColumnExtremaRowsAsync(ColumnExtremaRequest request, CancellationToken cancellationToken = default)
            => Task.FromResult(new ColumnExtremaResult());

        public Task<DistinctValuesResult> GetDistinctValuesAsync(DistinctValuesRequest request, CancellationToken cancellationToken = default)
            => Task.FromResult(new DistinctValuesResult { ColumnName = request.ColumnName });

        public Task<GroupAggregationResult> GroupAndAggregateAsync(GroupAggregationRequest request, CancellationToken cancellationToken = default)
            => Task.FromResult(new GroupAggregationResult());

        public Task<NumericSummary> GetNumericSummaryAsync(string columnName, FilterExpression? filter = null, CancellationToken cancellationToken = default)
            => Task.FromResult(new NumericSummary { ColumnName = columnName });

        public Task<CategoricalSummary> GetCategoricalSummaryAsync(string columnName, FilterExpression? filter = null, int top = 10, CancellationToken cancellationToken = default)
            => Task.FromResult(new CategoricalSummary { ColumnName = columnName });

        public Task<SubsetComparisonResult> CompareSubsetsAsync(SubsetComparisonRequest request, CancellationToken cancellationToken = default)
            => Task.FromResult(new SubsetComparisonResult());

        public Task<DatasetDescription> DescribeDatasetAsync(CancellationToken cancellationToken = default)
            => Task.FromResult(new DatasetDescription { DatasetName = "stub" });

        public Task<DatasetReport> BuildReportAsync(ReportRequest request, CancellationToken cancellationToken = default)
            => Task.FromResult(new DatasetReport());

        public Task<ColumnProfilingResult> ProfileColumnsAsync(ColumnProfilingRequest request, CancellationToken cancellationToken = default)
            => Task.FromResult(new ColumnProfilingResult());
    }

    private sealed class KeywordMistakeSearchColumnsRuntime : IAgentToolRuntime
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
                    ParametersJsonSchema = """{"type":"object","properties":{},"additionalProperties":false}"""
                }
            ];

        public Task<AgentToolExecutionRecord> ExecuteAsync(string toolName, string argumentsJson, CancellationToken cancellationToken = default)
        {
            if (toolName.Equals("search_columns", StringComparison.OrdinalIgnoreCase)
                && argumentsJson.Contains("keywords", StringComparison.OrdinalIgnoreCase))
            {
                return Task.FromResult(
                    new AgentToolExecutionRecord
                    {
                        ToolName = toolName,
                        ArgumentsJson = argumentsJson,
                        ResultJson = """{"error":"search_columns: invalid tool arguments JSON (missing keyword)"}""",
                        IsError = true
                    });
            }

            return Task.FromResult(
                new AgentToolExecutionRecord
                {
                    ToolName = toolName,
                    ArgumentsJson = argumentsJson,
                    ResultJson = """{"matches":[]}"""
                });
        }
    }

    private sealed class NoopToolRuntime : IAgentToolRuntime
    {
        public IReadOnlyList<AgentToolDefinition> GetTools()
            => Array.Empty<AgentToolDefinition>();

        public Task<AgentToolExecutionRecord> ExecuteAsync(string toolName, string argumentsJson, CancellationToken cancellationToken = default)
            => throw new InvalidOperationException("No tools are available in this test runtime.");
    }

    private sealed class ModelDrivenQueuingChatClient : IAgentChatClient
    {
        private readonly Queue<AgentModelResponse> _responses;
        public List<AgentModelRequest> Requests { get; } = [];

        public ModelDrivenQueuingChatClient(params AgentModelResponse[] responses)
        {
            _responses = new Queue<AgentModelResponse>(responses);
        }

        public Task<AgentModelResponse> CompleteAsync(AgentModelRequest request, CancellationToken cancellationToken = default)
        {
            Requests.Add(request);
            if (_responses.Count == 0)
            {
                throw new InvalidOperationException("Test chat client response queue exhausted.");
            }

            return Task.FromResult(_responses.Dequeue());
        }

        public Task<IReadOnlyList<string>> GetAvailableModelsAsync(CancellationToken cancellationToken = default)
            => Task.FromResult<IReadOnlyList<string>>(["m"]);
    }

    private sealed class ModelDrivenRecordingChatClient : IAgentChatClient
    {
        private readonly Queue<AgentModelResponse> _responses;
        public List<AgentModelRequest> Requests { get; } = [];

        public ModelDrivenRecordingChatClient(params AgentModelResponse[] responses)
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
}
