using MachineHealthExplorer.Agent.Abstractions;
using MachineHealthExplorer.Agent.Models;
using MachineHealthExplorer.Agent.MultiAgent;
using Microsoft.Extensions.Logging.Abstractions;

namespace MachineHealthExplorer.Tests;

/// <summary>
/// End-to-end style checks without domain-specific production code: structured fallback must carry
/// row_count, conditional count semantics, derived rate, and multi-key grouping metadata for the composer payload.
/// </summary>
public sealed class SyntheticComposerEvidencePipelineTests
{
    [Fact]
    public async Task FinalComposerPayload_FromToolFallback_CarriesRateAndMultiKeyEvidence()
    {
        var args =
            """{"groupByColumns":["dim_a","dim_b"],"aggregations":[{"alias":"row_count","function":"Count"},{"alias":"sub_count","function":"Count","filter":{"columnName":"flag","operator":"Equals","value":true}}],"derivedMetrics":[{"alias":"sub_rate","expression":"sub_count / row_count"}]}""";
        var rows =
            """{"columns":["dim_a","dim_b","row_count","sub_count","sub_rate"],"rows":[{"dim_a":"A","dim_b":"X","row_count":40,"sub_count":5,"sub_rate":0.125}],"totalGroups":1,"scopedRowCount":40,"page":1,"pageSize":50}""";
        var exec = new AgentToolExecutionRecord
        {
            ToolName = "group_and_aggregate",
            ArgumentsJson = args,
            ResultJson = rows,
            IsError = false
        };

        var structured = SpecialistStructuredOutputParser.FromToolFallback(
            AgentSpecialistKind.QueryAnalysis,
            [exec],
            toolEvidenceMaxChars: 12_000);

        var payloadJson = System.Text.Json.JsonSerializer.Serialize(new
        {
            specialistResults = new[]
            {
                new
                {
                    specialist = "QueryAnalysis",
                    success = true,
                    failure = (string?)null,
                    structured
                }
            }
        });

        Assert.Contains("sub_rate", payloadJson, StringComparison.Ordinal);
        Assert.Contains("perAggregationFilterPresent", payloadJson, StringComparison.Ordinal);
        Assert.Contains("dim_b", payloadJson, StringComparison.Ordinal);

        var recording = new SingleResponseChatClient(
            new AgentModelResponse { Model = "m", Content = "Resposta baseada apenas nas evidências.", FinishReason = "stop" });

        var composer = new FinalComposerAgent(
            new AgentOptions { Model = "m" },
            recording,
            NullLogger.Instance);

        await composer.ComposeFirstResponseAsync(
                new FinalComposerInput(
                    OriginalUserQuestion: "Ranking quantitativo genérico entre dimensões.",
                    DetectedLanguage: "pt",
                    ConversationRollingSummary: null,
                    SpecialistResults:
                    [
                        new AgentTaskResult(
                            AgentSpecialistKind.QueryAnalysis,
                            Success: true,
                            FailureMessage: null,
                            ToolExecutions: [exec],
                            structured,
                            SpecialistScratchTranscript: Array.Empty<AgentConversationMessage>())
                    ],
                    RecentUserAssistantTail: Array.Empty<AgentConversationMessage>(),
                    SchemaColumnNamesFromTools: null),
                "m",
                new AgentConversationMemory(),
                CancellationToken.None);

        Assert.NotNull(recording.LastRequest);
    }

    private sealed class SingleResponseChatClient : IAgentChatClient
    {
        private readonly AgentModelResponse _response;
        public AgentModelRequest? LastRequest { get; private set; }

        public SingleResponseChatClient(AgentModelResponse response) => _response = response;

        public Task<AgentModelResponse> CompleteAsync(AgentModelRequest request, CancellationToken cancellationToken = default)
        {
            LastRequest = request;
            return Task.FromResult(_response);
        }

        public Task<IReadOnlyList<string>> GetAvailableModelsAsync(CancellationToken cancellationToken = default)
            => Task.FromResult<IReadOnlyList<string>>(Array.Empty<string>());
    }
}
