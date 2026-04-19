using MachineHealthExplorer.Agent.Abstractions;
using MachineHealthExplorer.Agent.Models;
using MachineHealthExplorer.Agent.Serialization;
using MachineHealthExplorer.Agent.Services;
using MachineHealthExplorer.Domain.Models;
using MachineHealthExplorer.Tools.Abstractions;
using Microsoft.Extensions.Logging.Abstractions;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace MachineHealthExplorer.Tests;

public sealed class AgentRuntimeTests
{
    [Fact]
    public async Task Orchestrator_ExecutesToolCalls_AndReturnsFinalAssistantMessage()
    {
        var chatClient = new StubAgentChatClient(
            new AgentModelResponse
            {
                Model = "qwen-test",
                ToolCalls =
                [
                    new AgentToolCall
                    {
                        Id = "tool_1",
                        Name = "describe_dataset",
                        ArgumentsJson = "{}"
                    }
                ]
            },
            new AgentModelResponse
            {
                Model = "qwen-test",
                Content = "The dataset has 10,000 rows and 14 columns."
            });

        var toolRuntime = new StubAgentToolRuntime();
        var orchestrator = new LmStudioAgentOrchestrator(
            new AgentOptions
            {
                Model = "qwen-test",
                MaxToolIterations = 3,
                EnableWorkerPasses = false,
                EnableContextCompaction = false
            },
            chatClient,
            toolRuntime,
            NullLoggerFactory.Instance);

        var result = await orchestrator.RunAsync(new AgentExecutionContext
        {
            UserInput = "Summarize the dataset."
        });

        Assert.True(result.IsImplemented);
        Assert.Equal("qwen-test", result.Model);
        Assert.Equal("The dataset has 10,000 rows and 14 columns.", result.Message);
        Assert.Single(result.ToolExecutions);
        Assert.Collection(
            result.UpdatedConversation,
            message => Assert.Equal(AgentConversationRole.User, message.Role),
            message =>
            {
                Assert.Equal(AgentConversationRole.Assistant, message.Role);
                Assert.Single(message.ToolCalls);
            },
            message =>
            {
                Assert.Equal(AgentConversationRole.Tool, message.Role);
                Assert.Equal("describe_dataset", message.Name);
                Assert.Contains("mhe_tool_evidence_v1", message.Content ?? string.Empty, StringComparison.Ordinal);
            },
            message =>
            {
                Assert.Equal(AgentConversationRole.Assistant, message.Role);
                Assert.Equal("The dataset has 10,000 rows and 14 columns.", message.Content);
            });
    }

    [Fact]
    public async Task ToolRuntime_DeserializesNestedFilters_ForQueryTools()
    {
        var toolService = new StubDatasetToolService();
        var runtime = new DatasetAgentToolRuntime(new StubDatasetToolCatalog(), toolService);

        const string argumentsJson = """
        {
          "filter": {
            "operator": "And",
            "expressions": [
              {
                "columnName": "Machine failure",
                "operator": "Equals",
                "value": true
              },
              {
                "columnName": "Torque [Nm]",
                "operator": "GreaterThanOrEqual",
                "value": 50
              }
            ]
          },
          "selectedColumns": ["Type", "Torque [Nm]"],
          "page": 1,
          "pageSize": 5
        }
        """;

        var result = await runtime.ExecuteAsync("query_rows", argumentsJson);

        Assert.False(result.IsError);
        Assert.NotNull(toolService.LastQueryRequest);
        var filterGroup = Assert.IsType<FilterGroupExpression>(toolService.LastQueryRequest!.Filter);
        Assert.Equal(LogicalOperator.And, filterGroup.Operator);
        var conditions = filterGroup.Expressions.Cast<FilterConditionExpression>().ToArray();
        Assert.Equal("Machine failure", conditions[0].ColumnName);
        Assert.Equal(true, conditions[0].Value);
        Assert.Equal("Torque [Nm]", conditions[1].ColumnName);
        Assert.Equal(50L, conditions[1].Value);
    }

    [Fact]
    public async Task LmStudioChatClient_ParsesSnakeCaseToolCalls_FromLmStudioResponse()
    {
        const string responseJson = """
        {
          "id": "chatcmpl-test",
          "object": "chat.completion",
          "created": 1776549825,
          "model": "google/gemma-4-e4b",
          "choices": [
            {
              "index": 0,
              "message": {
                "role": "assistant",
                "content": "",
                "tool_calls": [
                  {
                    "id": "414934148",
                    "type": "function",
                    "function": {
                      "name": "describe_dataset",
                      "arguments": "{}"
                    }
                  }
                ]
              },
              "finish_reason": "tool_calls"
            }
          ]
        }
        """;

        using var httpClient = new HttpClient(new StubHttpMessageHandler(responseJson))
        {
            BaseAddress = new Uri("http://127.0.0.1:1234/v1/")
        };

        using var chatClient = new LmStudioChatClient(
            new AgentOptions
            {
                BaseUrl = "http://127.0.0.1:1234/v1",
                Model = "google/gemma-4-e4b"
            },
            httpClient);

        var response = await chatClient.CompleteAsync(new AgentModelRequest
        {
            Model = "google/gemma-4-e4b",
            SystemPrompt = "You are a test agent.",
            Messages =
            [
                new AgentConversationMessage
                {
                    Role = AgentConversationRole.User,
                    Content = "qual a vida util media da maquina ?"
                }
            ],
            Tools =
            [
                new AgentToolDefinition
                {
                    Name = "describe_dataset",
                    Description = "Describe the dataset."
                }
            ]
        });

        Assert.Equal("tool_calls", response.FinishReason);
        var toolCall = Assert.Single(response.ToolCalls);
        Assert.Equal("414934148", toolCall.Id);
        Assert.Equal("describe_dataset", toolCall.Name);
        Assert.Equal("{}", toolCall.ArgumentsJson);
    }

    [Fact]
    public async Task Orchestrator_ContinuesTruncatedFinalAnswer_Automatically()
    {
        var chatClient = new StubAgentChatClient(
            new AgentModelResponse
            {
                Model = "qwen-test",
                Content = "Em resumo, quanto",
                FinishReason = "length"
            },
            new AgentModelResponse
            {
                Model = "qwen-test",
                Content = " mais tempo a máquina permanece saudável, menor tende a ser o risco imediato de falha.",
                FinishReason = "stop"
            });

        var toolRuntime = new StubAgentToolRuntime();
        var orchestrator = new LmStudioAgentOrchestrator(
            new AgentOptions
            {
                Model = "qwen-test",
                MaxToolIterations = 3,
                MaxContinuationRounds = 6,
                EnableWorkerPasses = false,
                EnableContextCompaction = false
            },
            chatClient,
            toolRuntime,
            NullLoggerFactory.Instance);

        var result = await orchestrator.RunAsync(new AgentExecutionContext
        {
            UserInput = "Explique em português."
        });

        Assert.False(result.ContinuationExhausted);
        Assert.Contains("Em resumo, quanto mais tempo", result.Message, StringComparison.Ordinal);
        Assert.Contains("risco", result.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Orchestrator_MergesOverlappingContinuation_WithoutObviousDuplication()
    {
        var chatClient = new StubAgentChatClient(
            new AgentModelResponse
            {
                Model = "qwen-test",
                Content = "abcdef",
                FinishReason = "length"
            },
            new AgentModelResponse
            {
                Model = "qwen-test",
                Content = "cdefgh",
                FinishReason = "stop"
            });

        var toolRuntime = new StubAgentToolRuntime();
        var orchestrator = new LmStudioAgentOrchestrator(
            new AgentOptions
            {
                Model = "qwen-test",
                MaxToolIterations = 2,
                EnableWorkerPasses = false,
                EnableContextCompaction = false
            },
            chatClient,
            toolRuntime,
            NullLoggerFactory.Instance);

        var result = await orchestrator.RunAsync(new AgentExecutionContext { UserInput = "overlap" });

        Assert.Equal("abcdefgh", result.Message);
    }

    [Fact]
    public async Task Orchestrator_SetsContinuationExhausted_WhenContinuationBudgetEnds()
    {
        var chatClient = new StubAgentChatClient(
            new AgentModelResponse { Model = "qwen-test", Content = "a", FinishReason = "length" },
            new AgentModelResponse { Model = "qwen-test", Content = "b", FinishReason = "length" },
            new AgentModelResponse { Model = "qwen-test", Content = "c", FinishReason = "length" },
            new AgentModelResponse { Model = "qwen-test", Content = "final", FinishReason = "stop" });

        var toolRuntime = new StubAgentToolRuntime();
        var orchestrator = new LmStudioAgentOrchestrator(
            new AgentOptions
            {
                Model = "qwen-test",
                MaxToolIterations = 2,
                MaxContinuationRounds = 2,
                EnableWorkerPasses = false,
                EnableContextCompaction = false
            },
            chatClient,
            toolRuntime,
            NullLoggerFactory.Instance);

        var result = await orchestrator.RunAsync(new AgentExecutionContext { UserInput = "long" });

        Assert.True(result.ContinuationExhausted);
        Assert.Contains("final", result.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Orchestrator_ReconstructsContext_FromMemory_WhenHistoryIsCompacted()
    {
        var history = new List<AgentConversationMessage>();
        for (var index = 0; index < 30; index++)
        {
            history.Add(new AgentConversationMessage
            {
                Role = index % 2 == 0 ? AgentConversationRole.User : AgentConversationRole.Assistant,
                Content = $"turn-{index}"
            });
        }

        var memory = new AgentConversationMemory
        {
            RollingSummary = "FACT_LOCKED_XYZ",
            LanguagePreference = "pt"
        };

        var chatClient = new StubAgentChatClient(new AgentModelResponse
        {
            Model = "qwen-test",
            Content = "Resposta pós-compactação.",
            FinishReason = "stop"
        });

        var toolRuntime = new StubAgentToolRuntime();
        var orchestrator = new LmStudioAgentOrchestrator(
            new AgentOptions
            {
                Model = "qwen-test",
                MaxToolIterations = 2,
                MaxConversationMessages = 8,
                CompactionKeepRecentMessages = 6,
                EnableContextCompaction = true,
                EnableWorkerPasses = false
            },
            chatClient,
            toolRuntime,
            NullLoggerFactory.Instance);

        var result = await orchestrator.RunAsync(new AgentExecutionContext
        {
            UserInput = "pergunta final",
            ConversationHistory = history,
            ConversationMemory = memory
        });

        Assert.True(result.UpdatedConversation.Count < history.Count + 1);
        Assert.Contains("FACT_LOCKED_XYZ", result.UpdatedConversationMemory?.RollingSummary ?? string.Empty, StringComparison.Ordinal);
        Assert.Equal("Resposta pós-compactação.", result.Message);
    }

    [Fact]
    public async Task Orchestrator_WorkerMemoryRefresh_StillCompletesToolFlow()
    {
        const string memoryWorkerJson = """
        {
          "currentUserIntent": "Summarize the dataset.",
          "pendingQuestions": [],
          "confirmedFacts": ["Dataset described via tool output."],
          "toolHighlights": [{"tool": "describe_dataset", "summary": "10k rows"}],
          "language": "en",
          "rollingSummary": "Dataset summary requested."
        }
        """;

        var chatClient = new StubAgentChatClient(
            new AgentModelResponse
            {
                Model = "qwen-test",
                ToolCalls =
                [
                    new AgentToolCall
                    {
                        Id = "tool_1",
                        Name = "describe_dataset",
                        ArgumentsJson = "{}"
                    }
                ]
            },
            new AgentModelResponse
            {
                Model = "qwen-test",
                Content = memoryWorkerJson,
                FinishReason = "stop"
            },
            new AgentModelResponse
            {
                Model = "qwen-test",
                Content = "The dataset has 10,000 rows and 14 columns.",
                FinishReason = "stop"
            },
            new AgentModelResponse
            {
                Model = "qwen-test",
                Content = """
                {
                  "currentUserIntent": "Summarize the dataset.",
                  "pendingQuestions": [],
                  "confirmedFacts": ["Final answer produced."],
                  "toolHighlights": [],
                  "language": "en",
                  "rollingSummary": "Finalized assistant turn."
                }
                """,
                FinishReason = "stop"
            });

        var toolRuntime = new StubAgentToolRuntime();
        var orchestrator = new LmStudioAgentOrchestrator(
            new AgentOptions
            {
                Model = "qwen-test",
                MaxToolIterations = 3,
                EnableWorkerPasses = true,
                EnableContextCompaction = false
            },
            chatClient,
            toolRuntime,
            NullLoggerFactory.Instance);

        var result = await orchestrator.RunAsync(new AgentExecutionContext
        {
            UserInput = "Summarize the dataset."
        });

        Assert.Equal("The dataset has 10,000 rows and 14 columns.", result.Message);
        Assert.Contains("Dataset summary requested.", result.UpdatedConversationMemory?.RollingSummary ?? string.Empty, StringComparison.Ordinal);
        Assert.Contains("Finalized assistant turn.", result.UpdatedConversationMemory?.RollingSummary ?? string.Empty, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Orchestrator_RemainsCoherentAcrossLongChat_WithCompaction()
    {
        var responses = Enumerable.Range(0, 24)
            .Select(index => new AgentModelResponse
            {
                Model = "qwen-test",
                Content = $"resposta-{index}",
                FinishReason = "stop"
            })
            .ToArray();

        var chatClient = new StubAgentChatClient(responses);

        var toolRuntime = new StubAgentToolRuntime();
        var orchestrator = new LmStudioAgentOrchestrator(
            new AgentOptions
            {
                Model = "qwen-test",
                MaxToolIterations = 2,
                MaxConversationMessages = 6,
                CompactionKeepRecentMessages = 4,
                EnableContextCompaction = true,
                EnableWorkerPasses = false
            },
            chatClient,
            toolRuntime,
            NullLoggerFactory.Instance);

        var memory = (AgentConversationMemory?)null;
        var conversation = new List<AgentConversationMessage>();

        for (var turn = 0; turn < 12; turn++)
        {
            var result = await orchestrator.RunAsync(new AgentExecutionContext
            {
                UserInput = $"turn {turn}",
                ConversationHistory = conversation,
                ConversationMemory = memory
            });

            conversation = result.UpdatedConversation.ToList();
            memory = result.UpdatedConversationMemory;
        }

        Assert.NotNull(memory);
        Assert.Contains("turn", memory!.RollingSummary, StringComparison.OrdinalIgnoreCase);
        Assert.True(conversation.Count <= 20, "O histórico bruto deve permanecer limitado após compactações repetidas.");
        Assert.Equal("resposta-11", conversation[^1].Content);
    }

    [Fact]
    public async Task Orchestrator_ScopesTools_DownFromFullCatalog()
    {
        const string plannerJson = """
        {"need_tools":true,"tools":["describe_dataset"],"reason":"minimal"}
        """;

        var recording = new RecordingStubAgentChatClient(
            new AgentModelResponse { Model = "m", Content = plannerJson, FinishReason = "stop" },
            new AgentModelResponse { Model = "m", Content = "done", FinishReason = "stop" });

        var orchestrator = new LmStudioAgentOrchestrator(
            new AgentOptions
            {
                Model = "m",
                MaxToolIterations = 2,
                EnableDynamicToolScoping = true,
                EnableToolPlannerPass = true,
                EnableWorkerPasses = false,
                EnableContextCompaction = false,
                EnableTokenBudgetCompaction = false
            },
            recording,
            new StubMultiToolRuntime(),
            NullLoggerFactory.Instance);

        var result = await orchestrator.RunAsync(new AgentExecutionContext
        {
            UserInput = "What does this dataset contain?"
        });

        Assert.Equal("done", result.Message);
        Assert.True(recording.Requests.Count >= 2);
        Assert.Empty(recording.Requests[0].Tools);
        Assert.False(recording.Requests[0].EnableTools);
        Assert.True(recording.Requests[1].EnableTools);
        Assert.Single(recording.Requests[1].Tools);
        Assert.Equal("describe_dataset", recording.Requests[1].Tools[0].Name);
        Assert.True(recording.Requests[1].UseMinimalToolSchemas);
    }

    [Fact]
    public async Task Orchestrator_Compacts_WhenPromptBudgetExceedsSoftSlot()
    {
        var bigBlock = new string('x', 3200);
        var history = new List<AgentConversationMessage>();
        for (var index = 0; index < 10; index++)
        {
            history.Add(new AgentConversationMessage
            {
                Role = index % 2 == 0 ? AgentConversationRole.User : AgentConversationRole.Assistant,
                Content = $"{bigBlock}-{index}"
            });
        }

        var chatClient = new StubAgentChatClient(new AgentModelResponse
        {
            Model = "m",
            Content = "ok",
            FinishReason = "stop"
        });

        var orchestrator = new LmStudioAgentOrchestrator(
            new AgentOptions
            {
                Model = "m",
                ContextSlotTokens = 2600,
                ContextSafetyMarginTokens = 200,
                ReasoningReserveTokens = 200,
                ContextBudgetCharsPerToken = 4,
                EnableTokenBudgetCompaction = true,
                EnableContextCompaction = true,
                CompactionKeepRecentMessages = 4,
                MaxConversationMessages = 200,
                EnableWorkerPasses = false,
                EnableDynamicToolScoping = false,
                EnableToolPlannerPass = false
            },
            chatClient,
            new StubAgentToolRuntime(),
            NullLoggerFactory.Instance);

        var result = await orchestrator.RunAsync(new AgentExecutionContext
        {
            UserInput = "final",
            ConversationHistory = history
        });

        Assert.True(result.UpdatedConversation.Count < history.Count + 1);
        Assert.Equal("ok", result.Message);
    }

    [Fact]
    public async Task Orchestrator_RecoversFromReasoningOnlyResponse_WithEmptyContent()
    {
        var chatClient = new StubAgentChatClient(
            new AgentModelResponse
            {
                Model = "m",
                Content = "",
                ReasoningContent = "internal reasoning should never be shown",
                FinishReason = "length"
            },
            new AgentModelResponse
            {
                Model = "m",
                Content = "Resposta visível para o utilizador.",
                FinishReason = "stop"
            });

        var orchestrator = new LmStudioAgentOrchestrator(
            new AgentOptions
            {
                Model = "m",
                MaxToolIterations = 2,
                EnableWorkerPasses = false,
                EnableContextCompaction = false,
                EnableTokenBudgetCompaction = false
            },
            chatClient,
            new StubAgentToolRuntime(),
            NullLoggerFactory.Instance);

        var result = await orchestrator.RunAsync(new AgentExecutionContext
        {
            UserInput = "Explica."
        });

        Assert.Equal("Resposta visível para o utilizador.", result.Message);
        Assert.DoesNotContain("internal reasoning", result.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Orchestrator_PersistsSingleConsolidatedAssistantTurn_AfterTruncationContinuation()
    {
        var chatClient = new StubAgentChatClient(
            new AgentModelResponse { Model = "m", Content = "PART-A", FinishReason = "length" },
            new AgentModelResponse { Model = "m", Content = "PART-B", FinishReason = "stop" });

        var orchestrator = new LmStudioAgentOrchestrator(
            new AgentOptions
            {
                Model = "m",
                MaxToolIterations = 2,
                MaxContinuationRounds = 6,
                EnableWorkerPasses = false,
                EnableContextCompaction = false,
                EnableTokenBudgetCompaction = false
            },
            chatClient,
            new StubAgentToolRuntime(),
            NullLoggerFactory.Instance);

        var result = await orchestrator.RunAsync(new AgentExecutionContext { UserInput = "merge" });

        var assistantMessages = result.UpdatedConversation.Where(message => message.Role == AgentConversationRole.Assistant).ToArray();
        Assert.Single(assistantMessages);
        Assert.Contains("PART-A", assistantMessages[0].Content, StringComparison.Ordinal);
        Assert.Contains("PART-B", assistantMessages[0].Content, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Orchestrator_RemainsCoherent_AfterCompactionWithWorkerSummary()
    {
        var history = new List<AgentConversationMessage>();
        for (var index = 0; index < 15; index++)
        {
            history.Add(new AgentConversationMessage
            {
                Role = index % 2 == 0 ? AgentConversationRole.User : AgentConversationRole.Assistant,
                Content = $"msg-{index}"
            });
        }

        var chatClient = new StubAgentChatClient(
            new AgentModelResponse { Model = "m", Content = "rolled_up_summary_from_worker", FinishReason = "stop" },
            new AgentModelResponse { Model = "m", Content = "final answer", FinishReason = "stop" },
            new AgentModelResponse
            {
                Model = "m",
                Content = """
                {
                  "currentUserIntent": "next question",
                  "pendingQuestions": [],
                  "confirmedFacts": [],
                  "toolHighlights": [],
                  "language": "en",
                  "rollingSummary": "post_final_memory_pass"
                }
                """,
                FinishReason = "stop"
            });

        var orchestrator = new LmStudioAgentOrchestrator(
            new AgentOptions
            {
                Model = "m",
                MaxToolIterations = 2,
                MaxConversationMessages = 8,
                CompactionKeepRecentMessages = 6,
                EnableContextCompaction = true,
                EnableWorkerPasses = true,
                EnableDynamicToolScoping = false,
                EnableToolPlannerPass = false,
                EnableTokenBudgetCompaction = false
            },
            chatClient,
            new StubAgentToolRuntime(),
            NullLoggerFactory.Instance);

        var result = await orchestrator.RunAsync(new AgentExecutionContext
        {
            UserInput = "next question",
            ConversationHistory = history
        });

        var rolling = result.UpdatedConversationMemory?.RollingSummary ?? string.Empty;
        Assert.Contains("rolled_up_summary_from_worker", rolling, StringComparison.Ordinal);
        Assert.Contains("post_final_memory_pass", rolling, StringComparison.Ordinal);
        Assert.Equal("final answer", result.Message);
    }

    [Fact]
    public async Task LmStudioChatClient_ParsesUsageAndReasoningContent()
    {
        const string responseJson = """
        {
          "model": "google/gemma-4-e4b",
          "usage": {
            "prompt_tokens": 3920,
            "completion_tokens": 176,
            "total_tokens": 4096,
            "completion_tokens_details": { "reasoning_tokens": 120 }
          },
          "choices": [
            {
              "message": {
                "role": "assistant",
                "content": "",
                "reasoning_content": "thinking..."
              },
              "finish_reason": "length"
            }
          ]
        }
        """;

        using var httpClient = new HttpClient(new StubHttpMessageHandler(responseJson))
        {
            BaseAddress = new Uri("http://127.0.0.1:1234/v1/")
        };

        using var chatClient = new LmStudioChatClient(
            new AgentOptions { BaseUrl = "http://127.0.0.1:1234/v1", Model = "google/gemma-4-e4b" },
            httpClient);

        var response = await chatClient.CompleteAsync(new AgentModelRequest
        {
            Model = "google/gemma-4-e4b",
            SystemPrompt = "sys",
            Messages = [new AgentConversationMessage { Role = AgentConversationRole.User, Content = "hi" }]
        });

        Assert.Equal("length", response.FinishReason);
        Assert.Equal("thinking...", response.ReasoningContent);
        Assert.NotNull(response.Usage);
        Assert.Equal(3920, response.Usage!.PromptTokens);
        Assert.Equal(176, response.Usage.CompletionTokens);
        Assert.Equal(120, response.Usage.ReasoningTokens);
    }

    [Fact]
    public async Task LmStudioChatClient_PromotesPseudoToolCall_FromContent_AndCanonicalizesQuerySchema()
    {
        const string responseJson = """
        {
          "id": "chatcmpl-pseudo",
          "object": "chat.completion",
          "created": 1776549825,
          "model": "m",
          "choices": [
            {
              "index": 0,
              "message": {
                "role": "assistant",
                "content": "<|tool_call|>call: query_schema{}<|tool_call|>",
                "tool_calls": []
              },
              "finish_reason": "stop"
            }
          ]
        }
        """;

        using var httpClient = new HttpClient(new StubHttpMessageHandler(responseJson))
        {
            BaseAddress = new Uri("http://127.0.0.1:1234/v1/")
        };

        using var chatClient = new LmStudioChatClient(
            new AgentOptions { BaseUrl = "http://127.0.0.1:1234/v1", Model = "m" },
            httpClient);

        var response = await chatClient.CompleteAsync(new AgentModelRequest
        {
            Model = "m",
            SystemPrompt = "sys",
            Messages =
            [
                new AgentConversationMessage
                {
                    Role = AgentConversationRole.User,
                    Content = "qual a temperatura maxima?"
                }
            ],
            Tools =
            [
                new AgentToolDefinition { Name = "get_schema", Description = "Schema." }
            ],
            EnableTools = true
        });

        var call = Assert.Single(response.ToolCalls);
        Assert.Equal("get_schema", call.Name);
        Assert.StartsWith("pseudo_", call.Id, StringComparison.Ordinal);
        Assert.Null(response.Content);
    }

    [Fact]
    public async Task LmStudioChatClient_NormalizesStructuredToolCallNames_UsingAliases()
    {
        const string responseJson = """
        {
          "id": "chatcmpl-alias",
          "object": "chat.completion",
          "created": 1776549825,
          "model": "m",
          "choices": [
            {
              "index": 0,
              "message": {
                "role": "assistant",
                "content": "",
                "tool_calls": [
                  {
                    "id": "99",
                    "type": "function",
                    "function": {
                      "name": "query_schema",
                      "arguments": "{}"
                    }
                  }
                ]
              },
              "finish_reason": "tool_calls"
            }
          ]
        }
        """;

        using var httpClient = new HttpClient(new StubHttpMessageHandler(responseJson))
        {
            BaseAddress = new Uri("http://127.0.0.1:1234/v1/")
        };

        using var chatClient = new LmStudioChatClient(
            new AgentOptions { BaseUrl = "http://127.0.0.1:1234/v1", Model = "m" },
            httpClient);

        var response = await chatClient.CompleteAsync(new AgentModelRequest
        {
            Model = "m",
            SystemPrompt = "sys",
            Messages = [new AgentConversationMessage { Role = AgentConversationRole.User, Content = "schema" }],
            Tools = [new AgentToolDefinition { Name = "get_schema", Description = "Schema." }],
            EnableTools = true
        });

        var call = Assert.Single(response.ToolCalls);
        Assert.Equal("99", call.Id);
        Assert.Equal("get_schema", call.Name);
    }

    [Fact]
    public void VisibleNormalizer_PseudoOnlyAssistantSurface_IsNotUserVisible()
    {
        Assert.False(AgentVisibleResponseNormalizer.IsUserVisibleAssistantText(
            "<|tool_call|>call: get_schema{}<|tool_call|>"));
        Assert.Null(AgentVisibleResponseNormalizer.StripInternalAssistantSurface(
            "<|tool_call|>call: get_schema{}<|tool_call|>"));
    }

    [Fact]
    public void VisibleNormalizer_MalformedAsymmetricPseudoMarkers_YieldsNoVisibleSurface()
    {
        const string malformed =
            "<|tool_call>call:analyze_column{columnName:<|\"|>Air temperature [K]<|\"|>,operation:<|\"|>max<|\"|>}<tool_call|>";
        Assert.Null(AgentVisibleResponseNormalizer.StripInternalAssistantSurface(malformed));
        Assert.False(AgentVisibleResponseNormalizer.IsUserVisibleAssistantText(malformed));
    }

    [Fact]
    public async Task Orchestrator_PlannerTruncated_ReexposesMinimalToolkit_AfterDescribe_ForPortugueseNumericQuestion()
    {
        var recording = new RecordingStubAgentChatClient(
            new AgentModelResponse { Model = "m", Content = "", ReasoningContent = "{", FinishReason = "length" },
            new AgentModelResponse
            {
                Model = "m",
                ToolCalls =
                [
                    new AgentToolCall { Id = "t1", Name = "describe_dataset", ArgumentsJson = "{}" }
                ],
                FinishReason = "tool_calls"
            },
            new AgentModelResponse { Model = "m", Content = "", ReasoningContent = "...", FinishReason = "length" },
            new AgentModelResponse { Model = "m", Content = "Resposta final.", FinishReason = "stop" });

        var orchestrator = new LmStudioAgentOrchestrator(
            new AgentOptions
            {
                Model = "m",
                MaxToolIterations = 4,
                EnableDynamicToolScoping = true,
                EnableToolPlannerPass = true,
                EnableWorkerPasses = false,
                EnableContextCompaction = false,
                EnableTokenBudgetCompaction = false
            },
            recording,
            new StubMultiToolRuntime(),
            NullLoggerFactory.Instance);

        await orchestrator.RunAsync(new AgentExecutionContext
        {
            UserInput = "qual o momento de maior temperatura ? e qual o valor dela em celsius e k ?"
        });

        Assert.True(recording.Requests.Count >= 4);
        var secondAgentRequest = recording.Requests[3];
        Assert.Contains(secondAgentRequest.Tools,
            tool => tool.Name.Equals("group_and_aggregate", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public async Task Orchestrator_FinalAnswer_ContainsNoPseudoToolMarkers_WhenModelEmitsMalformedPseudo()
    {
        var chatClient = new StubAgentChatClient(
            new AgentModelResponse
            {
                Model = "m",
                Content = "<|tool_call>call:analyze_column{}<tool_call|>",
                FinishReason = "stop"
            },
            new AgentModelResponse
            {
                Model = "m",
                Content = "Resposta segura após recuperação.",
                FinishReason = "stop"
            });

        var orchestrator = new LmStudioAgentOrchestrator(
            new AgentOptions
            {
                Model = "m",
                MaxToolIterations = 2,
                EnableWorkerPasses = false,
                EnableContextCompaction = false,
                EnableTokenBudgetCompaction = false,
                EnableDynamicToolScoping = false,
                EnableToolPlannerPass = false
            },
            chatClient,
            new StubMultiToolRuntime(),
            NullLoggerFactory.Instance);

        var result = await orchestrator.RunAsync(new AgentExecutionContext { UserInput = "teste" });

        Assert.Contains("Resposta segura", result.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("<|tool_call", result.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("<tool_call|>", result.Message, StringComparison.Ordinal);
        foreach (var message in result.UpdatedConversation.Where(message => message.Role == AgentConversationRole.Assistant))
        {
            Assert.DoesNotContain("<|tool_call", message.Content ?? string.Empty, StringComparison.Ordinal);
        }
    }

    [Fact]
    public async Task Orchestrator_DoesNotExecuteUnknownToolNames_FromModelToolCalls()
    {
        var chatClient = new StubAgentChatClient(new AgentModelResponse
        {
            Model = "m",
            Content = "Resposta direta.",
            ToolCalls =
            [
                new AgentToolCall { Id = "1", Name = "analyze_column", ArgumentsJson = "{}" }
            ],
            FinishReason = "tool_calls"
        });

        var orchestrator = new LmStudioAgentOrchestrator(
            new AgentOptions
            {
                Model = "m",
                MaxToolIterations = 2,
                EnableWorkerPasses = false,
                EnableContextCompaction = false,
                EnableTokenBudgetCompaction = false,
                EnableDynamicToolScoping = false,
                EnableToolPlannerPass = false
            },
            chatClient,
            new StubMultiToolRuntime(),
            NullLoggerFactory.Instance);

        var result = await orchestrator.RunAsync(new AgentExecutionContext { UserInput = "pergunta" });

        Assert.Empty(result.ToolExecutions);
        Assert.Equal("Resposta direta.", result.Message);
    }

    [Fact]
    public async Task Orchestrator_FallbackExposesMinimalToolkit_WhenPlannerSaysNoTools_ForPortugueseNumericQuestion()
    {
        const string plannerNoTools = """
        {"need_tools":false,"tools":[],"reason":"wrong"}
        """;

        var recording = new RecordingStubAgentChatClient(
            new AgentModelResponse { Model = "m", Content = plannerNoTools, FinishReason = "stop" },
            new AgentModelResponse { Model = "m", Content = "ok", FinishReason = "stop" });

        var orchestrator = new LmStudioAgentOrchestrator(
            new AgentOptions
            {
                Model = "m",
                MaxToolIterations = 2,
                EnableDynamicToolScoping = true,
                EnableToolPlannerPass = true,
                EnableWorkerPasses = false,
                EnableContextCompaction = false,
                EnableTokenBudgetCompaction = false
            },
            recording,
            new StubMultiToolRuntime(),
            NullLoggerFactory.Instance);

        await orchestrator.RunAsync(new AgentExecutionContext
        {
            UserInput = "ei, qual a temperatura maxima ?"
        });

        Assert.True(recording.Requests.Count >= 2);
        var scoped = recording.Requests[1].Tools;
        Assert.Contains(scoped, tool => tool.Name.Equals("get_schema", StringComparison.OrdinalIgnoreCase));
        Assert.Contains(scoped, tool => tool.Name.Equals("group_and_aggregate", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public async Task Orchestrator_DoesNotPersistPseudoToolCallMarkers_AfterLmStudioCompatibilityPass()
    {
        const string pseudoResponse = """
        {
          "model": "m",
          "choices": [
            {
              "message": {
                "role": "assistant",
                "content": "<|tool_call|>call: query_schema{}<|tool_call|>",
                "tool_calls": []
              },
              "finish_reason": "stop"
            }
          ]
        }
        """;

        const string finalResponse = """
        {
          "model": "m",
          "choices": [
            {
              "message": {
                "role": "assistant",
                "content": "Temperatura máxima: 318 (valor ilustrativo).",
                "tool_calls": []
              },
              "finish_reason": "stop"
            }
          ]
        }
        """;

        using var httpClient = new HttpClient(new QueuedStubHttpMessageHandler(pseudoResponse, finalResponse))
        {
            BaseAddress = new Uri("http://127.0.0.1:1234/v1/")
        };

        using var chatClient = new LmStudioChatClient(
            new AgentOptions { BaseUrl = "http://127.0.0.1:1234/v1", Model = "m" },
            httpClient);

        var orchestrator = new LmStudioAgentOrchestrator(
            new AgentOptions
            {
                Model = "m",
                MaxToolIterations = 3,
                EnableWorkerPasses = false,
                EnableContextCompaction = false,
                EnableTokenBudgetCompaction = false,
                EnableDynamicToolScoping = false,
                EnableToolPlannerPass = false
            },
            chatClient,
            new StubMultiToolRuntime(),
            NullLoggerFactory.Instance);

        var result = await orchestrator.RunAsync(new AgentExecutionContext
        {
            UserInput = "qual a temperatura máxima?"
        });

        Assert.Contains("318", result.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("<|tool_call|>", result.Message, StringComparison.Ordinal);

        foreach (var message in result.UpdatedConversation.Where(message => message.Role == AgentConversationRole.Assistant))
        {
            Assert.DoesNotContain("<|tool_call|>", message.Content ?? string.Empty, StringComparison.Ordinal);
        }
    }

    [Fact]
    public async Task ToolRuntime_MapsQuerySchemaAlias_ToGetSchemaExecution()
    {
        var service = new StubDatasetToolService();
        var runtime = new DatasetAgentToolRuntime(new StubDatasetToolCatalog(), service);
        var result = await runtime.ExecuteAsync("query_schema", "{}");
        Assert.False(result.IsError);
    }

    [Fact]
    public void OpenAiToolParametersNormalizer_AddsProperties_ToNestedFilter_InDatasetQueryRowsSchema()
    {
        var runtime = new DatasetAgentToolRuntime(new StubDatasetToolCatalog(), new StubDatasetToolService());
        var schemaJson = runtime.GetTools().Single().ParametersJsonSchema;
        var node = JsonNode.Parse(schemaJson)!;
        OpenAiCompatibleToolParametersNormalizer.PrepareToolParametersSchema(node);
        using var doc = JsonDocument.Parse(node.ToJsonString());
        var filter = doc.RootElement.GetProperty("properties").GetProperty("filter");
        Assert.True(filter.TryGetProperty("properties", out var filterProps));
        Assert.Equal(JsonValueKind.Object, filterProps.ValueKind);
    }

    private sealed class RecordingStubAgentChatClient : IAgentChatClient
    {
        private readonly Queue<AgentModelResponse> _responses;
        public List<AgentModelRequest> Requests { get; } = [];

        public RecordingStubAgentChatClient(params AgentModelResponse[] responses)
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

    private sealed class StubMultiToolRuntime : IAgentToolRuntime
    {
        public IReadOnlyList<AgentToolDefinition> GetTools()
        {
            var schema = """{"type":"object","properties":{"q":{"type":"string"}}}""";
            return
            [
                new AgentToolDefinition { Name = "describe_dataset", Description = "Describe dataset.", ParametersJsonSchema = schema },
                new AgentToolDefinition { Name = "get_schema", Description = "Schema.", ParametersJsonSchema = schema },
                new AgentToolDefinition { Name = "query_rows", Description = "Query rows.", ParametersJsonSchema = schema },
                new AgentToolDefinition { Name = "search_columns", Description = "Search columns.", ParametersJsonSchema = schema },
                new AgentToolDefinition { Name = "group_and_aggregate", Description = "Group.", ParametersJsonSchema = schema },
                new AgentToolDefinition { Name = "profile_columns", Description = "Profile.", ParametersJsonSchema = schema }
            ];
        }

        public Task<AgentToolExecutionRecord> ExecuteAsync(string toolName, string argumentsJson, CancellationToken cancellationToken = default)
            => Task.FromResult(new AgentToolExecutionRecord
            {
                ToolName = toolName,
                ArgumentsJson = argumentsJson,
                ResultJson = """{"ok":true}"""
            });
    }

    private sealed class StubAgentChatClient : IAgentChatClient
    {
        private readonly Queue<AgentModelResponse> _responses;

        public StubAgentChatClient(params AgentModelResponse[] responses)
        {
            _responses = new Queue<AgentModelResponse>(responses);
        }

        public Task<AgentModelResponse> CompleteAsync(AgentModelRequest request, CancellationToken cancellationToken = default)
            => Task.FromResult(_responses.Dequeue());

        public Task<IReadOnlyList<string>> GetAvailableModelsAsync(CancellationToken cancellationToken = default)
            => Task.FromResult<IReadOnlyList<string>>(["qwen-test"]);
    }

    private sealed class StubAgentToolRuntime : IAgentToolRuntime
    {
        public IReadOnlyList<AgentToolDefinition> GetTools()
            => [new AgentToolDefinition
            {
                Name = "describe_dataset",
                Description = "Describe the dataset."
            }];

        public Task<AgentToolExecutionRecord> ExecuteAsync(string toolName, string argumentsJson, CancellationToken cancellationToken = default)
            => Task.FromResult(new AgentToolExecutionRecord
            {
                ToolName = toolName,
                ArgumentsJson = argumentsJson,
                ResultJson = """{"datasetName":"AI4I 2020 Predictive Maintenance Dataset","rowCount":10000,"columnCount":14}"""
            });
    }

    private sealed class StubDatasetToolCatalog : IDatasetToolCatalog
    {
        public IReadOnlyList<DatasetToolDescriptor> GetTools()
            => [new DatasetToolDescriptor
            {
                Name = "query_rows",
                Description = "Run a dataset row query."
            }];
    }

    private sealed class StubDatasetToolService : IDatasetToolService
    {
        public QueryRequest? LastQueryRequest { get; private set; }

        public Task<DatasetSchema> GetSchemaAsync(CancellationToken cancellationToken = default)
            => Task.FromResult(new DatasetSchema());

        public Task<DatasetDescription> DescribeDatasetAsync(CancellationToken cancellationToken = default)
            => Task.FromResult(new DatasetDescription());

        public Task<ColumnProfile> GetColumnProfileAsync(string columnName, CancellationToken cancellationToken = default)
            => Task.FromResult(new ColumnProfile());

        public Task<ColumnProfilingResult> ProfileColumnsAsync(ColumnProfilingRequest request, CancellationToken cancellationToken = default)
            => Task.FromResult(new ColumnProfilingResult());

        public Task<QueryResult> QueryRowsAsync(QueryRequest request, CancellationToken cancellationToken = default)
        {
            LastQueryRequest = request;
            return Task.FromResult(new QueryResult());
        }

        public Task<ColumnExtremaResult> FindColumnExtremaRowsAsync(ColumnExtremaRequest request, CancellationToken cancellationToken = default)
            => Task.FromResult(new ColumnExtremaResult());

        public Task<GroupAggregationResult> GroupAndAggregateAsync(GroupAggregationRequest request, CancellationToken cancellationToken = default)
            => Task.FromResult(new GroupAggregationResult());

        public Task<DistinctValuesResult> GetDistinctValuesAsync(DistinctValuesRequest request, CancellationToken cancellationToken = default)
            => Task.FromResult(new DistinctValuesResult());

        public Task<SubsetComparisonResult> CompareSubsetsAsync(SubsetComparisonRequest request, CancellationToken cancellationToken = default)
            => Task.FromResult(new SubsetComparisonResult());

        public Task<SearchColumnsResult> SearchColumnsAsync(string keyword, CancellationToken cancellationToken = default)
            => Task.FromResult(new SearchColumnsResult());

        public Task<DatasetReport> BuildReportAsync(ReportRequest request, CancellationToken cancellationToken = default)
            => Task.FromResult(new DatasetReport());

        public Task<FailureAnalysisSummary> GetFailureAnalysisAsync(CancellationToken cancellationToken = default)
            => Task.FromResult(new FailureAnalysisSummary());

        public Task<SubsetComparisonResult> CompareFailureCohortsAsync(CancellationToken cancellationToken = default)
            => Task.FromResult(new SubsetComparisonResult());

        public Task<IReadOnlyList<ValueFrequency>> GetFailureModesAsync(CancellationToken cancellationToken = default)
            => Task.FromResult<IReadOnlyList<ValueFrequency>>(Array.Empty<ValueFrequency>());

        public Task<OperatingConditionSummary> GetOperatingConditionSummaryAsync(FilterExpression? filter = null, CancellationToken cancellationToken = default)
            => Task.FromResult(new OperatingConditionSummary());

        public Task<DatasetReport> BuildExecutiveReportAsync(CancellationToken cancellationToken = default)
            => Task.FromResult(new DatasetReport());

        public Task<IReadOnlyList<AnalysisExample>> GetAnalysisExamplesAsync(CancellationToken cancellationToken = default)
            => Task.FromResult<IReadOnlyList<AnalysisExample>>(Array.Empty<AnalysisExample>());
    }

    private sealed class StubHttpMessageHandler : HttpMessageHandler
    {
        private readonly string _responseJson;

        public StubHttpMessageHandler(string responseJson)
        {
            _responseJson = responseJson;
        }

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
            => Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(_responseJson, Encoding.UTF8, "application/json")
            });
    }

    private sealed class QueuedStubHttpMessageHandler : HttpMessageHandler
    {
        private readonly Queue<string> _responses;

        public QueuedStubHttpMessageHandler(params string[] responses)
        {
            _responses = new Queue<string>(responses);
        }

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            if (_responses.Count == 0)
            {
                throw new InvalidOperationException("The stub HTTP queue is empty.");
            }

            var payload = _responses.Dequeue();
            return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(payload, Encoding.UTF8, "application/json")
            });
        }
    }
}
