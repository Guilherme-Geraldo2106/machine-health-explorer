using MachineHealthExplorer.Agent.Abstractions;
using MachineHealthExplorer.Agent.Models;
using MachineHealthExplorer.Agent.Services;
using MachineHealthExplorer.Logging.Services;
using Microsoft.Extensions.Logging.Abstractions;

namespace MachineHealthExplorer.Tests;

public sealed class AgentAssistantAnswerPipelineContinuationTests
{
    [Fact]
    public async Task AnswerPipeline_LengthFinish_AttemptsContinuationWhenContextFits()
    {
        var options = new AgentOptions
        {
            Model = "m",
            HostContextTokens = 32_000,
            ContextSlotTokens = 32_000,
            ContextSafetyMarginTokens = 384,
            MaxContinuationRounds = 2,
            MinAssistantCompletionTokens = 256,
            ReasoningReserveTokens = 400,
            MaxOutputTokens = 4096,
            EnableContextCompaction = false,
            EnableTokenBudgetCompaction = false
        };

        var chat = new ContinuationQueueChatClient(new AgentModelResponse
        {
            Model = "m",
            Content = " Segunda parte.",
            FinishReason = "stop"
        });

        var workerRunner = new AgentEphemeralWorkerRunner(options, chat);
        var pipeline = new AgentAssistantAnswerPipeline(options, chat, workerRunner, NullLogger.Instance, NullChatSessionLogger.Instance);
        var memory = new AgentConversationMemory();
        var prefix = new List<AgentConversationMessage>();
        var first = new AgentModelResponse
        {
            Model = "m",
            Content = "Primeira parte.",
            FinishReason = "length"
        };

        var (message, exhausted) = await pipeline.CompleteAssistantAnswerAsync(
                "m",
                memory,
                prefix,
                first,
                "system",
                CancellationToken.None);

        Assert.False(exhausted);
        Assert.Contains("Primeira parte", message, StringComparison.Ordinal);
        Assert.Contains("Segunda parte", message, StringComparison.Ordinal);
        Assert.True(chat.Requests.Count >= 1);
    }

    private sealed class ContinuationQueueChatClient : IAgentChatClient
    {
        private readonly Queue<AgentModelResponse> _queue;

        public ContinuationQueueChatClient(params AgentModelResponse[] responses)
        {
            _queue = new Queue<AgentModelResponse>(responses);
        }

        public List<AgentModelRequest> Requests { get; } = [];

        public Task<AgentModelResponse> CompleteAsync(AgentModelRequest request, CancellationToken cancellationToken = default)
        {
            Requests.Add(request);
            if (_queue.Count == 0)
            {
                return Task.FromResult(new AgentModelResponse { Model = "m", Content = "tail", FinishReason = "stop" });
            }

            return Task.FromResult(_queue.Dequeue());
        }

        public Task<IReadOnlyList<string>> GetAvailableModelsAsync(CancellationToken cancellationToken = default)
            => Task.FromResult<IReadOnlyList<string>>(["m"]);
    }
}
