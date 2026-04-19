using MachineHealthExplorer.Agent.Models;
using MachineHealthExplorer.Agent.Services;

namespace MachineHealthExplorer.Tests;

public sealed class AgentContextBudgetAndMemoryTests
{
    [Fact]
    public void WithToolEvidence_DedupesByToolAndDigestPrefix_KeepsCap()
    {
        var memory = new AgentConversationMemory();
        var digest = new string('x', 200);
        memory = memory.WithToolEvidence(
            [
                new AgentToolEvidenceDigest { ToolName = "describe_dataset", Digest = digest },
                new AgentToolEvidenceDigest { ToolName = "describe_dataset", Digest = digest },
                new AgentToolEvidenceDigest { ToolName = "query_rows", Digest = "other" }
            ],
            digestMaxChars: 80,
            maxStoredDigests: 2);

        Assert.Equal(2, memory.ToolEvidenceDigests.Count);
        Assert.Equal("query_rows", memory.ToolEvidenceDigests[^1].ToolName);
    }

    [Fact]
    public void LooksLikeContextLengthExceeded_DetectsLmStudioMessage()
    {
        const string body = """{"error":"The number of tokens to keep from the initial prompt is greater than the context length (n_keep: 5453>= n_ctx: 4096)."}""";
        Assert.True(AgentModelBackendException.LooksLikeContextLengthExceeded(body));
    }

    [Fact]
    public void BuildBoundedMemoryWorkerUserContent_ReducesSizeForTightBudget()
    {
        var options = new AgentOptions
        {
            ContextSlotTokens = 900,
            HostContextTokens = 0,
            WorkerMaxOutputTokens = 400,
            WorkerPromptReserveTokens = 120,
            ContextBudgetCharsPerToken = 3,
            MemoryEvidenceDigestMaxChars = 120,
            MemoryToolEvidenceMaxDigests = 4
        };

        var hugeTranscript = Enumerable.Range(0, 40)
            .Select(i => new AgentConversationMessage
            {
                Role = i % 2 == 0 ? AgentConversationRole.User : AgentConversationRole.Assistant,
                Content = new string('m', 1200)
            })
            .ToArray();

        var tools = new[]
        {
            new AgentToolExecutionRecord { ToolName = "describe_dataset", ResultJson = new string('j', 4000) }
        };

        var memory = new AgentConversationMemory { CurrentUserIntent = "intent", RollingSummary = new string('r', 8000) };
        var compact = AgentPromptBudgetGuard.BuildCompactMemoryProjectionJson(memory, options);
        var system = "system prompt for worker";
        var user = AgentPromptBudgetGuard.BuildBoundedMemoryWorkerUserContent(
            options,
            system,
            "short question",
            hugeTranscript,
            tools,
            compact,
            options.WorkerMaxOutputTokens);

        var est = AgentPromptBudgetGuard.EstimateWorkerPromptTokens(options, system, user);
        var budget = AgentPromptBudgetGuard.MaxWorkerPromptTokens(options, options.WorkerMaxOutputTokens);
        Assert.True(est <= budget, $"Estimated {est} should be <= budget {budget}.");
    }
}
