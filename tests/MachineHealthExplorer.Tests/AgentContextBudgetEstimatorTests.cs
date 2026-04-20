using MachineHealthExplorer.Agent.Models;
using MachineHealthExplorer.Agent.Services;

namespace MachineHealthExplorer.Tests;

public sealed class AgentContextBudgetEstimatorTests
{
    [Fact]
    public void ComputeEffectiveMaxOutputTokens_WhenRemainingBelowFloor_DoesNotInflateToMinAssistantFloor()
    {
        var options = new AgentOptions
        {
            ContextSlotTokens = 4096,
            HostContextTokens = 0,
            ContextSafetyMarginTokens = 384,
            ReasoningReserveTokens = 768,
            MaxOutputTokens = 4096,
            MinAssistantCompletionTokens = 448
        };

        var estimatedPrompt = 3600;
        var maxOut = AgentContextBudgetEstimator.ComputeEffectiveMaxOutputTokens(
            options,
            estimatedPrompt,
            reasoningPressureSteps: 0,
            lastUsage: null);

        Assert.True(maxOut < options.MinAssistantCompletionTokens);
        Assert.True(maxOut >= 1);
        Assert.True(maxOut <= options.MaxOutputTokens);
    }

    [Fact]
    public void ComputeEffectiveMaxOutputTokens_SmallMaxOutputTokens_ClampsFloorToMaxOutput()
    {
        var options = new AgentOptions
        {
            ContextSlotTokens = 4096,
            MaxOutputTokens = 256,
            MinAssistantCompletionTokens = 448,
            ContextSafetyMarginTokens = 384,
            ReasoningReserveTokens = 256
        };

        var maxOut = AgentContextBudgetEstimator.ComputeEffectiveMaxOutputTokens(
            options,
            estimatedPromptTokens: 500,
            reasoningPressureSteps: 0,
            lastUsage: null);

        Assert.Equal(256, maxOut);
    }

    [Fact]
    public void ComputeEffectiveMaxOutputTokens_RemainingAboveFloor_ReturnsRemainingCappedByMax()
    {
        var options = new AgentOptions
        {
            ContextSlotTokens = 8192,
            HostContextTokens = 0,
            ContextSafetyMarginTokens = 384,
            ReasoningReserveTokens = 256,
            MaxOutputTokens = 4096,
            MinAssistantCompletionTokens = 448
        };

        var maxOut = AgentContextBudgetEstimator.ComputeEffectiveMaxOutputTokens(
            options,
            estimatedPromptTokens: 800,
            reasoningPressureSteps: 0,
            lastUsage: null);

        Assert.True(maxOut >= 448);
        Assert.True(maxOut <= 4096);
    }

    [Fact]
    public void ComputeToolTurnEffectiveMaxOutputTokens_TypicalSmallContext_IsNotTinyWhenToolTurnReserveIsLow()
    {
        var options = new AgentOptions
        {
            ContextSlotTokens = 4096,
            HostContextTokens = 0,
            ContextSafetyMarginTokens = 384,
            ReasoningReserveTokens = 768,
            MaxOutputTokens = 4096,
            MinAssistantCompletionTokens = 448,
            MultiAgent = new MultiAgentOrchestrationOptions
            {
                ToolTurnMinOutputTokens = 512,
                ToolTurnReasoningReserveTokens = 128,
                SpecialistToolCallMaxOutputTokens = 900
            }
        };

        var estimatedPrompt = 2900;
        var toolTurn = AgentContextBudgetEstimator.ComputeToolTurnEffectiveMaxOutputTokens(
            options,
            estimatedPrompt,
            lastUsage: null);

        Assert.True(toolTurn >= 512, $"tool turn max was {toolTurn}, expected at least the configured tool-turn floor");
        var capped = Math.Min(toolTurn, options.MultiAgent.SpecialistToolCallMaxOutputTokens);
        Assert.True(capped >= 512);
    }
}
