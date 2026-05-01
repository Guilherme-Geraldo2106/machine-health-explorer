using MachineHealthExplorer.Agent.Models;

namespace MachineHealthExplorer.Agent.Services;

internal static class AgentContextBudgetEstimator
{
    public static int GetEffectiveHostContextTokens(AgentOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        return options.HostContextTokens > 0 ? options.HostContextTokens : options.ContextSlotTokens;
    }

    /// <summary>
    /// Minimum max_tokens for assistant completions (no tools) — callers must not invoke the model below this when a compliant call is required.
    /// </summary>
    public static int GetAssistantCompletionFloorTokens(AgentOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        return Math.Clamp(
            options.MinAssistantCompletionTokens,
            96,
            Math.Max(96, options.MaxOutputTokens));
    }

    /// <summary>
    /// Minimum max_tokens for tool-enabled specialist turns (max of configured tool floors).
    /// </summary>
    public static int GetToolTurnCompletionFloorTokens(AgentOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        var multi = options.MultiAgent;
        var merged = Math.Max(multi.ToolTurnMinOutputTokens, multi.ToolTurnSafeMinMaxOutputTokens);
        return Math.Clamp(merged, 96, Math.Max(96, options.MaxOutputTokens));
    }

    public static int EstimatePromptTokens(
        AgentOptions options,
        string systemPrompt,
        IReadOnlyList<AgentConversationMessage> messages,
        IReadOnlyList<AgentToolDefinition> tools)
    {
        var divisor = Math.Max(2, options.ContextBudgetCharsPerToken);
        var chars = systemPrompt.Length + EstimateMessagesChars(messages) + EstimateToolsChars(tools, options);
        return Math.Max(1, chars / divisor);
    }

    public static int EstimateToolsChars(IReadOnlyList<AgentToolDefinition> tools, AgentOptions options)
    {
        var total = 0;
        foreach (var tool in tools)
        {
            var schema = tool.ParametersJsonSchema;
            if (schema.Length > options.MaxToolSchemaCharsPerTool)
            {
                schema = schema[..options.MaxToolSchemaCharsPerTool];
            }

            total += tool.Name.Length + tool.Description.Length + schema.Length + 96;
        }

        return total;
    }

    private static int EstimateMessagesChars(IReadOnlyList<AgentConversationMessage> messages)
    {
        var total = 0;
        foreach (var message in messages)
        {
            total += (message.Content ?? string.Empty).Length + (message.Name ?? string.Empty).Length + 24;
            foreach (var call in message.ToolCalls)
            {
                total += call.Name.Length + call.ArgumentsJson.Length + call.Id.Length + 32;
            }
        }

        return total;
    }

    /// <summary>
    /// Sizes max_tokens for non-tool assistant completions. Reasoning tokens count inside the same completion budget; this prefers
    /// <paramref name="visibleCompletionFloor"/> + reasoning headroom when the host context slot allows it.
    /// When <paramref name="continuationAssistantPass"/> is true, prior-turn reasoning usage must not shrink the next chunk budget.
    /// </summary>
    public static int ComputeEffectiveMaxOutputTokens(
        AgentOptions options,
        int estimatedPromptTokens,
        int reasoningPressureSteps,
        AgentTokenUsage? lastUsage,
        bool continuationAssistantPass = false,
        int? visibleCompletionFloorOverride = null)
    {
        ArgumentNullException.ThrowIfNull(options);
        var host = GetEffectiveHostContextTokens(options);
        var softContext = Math.Max(512, host - options.ContextSafetyMarginTokens);
        var slotForCompletion = softContext - estimatedPromptTokens;
        var completionFloor = visibleCompletionFloorOverride ?? GetAssistantCompletionFloorTokens(options);
        if (slotForCompletion < completionFloor)
        {
            return 0;
        }

        var composerReasoningBudget = options.MultiAgent.FinalComposerReasoningReserveTokens > 0
            ? options.MultiAgent.FinalComposerReasoningReserveTokens
            : options.ReasoningReserveTokens;

        var reasoningHeadroom = continuationAssistantPass
            ? Math.Clamp(composerReasoningBudget / 3, 96, 640)
            : composerReasoningBudget + Math.Clamp(reasoningPressureSteps, 0, 12) * 128;

        if (!continuationAssistantPass
            && lastUsage?.ReasoningTokens is { } priorReasoning
            && priorReasoning > 0)
        {
            reasoningHeadroom = Math.Max(reasoningHeadroom, Math.Clamp(priorReasoning, 256, 4096));
        }

        var minTarget = completionFloor + reasoningHeadroom;
        var capped = Math.Min(options.MaxOutputTokens, slotForCompletion);
        if (slotForCompletion < completionFloor)
        {
            return 0;
        }

        if (capped < completionFloor)
        {
            return capped;
        }

        return Math.Max(completionFloor, Math.Min(capped, Math.Max(minTarget, completionFloor)));
    }

    /// <summary>
    /// Tokens still available inside the soft context window after prompt + reasoning reserve (may be negative).
    /// </summary>
    public static int EstimateRemainingContextTokens(
        AgentOptions options,
        int estimatedPromptTokens,
        int reasoningPressureSteps,
        AgentTokenUsage? lastUsage)
    {
        var reasoningReserve = options.ReasoningReserveTokens;
        if (lastUsage?.ReasoningTokens is { } reasoning && reasoning > 0)
        {
            reasoningReserve = Math.Clamp(reasoningReserve + reasoning, 256, 4096);
        }

        reasoningReserve += Math.Clamp(reasoningPressureSteps, 0, 12) * 128;
        var host = GetEffectiveHostContextTokens(options);
        var softContext = Math.Max(512, host - options.ContextSafetyMarginTokens);
        return softContext - estimatedPromptTokens - reasoningReserve;
    }

    /// <summary>
    /// Like <see cref="EstimateRemainingContextTokens"/> but uses tool-turn reasoning reserve for sizing specialist tool rounds.
    /// </summary>
    public static int EstimateRemainingContextTokensForToolTurn(
        AgentOptions options,
        int estimatedPromptTokens,
        AgentTokenUsage? lastUsage)
    {
        var reasoningReserve = Math.Max(0, options.MultiAgent.ToolTurnReasoningReserveTokens);
        if (lastUsage?.ReasoningTokens is { } reasoning && reasoning > 0)
        {
            reasoningReserve = Math.Clamp(reasoningReserve + reasoning, 0, 2048);
        }

        var host = GetEffectiveHostContextTokens(options);
        var softContext = Math.Max(512, host - options.ContextSafetyMarginTokens);
        return softContext - estimatedPromptTokens - reasoningReserve;
    }

    /// <summary>
    /// Sizes max_tokens for tool-call turns. Returns <c>0</c> when the soft context cannot fit the tool-turn completion floor.
    /// </summary>
    public static int ComputeToolTurnEffectiveMaxOutputTokens(
        AgentOptions options,
        int estimatedPromptTokens,
        AgentTokenUsage? lastUsage)
    {
        var reasoningReserve = Math.Max(0, options.MultiAgent.ToolTurnReasoningReserveTokens);
        if (lastUsage?.ReasoningTokens is { } reasoning && reasoning > 0)
        {
            reasoningReserve = Math.Clamp(reasoningReserve + reasoning, 0, 2048);
        }

        var host = GetEffectiveHostContextTokens(options);
        var softContext = Math.Max(512, host - options.ContextSafetyMarginTokens);
        var remainingForCompletion = softContext - estimatedPromptTokens - reasoningReserve;
        var completionFloor = GetToolTurnCompletionFloorTokens(options);
        var cappedByMax = Math.Min(options.MaxOutputTokens, remainingForCompletion);

        if (remainingForCompletion < completionFloor || cappedByMax < completionFloor)
        {
            return 0;
        }

        return Math.Min(options.MaxOutputTokens, Math.Max(completionFloor, cappedByMax));
    }
}
