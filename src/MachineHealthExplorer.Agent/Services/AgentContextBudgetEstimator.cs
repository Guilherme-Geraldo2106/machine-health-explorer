using MachineHealthExplorer.Agent.Models;

namespace MachineHealthExplorer.Agent.Services;

internal static class AgentContextBudgetEstimator
{
    public static int GetEffectiveHostContextTokens(AgentOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        return options.HostContextTokens > 0 ? options.HostContextTokens : options.ContextSlotTokens;
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

    public static int ComputeEffectiveMaxOutputTokens(
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
        var remainingForCompletion = softContext - estimatedPromptTokens - reasoningReserve;
        var capped = Math.Clamp(remainingForCompletion, 96, options.MaxOutputTokens);
        return Math.Min(options.MaxOutputTokens, capped);
    }
}
