namespace MachineHealthExplorer.Agent.Services;

internal static class AgentFinishReason
{
    public static bool IsTruncated(string? finishReason)
    {
        if (string.IsNullOrWhiteSpace(finishReason))
        {
            return false;
        }

        return finishReason.Trim().ToLowerInvariant() switch
        {
            "length" or "max_tokens" or "max_length" or "model_length" => true,
            _ => false
        };
    }

    public static bool IsToolCalls(string? finishReason)
    {
        if (string.IsNullOrWhiteSpace(finishReason))
        {
            return false;
        }

        return finishReason.Trim().Equals("tool_calls", StringComparison.OrdinalIgnoreCase);
    }
}
