namespace MachineHealthExplorer.Agent.Models;

public sealed record AgentTokenUsage
{
    public int PromptTokens { get; init; }
    public int CompletionTokens { get; init; }
    public int TotalTokens { get; init; }
    public int? ReasoningTokens { get; init; }
}
