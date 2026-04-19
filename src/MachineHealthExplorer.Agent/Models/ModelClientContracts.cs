namespace MachineHealthExplorer.Agent.Models;

public sealed record AgentToolDefinition
{
    public string Name { get; init; } = string.Empty;
    public string Description { get; init; } = string.Empty;
    public string ParametersJsonSchema { get; init; } = """{"type":"object","properties":{},"additionalProperties":false}""";
}

public sealed record AgentModelRequest
{
    public string Model { get; init; } = string.Empty;
    public string SystemPrompt { get; init; } = string.Empty;
    public IReadOnlyList<AgentConversationMessage> Messages { get; init; } = Array.Empty<AgentConversationMessage>();
    public IReadOnlyList<AgentToolDefinition> Tools { get; init; } = Array.Empty<AgentToolDefinition>();
    public double Temperature { get; init; } = 0.1;
    public int MaxOutputTokens { get; init; } = 1024;
    public bool EnableTools { get; init; } = true;
    public bool UseMinimalToolSchemas { get; init; }
}

public sealed record AgentModelResponse
{
    public string Model { get; init; } = string.Empty;
    public string? Content { get; init; }
    public string? ReasoningContent { get; init; }
    public string FinishReason { get; init; } = string.Empty;
    public IReadOnlyList<AgentToolCall> ToolCalls { get; init; } = Array.Empty<AgentToolCall>();
    public AgentTokenUsage? Usage { get; init; }
}
