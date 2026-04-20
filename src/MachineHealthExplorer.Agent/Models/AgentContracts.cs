namespace MachineHealthExplorer.Agent.Models;

public sealed record AgentExecutionContext
{
    public string UserInput { get; init; } = string.Empty;
    public IReadOnlyList<AgentConversationMessage> ConversationHistory { get; init; } = Array.Empty<AgentConversationMessage>();
    public AgentConversationMemory? ConversationMemory { get; init; }
    public DateTimeOffset RequestedAtUtc { get; init; } = DateTimeOffset.UtcNow;
}

public enum AgentConversationRole
{
    System = 0,
    User = 1,
    Assistant = 2,
    Tool = 3
}

public sealed record AgentConversationMessage
{
    public AgentConversationRole Role { get; init; } = AgentConversationRole.User;
    public string? Content { get; init; }
    public string? Name { get; init; }
    public string? ToolCallId { get; init; }
    public IReadOnlyList<AgentToolCall> ToolCalls { get; init; } = Array.Empty<AgentToolCall>();
}

public sealed record AgentToolCall
{
    public string Id { get; init; } = string.Empty;
    public string Name { get; init; } = string.Empty;
    public string ArgumentsJson { get; init; } = "{}";
}

public sealed record AgentToolExecutionRecord
{
    public string ToolName { get; init; } = string.Empty;
    public string ArgumentsJson { get; init; } = "{}";
    public string ResultJson { get; init; } = "{}";
    public bool IsError { get; init; }
}

public sealed record ToolRegistrationDescriptor
{
    public string Name { get; init; } = string.Empty;
    public string Description { get; init; } = string.Empty;
    public IReadOnlyList<string> InputHints { get; init; } = Array.Empty<string>();
    public IReadOnlyList<string> ExamplePrompts { get; init; } = Array.Empty<string>();
}

public sealed record AgentExecutionResult
{
    public bool IsImplemented { get; init; }
    public string Message { get; init; } = string.Empty;
    public string Model { get; init; } = string.Empty;
    public IReadOnlyList<ToolRegistrationDescriptor> AvailableTools { get; init; } = Array.Empty<ToolRegistrationDescriptor>();
    public IReadOnlyList<AgentConversationMessage> UpdatedConversation { get; init; } = Array.Empty<AgentConversationMessage>();
    public IReadOnlyList<AgentToolExecutionRecord> ToolExecutions { get; init; } = Array.Empty<AgentToolExecutionRecord>();
    public AgentConversationMemory? UpdatedConversationMemory { get; init; }
    public bool ContinuationExhausted { get; init; }
    public AgentMultiAgentExecutionTrace? MultiAgentTrace { get; init; }
}

public sealed record AgentMultiAgentExecutionTrace(
    AgentDispatchPlan Plan,
    IReadOnlyList<AgentSpecialistRunTrace> SpecialistRuns);

public sealed record AgentSpecialistRunTrace(
    AgentSpecialistKind SpecialistKind,
    string DispatchReason,
    IReadOnlyList<string> AllowedTools,
    IReadOnlyList<string> ToolsUsed,
    bool Success,
    string? FailureMessage);
