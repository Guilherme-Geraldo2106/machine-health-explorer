namespace MachineHealthExplorer.Agent.Models;

/// <summary>
/// OpenAI-compatible structured output (<c>response_format.type=json_schema</c>).
/// </summary>
public sealed record AgentJsonSchemaResponseFormat
{
    /// <summary>OpenAI/LM Studio envelope: typically <c>json_schema</c>.</summary>
    public string Type { get; init; } = "json_schema";

    /// <summary>Logical name for the schema (provider-specific).</summary>
    public string Name { get; init; } = string.Empty;

    /// <summary>When true, requires a fully closed JSON Schema (e.g. additionalProperties:false everywhere). Not all local backends accept this.</summary>
    public bool Strict { get; init; }

    /// <summary>JSON Schema object as a JSON string (object root).</summary>
    public string SchemaJson { get; init; } = "{}";
}

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
    /// <summary>
    /// When true with tools enabled, requests tool_choice=required (OpenAI-compatible). Used only on recovery turns when configured.
    /// </summary>
    public bool RequireToolCall { get; init; }
    /// <summary>
    /// When false, adds <c>parallel_tool_calls=false</c> for tool-enabled requests (expects at most one tool call).
    /// When null, property is omitted.
    /// </summary>
    public bool? ParallelToolCalls { get; init; }
    /// <summary>Optional structured output; client may retry without it if the backend rejects the field.</summary>
    public AgentJsonSchemaResponseFormat? ResponseFormat { get; init; }
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
