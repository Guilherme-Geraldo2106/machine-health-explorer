using System.Text.Json.Serialization;

namespace MachineHealthExplorer.Logging.Models;

public sealed record ChatLlmRequestSummary(
    [property: JsonPropertyName("maxTokens")] int MaxTokens,
    [property: JsonPropertyName("responseFormatType")] string? ResponseFormatType,
    [property: JsonPropertyName("responseFormatSchemaName")] string? ResponseFormatSchemaName,
    [property: JsonPropertyName("toolChoice")] string? ToolChoice,
    [property: JsonPropertyName("parallelToolCalls")] bool? ParallelToolCalls);
