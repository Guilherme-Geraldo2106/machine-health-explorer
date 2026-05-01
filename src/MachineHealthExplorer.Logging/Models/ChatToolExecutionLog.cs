using System.Text.Json.Serialization;

namespace MachineHealthExplorer.Logging.Models;

public sealed record ChatToolExecutionLog(
    [property: JsonPropertyName("toolName")] string ToolName,
    [property: JsonPropertyName("argumentsLength")] int ArgumentsLength,
    [property: JsonPropertyName("resultOriginalLength")] int ResultOriginalLength,
    [property: JsonPropertyName("previewLength")] int PreviewLength,
    [property: JsonPropertyName("isError")] bool IsError,
    [property: JsonPropertyName("elapsedMs")] double ElapsedMs);
