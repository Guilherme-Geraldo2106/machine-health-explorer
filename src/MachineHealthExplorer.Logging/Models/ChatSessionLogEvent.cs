using System.Text.Json.Serialization;

namespace MachineHealthExplorer.Logging.Models;

public sealed record ChatSessionLogEvent(
    [property: JsonPropertyName("timestamp")] DateTimeOffset Timestamp,
    [property: JsonPropertyName("sessionId")] string SessionId,
    [property: JsonPropertyName("eventType")] string EventType,
    [property: JsonPropertyName("direction")] string Direction,
    [property: JsonPropertyName("model")] string? Model,
    [property: JsonPropertyName("finishReason")] string? FinishReason,
    [property: JsonPropertyName("toolCallsSummary")] IReadOnlyList<ChatToolCallLogSummary>? ToolCallsSummary,
    [property: JsonPropertyName("requestPayloadJson")] string? RequestPayloadJson,
    [property: JsonPropertyName("responsePayloadRaw")] string? ResponsePayloadRaw,
    [property: JsonPropertyName("httpStatusCode")] int? HttpStatusCode,
    [property: JsonPropertyName("error")] string? Error);

public sealed record ChatToolCallLogSummary(
    [property: JsonPropertyName("id")] string? Id,
    [property: JsonPropertyName("name")] string Name,
    [property: JsonPropertyName("argumentsLength")] int ArgumentsLength);
