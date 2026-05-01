using System.Text.Json.Serialization;

namespace MachineHealthExplorer.Logging.Models;

public sealed record ChatCompletionTokenLog(
    [property: JsonPropertyName("promptTokens")] int? PromptTokens,
    [property: JsonPropertyName("completionTokens")] int? CompletionTokens,
    [property: JsonPropertyName("totalTokens")] int? TotalTokens,
    [property: JsonPropertyName("reasoningTokens")] int? ReasoningTokens,
    [property: JsonPropertyName("cachedPromptTokens")] int? CachedPromptTokens,
    [property: JsonPropertyName("promptTokensDetailsJson")] string? PromptTokensDetailsJson);
