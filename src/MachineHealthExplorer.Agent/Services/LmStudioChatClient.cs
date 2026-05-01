using MachineHealthExplorer.Agent.Abstractions;
using MachineHealthExplorer.Agent.Models;
using MachineHealthExplorer.Agent.Serialization;
using MachineHealthExplorer.Logging.Abstractions;
using MachineHealthExplorer.Logging.Models;
using MachineHealthExplorer.Logging.Services;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;

namespace MachineHealthExplorer.Agent.Services;

public sealed class LmStudioChatClient : IAgentChatClient, IDisposable
{
    private static readonly JsonNode MinimalToolParametersTemplate =
        JsonNode.Parse("""{"type":"object","properties":{},"additionalProperties":true}""")!;

    private readonly AgentOptions _options;
    private readonly HttpClient _httpClient;
    private readonly bool _ownsHttpClient;
    private readonly IChatSessionLogger _chatSessionLogger;

    public LmStudioChatClient(AgentOptions options)
        : this(options, httpClient: null, chatSessionLogger: null)
    {
    }

    public LmStudioChatClient(AgentOptions options, HttpClient? httpClient, IChatSessionLogger? chatSessionLogger = null)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _httpClient = httpClient ?? new HttpClient();
        _ownsHttpClient = httpClient is null;
        _chatSessionLogger = chatSessionLogger ?? NullChatSessionLogger.Instance;

        if (_httpClient.BaseAddress is null)
        {
            _httpClient.BaseAddress = new Uri(NormalizeBaseUrl(_options.BaseUrl), UriKind.Absolute);
        }
    }

    public async Task<AgentModelResponse> CompleteAsync(AgentModelRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        var wantStructured = _options.EnableStructuredJsonOutputs && request.ResponseFormat is not null;
        try
        {
            return await SendChatCompletionCoreAsync(request, includeResponseFormat: wantStructured, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (InvalidOperationException exception) when (wantStructured
                                                         && AgentModelBackendException.LooksLikeResponseFormatRejected(exception.Message))
        {
            return await SendChatCompletionCoreAsync(request, includeResponseFormat: false, cancellationToken)
                .ConfigureAwait(false);
        }
    }

    private async Task<AgentModelResponse> SendChatCompletionCoreAsync(
        AgentModelRequest request,
        bool includeResponseFormat,
        CancellationToken cancellationToken)
    {
        var requestBodyJson = BuildChatCompletionRequestJson(request, includeResponseFormat);
        using var httpRequest = new HttpRequestMessage(HttpMethod.Post, "chat/completions")
        {
            Content = new StringContent(
                requestBodyJson,
                Encoding.UTF8,
                "application/json")
        };

        if (!string.IsNullOrWhiteSpace(_options.ApiKey))
        {
            httpRequest.Headers.Authorization = new AuthenticationHeaderValue("Bearer", _options.ApiKey);
        }

        var requestSummary = BuildLlmRequestSummary(request, includeResponseFormat);
        _chatSessionLogger.Append(new ChatSessionLogEvent(
            default,
            string.Empty,
            "lm_studio.chat_completions.request",
            "outbound",
            request.Model,
            null,
            null,
            requestBodyJson,
            null,
            null,
            null,
            LlmRequestSummary: requestSummary));

        string content;
        HttpResponseMessage response;
        try
        {
            response = await _httpClient.SendAsync(httpRequest, cancellationToken).ConfigureAwait(false);
            content = await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            _chatSessionLogger.Append(new ChatSessionLogEvent(
                default,
                string.Empty,
                "lm_studio.chat_completions.error",
                "error",
                request.Model,
                null,
                null,
                requestBodyJson,
                null,
                null,
                exception.Message));
            throw;
        }

        using (response)
        {
            if (!response.IsSuccessStatusCode)
            {
                var httpError = $"LM Studio request failed with {(int)response.StatusCode} ({response.ReasonPhrase}): {content}";
                _chatSessionLogger.Append(new ChatSessionLogEvent(
                    default,
                    string.Empty,
                    "lm_studio.chat_completions.error",
                    "error",
                    request.Model,
                    null,
                    null,
                    requestBodyJson,
                    content,
                    (int)response.StatusCode,
                    httpError));
                if ((int)response.StatusCode == 400 && AgentModelBackendException.LooksLikeContextLengthExceeded(content))
                {
                    throw new AgentModelBackendException(httpError, (int)response.StatusCode, content, isContextLengthExceeded: true);
                }

                throw new InvalidOperationException(httpError);
            }

            var completion = AgentJsonSerializer.Deserialize<ChatCompletionResponse>(content);
            var choice = completion.Choices.FirstOrDefault()
                ?? throw new InvalidOperationException("LM Studio returned no completion choices.");

            var registeredToolNames = request.Tools.Select(tool => tool.Name).ToArray();
            var normalizedToolCalls = NormalizeApiToolCalls(choice.Message.ToolCalls, registeredToolNames);
            var responseContent = choice.Message.Content;

            if (request.EnableTools && registeredToolNames.Length > 0 && normalizedToolCalls.Count == 0
                && !string.IsNullOrWhiteSpace(responseContent))
            {
                var (pseudoCalls, strippedContent) = AgentPseudoToolCallExtractor.Extract(responseContent, registeredToolNames);
                responseContent = strippedContent;
                if (pseudoCalls.Count > 0)
                {
                    normalizedToolCalls = pseudoCalls.ToList();
                }
            }

            var usageModel = MapUsage(completion.Usage);
            var tokenLog = BuildCompletionTokenLog(usageModel, content);
            _chatSessionLogger.Append(new ChatSessionLogEvent(
                default,
                string.Empty,
                "lm_studio.chat_completions.response",
                "inbound",
                completion.Model ?? request.Model,
                choice.FinishReason,
                SummarizeToolCalls(choice.Message.ToolCalls),
                null,
                content,
                null,
                null,
                LlmRequestSummary: requestSummary,
                CompletionTokenUsage: tokenLog));

            return new AgentModelResponse
            {
                Model = completion.Model ?? request.Model,
                Content = responseContent,
                ReasoningContent = choice.Message.ReasoningContent,
                FinishReason = choice.FinishReason ?? string.Empty,
                ToolCalls = normalizedToolCalls,
                Usage = usageModel
            };
        }
    }

    private string BuildChatCompletionRequestJson(AgentModelRequest request, bool includeResponseFormat)
    {
        var includeTools = request.EnableTools && request.Tools.Count > 0;
        var root = new JsonObject
        {
            ["model"] = request.Model,
            ["temperature"] = request.Temperature,
            ["max_tokens"] = request.MaxOutputTokens
        };

        var messages = new JsonArray
        {
            new JsonObject
            {
                ["role"] = "system",
                ["content"] = request.SystemPrompt
            }
        };

        foreach (var message in request.Messages)
        {
            messages.Add(ToApiMessageNode(message));
        }

        root["messages"] = messages;

        if (includeTools)
        {
            var tools = new JsonArray();
            foreach (var tool in request.Tools)
            {
                var parametersNode = request.UseMinimalToolSchemas
                    ? MinimalToolParametersTemplate.DeepClone()
                    : JsonNode.Parse(tool.ParametersJsonSchema)!;
                OpenAiCompatibleToolParametersNormalizer.PrepareToolParametersSchema(parametersNode);
                var fn = new JsonObject
                {
                    ["name"] = tool.Name,
                    ["description"] = tool.Description,
                    ["parameters"] = parametersNode
                };
                if (_options.EnableStrictToolParameterSchemas)
                {
                    fn["strict"] = true;
                }

                tools.Add(new JsonObject
                {
                    ["type"] = "function",
                    ["function"] = fn
                });
            }

            root["tools"] = tools;
            root["tool_choice"] = request.RequireToolCall ? "required" : "auto";
            if (request.ParallelToolCalls == false)
            {
                root["parallel_tool_calls"] = false;
            }
        }

        if (includeResponseFormat && request.ResponseFormat is { } rf)
        {
            JsonNode schemaNode;
            try
            {
                schemaNode = JsonNode.Parse(string.IsNullOrWhiteSpace(rf.SchemaJson) ? "{}" : rf.SchemaJson)!;
            }
            catch (JsonException)
            {
                schemaNode = new JsonObject();
            }

            var strict = rf.Strict || _options.UseStrictJsonSchemaInResponseFormat;
            root["response_format"] = new JsonObject
            {
                ["type"] = string.IsNullOrWhiteSpace(rf.Type) ? "json_schema" : rf.Type,
                ["json_schema"] = new JsonObject
                {
                    ["name"] = string.IsNullOrWhiteSpace(rf.Name) ? "schema" : rf.Name,
                    ["strict"] = strict,
                    ["schema"] = schemaNode
                }
            };
        }

        return root.ToJsonString(new JsonSerializerOptions { WriteIndented = false });
    }

    private static JsonObject ToApiMessageNode(AgentConversationMessage message)
    {
        var role = message.Role switch
        {
            AgentConversationRole.System => "system",
            AgentConversationRole.User => "user",
            AgentConversationRole.Assistant => "assistant",
            AgentConversationRole.Tool => "tool",
            _ => "user"
        };

        if (message.Role == AgentConversationRole.Assistant && message.ToolCalls.Count > 0)
        {
            var toolCalls = new JsonArray();
            foreach (var toolCall in message.ToolCalls)
            {
                toolCalls.Add(new JsonObject
                {
                    ["id"] = toolCall.Id,
                    ["type"] = "function",
                    ["function"] = new JsonObject
                    {
                        ["name"] = toolCall.Name,
                        ["arguments"] = toolCall.ArgumentsJson
                    }
                });
            }

            return new JsonObject
            {
                ["role"] = role,
                ["content"] = string.IsNullOrWhiteSpace(message.Content) ? null : message.Content,
                ["tool_calls"] = toolCalls
            };
        }

        if (message.Role == AgentConversationRole.Tool)
        {
            return new JsonObject
            {
                ["role"] = role,
                ["content"] = message.Content ?? string.Empty,
                ["tool_call_id"] = message.ToolCallId,
                ["name"] = message.Name
            };
        }

        return new JsonObject
        {
            ["role"] = role,
            ["content"] = message.Content ?? string.Empty
        };
    }

    private static List<AgentToolCall> NormalizeApiToolCalls(
        IReadOnlyList<ChatCompletionToolCall>? toolCalls,
        IReadOnlyList<string> registeredToolNames)
    {
        if (toolCalls is null || toolCalls.Count == 0)
        {
            return new List<AgentToolCall>();
        }

        var result = new List<AgentToolCall>();
        foreach (var toolCall in toolCalls)
        {
            var rawName = toolCall.Function?.Name ?? string.Empty;
            if (string.IsNullOrWhiteSpace(rawName))
            {
                continue;
            }

            string resolvedName;
            if (registeredToolNames.Count > 0)
            {
                var mapped = AgentToolInvocationCanonicalizer.TryResolveRegisteredName(rawName, registeredToolNames);
                if (mapped is null)
                {
                    continue;
                }

                resolvedName = mapped;
            }
            else
            {
                resolvedName = AgentToolInvocationCanonicalizer.ApplyKnownAliases(rawName);
            }

            result.Add(new AgentToolCall
            {
                Id = toolCall.Id ?? string.Empty,
                Name = resolvedName,
                ArgumentsJson = string.IsNullOrWhiteSpace(toolCall.Function?.Arguments)
                    ? "{}"
                    : toolCall.Function!.Arguments!
            });
        }

        return result;
    }

    private static IReadOnlyList<ChatToolCallLogSummary>? SummarizeToolCalls(IReadOnlyList<ChatCompletionToolCall>? toolCalls)
    {
        if (toolCalls is null || toolCalls.Count == 0)
        {
            return null;
        }

        return toolCalls
            .Select(call => new ChatToolCallLogSummary(
                call.Id,
                call.Function?.Name ?? string.Empty,
                call.Function?.Arguments?.Length ?? 0))
            .Where(summary => !string.IsNullOrWhiteSpace(summary.Name))
            .ToArray();
    }

    public async Task<IReadOnlyList<string>> GetAvailableModelsAsync(CancellationToken cancellationToken = default)
    {
        using var httpRequest = new HttpRequestMessage(HttpMethod.Get, "models");
        if (!string.IsNullOrWhiteSpace(_options.ApiKey))
        {
            httpRequest.Headers.Authorization = new AuthenticationHeaderValue("Bearer", _options.ApiKey);
        }

        using var response = await _httpClient.SendAsync(httpRequest, cancellationToken).ConfigureAwait(false);
        var content = await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
        if (!response.IsSuccessStatusCode)
        {
            throw new InvalidOperationException(
                $"LM Studio model discovery failed with {(int)response.StatusCode} ({response.ReasonPhrase}): {content}");
        }

        var models = AgentJsonSerializer.Deserialize<ModelListResponse>(content);
        return models.Data
            .Select(model => model.Id)
            .Where(id => !string.IsNullOrWhiteSpace(id))
            .ToArray();
    }

    public void Dispose()
    {
        if (_ownsHttpClient)
        {
            _httpClient.Dispose();
        }
    }

    private static ChatLlmRequestSummary BuildLlmRequestSummary(AgentModelRequest request, bool includeResponseFormat)
    {
        string? rfType = null;
        string? rfName = null;
        if (includeResponseFormat && request.ResponseFormat is { } rf)
        {
            rfType = rf.Type;
            rfName = rf.Name;
        }

        string? toolChoice = null;
        if (request.EnableTools && request.Tools.Count > 0)
        {
            toolChoice = request.RequireToolCall ? "required" : "auto";
        }

        return new ChatLlmRequestSummary(
            request.MaxOutputTokens,
            rfType,
            rfName,
            toolChoice,
            request.ParallelToolCalls);
    }

    private static ChatCompletionTokenLog? BuildCompletionTokenLog(AgentTokenUsage? usage, string rawResponseJson)
    {
        if (usage is null)
        {
            return null;
        }

        return new ChatCompletionTokenLog(
            usage.PromptTokens,
            usage.CompletionTokens,
            usage.TotalTokens,
            usage.ReasoningTokens,
            usage.CachedPromptTokens,
            TryExtractPromptTokensDetailsJson(rawResponseJson));
    }

    private static string? TryExtractPromptTokensDetailsJson(string rawResponseJson)
    {
        try
        {
            using var document = JsonDocument.Parse(rawResponseJson);
            if (document.RootElement.TryGetProperty("usage", out var usage)
                && usage.TryGetProperty("prompt_tokens_details", out var details))
            {
                return details.GetRawText();
            }
        }
        catch (JsonException)
        {
        }

        return null;
    }

    private static AgentTokenUsage? MapUsage(ChatCompletionUsage? usage)
    {
        if (usage is null)
        {
            return null;
        }

        var reasoning = usage.CompletionTokensDetails?.ReasoningTokens;
        var cached = usage.PromptTokensDetails?.CachedTokens;
        return new AgentTokenUsage
        {
            PromptTokens = usage.PromptTokens,
            CompletionTokens = usage.CompletionTokens,
            TotalTokens = usage.TotalTokens,
            ReasoningTokens = reasoning,
            CachedPromptTokens = cached
        };
    }

    private static string NormalizeBaseUrl(string baseUrl)
    {
        var normalized = string.IsNullOrWhiteSpace(baseUrl)
            ? "http://127.0.0.1:1234/v1"
            : baseUrl.Trim();

        return normalized.EndsWith("/", StringComparison.Ordinal)
            ? normalized
            : $"{normalized}/";
    }

    private sealed record ChatCompletionResponse
    {
        public string? Model { get; init; }
        public IReadOnlyList<ChatCompletionChoice> Choices { get; init; } = Array.Empty<ChatCompletionChoice>();
        public ChatCompletionUsage? Usage { get; init; }
    }

    private sealed record ChatCompletionUsage
    {
        [JsonPropertyName("prompt_tokens")]
        public int PromptTokens { get; init; }

        [JsonPropertyName("completion_tokens")]
        public int CompletionTokens { get; init; }

        [JsonPropertyName("total_tokens")]
        public int TotalTokens { get; init; }

        [JsonPropertyName("completion_tokens_details")]
        public ChatCompletionCompletionTokenDetails? CompletionTokensDetails { get; init; }

        [JsonPropertyName("prompt_tokens_details")]
        public ChatCompletionPromptTokenDetails? PromptTokensDetails { get; init; }
    }

    private sealed record ChatCompletionPromptTokenDetails
    {
        [JsonPropertyName("cached_tokens")]
        public int? CachedTokens { get; init; }
    }

    private sealed record ChatCompletionCompletionTokenDetails
    {
        [JsonPropertyName("reasoning_tokens")]
        public int? ReasoningTokens { get; init; }
    }

    private sealed record ChatCompletionChoice
    {
        [JsonPropertyName("finish_reason")]
        public string? FinishReason { get; init; }
        public ChatCompletionMessage Message { get; init; } = new();
    }

    private sealed record ChatCompletionMessage
    {
        public string? Role { get; init; }
        public string? Content { get; init; }

        [JsonPropertyName("reasoning_content")]
        public string? ReasoningContent { get; init; }

        [JsonPropertyName("tool_calls")]
        public IReadOnlyList<ChatCompletionToolCall>? ToolCalls { get; init; }
    }

    private sealed record ChatCompletionToolCall
    {
        public string? Id { get; init; }
        public string? Type { get; init; }
        public ChatCompletionFunctionCall? Function { get; init; }
    }

    private sealed record ChatCompletionFunctionCall
    {
        public string? Name { get; init; }
        public string? Arguments { get; init; }
    }

    private sealed record ModelListResponse
    {
        public IReadOnlyList<ModelInfo> Data { get; init; } = Array.Empty<ModelInfo>();
    }

    private sealed record ModelInfo
    {
        public string Id { get; init; } = string.Empty;
    }
}
