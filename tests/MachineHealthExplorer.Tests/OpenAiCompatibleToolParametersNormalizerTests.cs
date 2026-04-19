using MachineHealthExplorer.Agent.Models;
using MachineHealthExplorer.Agent.Serialization;
using MachineHealthExplorer.Agent.Services;
using System.Net;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Linq;

namespace MachineHealthExplorer.Tests;

public sealed class OpenAiCompatibleToolParametersNormalizerTests
{
    [Fact]
    public void PrepareToolParametersSchema_AddsEmptyProperties_WhenObjectTypeHasNone()
    {
        var schema = JsonNode.Parse("""{"type":"object","additionalProperties":true}""")!;
        OpenAiCompatibleToolParametersNormalizer.PrepareToolParametersSchema(schema);

        using var doc = JsonDocument.Parse(schema.ToJsonString());
        Assert.True(doc.RootElement.TryGetProperty("properties", out var props));
        Assert.Equal(JsonValueKind.Object, props.ValueKind);
        Assert.Empty(props.EnumerateObject());
    }

    [Fact]
    public void PrepareToolParametersSchema_FillsNestedObjectSchemas_WithoutProperties()
    {
        const string json = """
        {
          "type": "object",
          "properties": {
            "filter": {
              "type": "object",
              "description": "nested",
              "additionalProperties": true
            }
          },
          "additionalProperties": false
        }
        """;

        var schema = JsonNode.Parse(json)!;
        OpenAiCompatibleToolParametersNormalizer.PrepareToolParametersSchema(schema);

        using var doc = JsonDocument.Parse(schema.ToJsonString());
        var filter = doc.RootElement.GetProperty("properties").GetProperty("filter");
        Assert.True(filter.TryGetProperty("properties", out var nestedProps));
        Assert.Equal(JsonValueKind.Object, nestedProps.ValueKind);
    }

    [Fact]
    public void PrepareToolParametersSchema_WhenTypeIsArrayIncludingObject_AddsProperties()
    {
        var schema = JsonNode.Parse("""{"type":["null","object"],"additionalProperties":true}""")!;
        OpenAiCompatibleToolParametersNormalizer.PrepareToolParametersSchema(schema);

        using var doc = JsonDocument.Parse(schema.ToJsonString());
        Assert.True(doc.RootElement.TryGetProperty("properties", out var props));
        Assert.Equal(JsonValueKind.Object, props.ValueKind);
    }

    [Fact]
    public void PrepareToolParametersSchema_ReplacesNonObjectProperties_WithEmptyObject()
    {
        var schema = JsonNode.Parse("""{"type":"object","properties":false,"additionalProperties":true}""")!;
        OpenAiCompatibleToolParametersNormalizer.PrepareToolParametersSchema(schema);

        using var doc = JsonDocument.Parse(schema.ToJsonString());
        var props = doc.RootElement.GetProperty("properties");
        Assert.Equal(JsonValueKind.Object, props.ValueKind);
    }

    [Fact]
    public async Task LmStudioChatClient_WithUseMinimalToolSchemas_SendsParametersWithProperties()
    {
        const string responseJson = """
        {
          "choices": [
            {
              "message": { "role": "assistant", "content": "ok" },
              "finish_reason": "stop"
            }
          ]
        }
        """;

        var handler = new CapturingHttpMessageHandler(responseJson);
        using var httpClient = new HttpClient(handler)
        {
            BaseAddress = new Uri("http://127.0.0.1:1234/v1/")
        };

        using var chatClient = new LmStudioChatClient(
            new AgentOptions { BaseUrl = "http://127.0.0.1:1234/v1", Model = "m" },
            httpClient);

        _ = await chatClient.CompleteAsync(new AgentModelRequest
        {
            Model = "m",
            SystemPrompt = "sys",
            EnableTools = true,
            UseMinimalToolSchemas = true,
            Messages = [new AgentConversationMessage { Role = AgentConversationRole.User, Content = "hi" }],
            Tools =
            [
                new AgentToolDefinition
                {
                    Name = "describe_dataset",
                    Description = "d",
                    ParametersJsonSchema = """{"type":"object","additionalProperties":true}"""
                }
            ]
        });

        Assert.False(string.IsNullOrWhiteSpace(handler.LastRequestBody));
        using var payload = JsonDocument.Parse(handler.LastRequestBody!);
        var tools = payload.RootElement.GetProperty("tools");
        var parameters = tools[0].GetProperty("function").GetProperty("parameters");
        AssertToolParametersSchemaHasObjectPropertiesRecursively(parameters);
    }

    [Fact]
    public async Task LmStudioChatClient_WithoutMinimalSchemas_NormalizesNestedObjectsInFullSchema()
    {
        const string responseJson = """
        {
          "choices": [
            {
              "message": { "role": "assistant", "content": "ok" },
              "finish_reason": "stop"
            }
          ]
        }
        """;

        var handler = new CapturingHttpMessageHandler(responseJson);
        using var httpClient = new HttpClient(handler)
        {
            BaseAddress = new Uri("http://127.0.0.1:1234/v1/")
        };

        using var chatClient = new LmStudioChatClient(
            new AgentOptions { BaseUrl = "http://127.0.0.1:1234/v1", Model = "m" },
            httpClient);

        const string schemaWithNestedObjectSansProperties = """
        {
          "type": "object",
          "properties": {
            "filter": {
              "type": "object",
              "additionalProperties": true
            }
          },
          "additionalProperties": false
        }
        """;

        _ = await chatClient.CompleteAsync(new AgentModelRequest
        {
            Model = "m",
            SystemPrompt = "sys",
            EnableTools = true,
            UseMinimalToolSchemas = false,
            Messages = [new AgentConversationMessage { Role = AgentConversationRole.User, Content = "hi" }],
            Tools =
            [
                new AgentToolDefinition
                {
                    Name = "query_rows",
                    Description = "q",
                    ParametersJsonSchema = schemaWithNestedObjectSansProperties
                }
            ]
        });

        using var payload = JsonDocument.Parse(handler.LastRequestBody!);
        var parameters = payload.RootElement.GetProperty("tools")[0].GetProperty("function").GetProperty("parameters");
        AssertToolParametersSchemaHasObjectPropertiesRecursively(parameters);
    }

    private static void AssertToolParametersSchemaHasObjectPropertiesRecursively(JsonElement node)
    {
        switch (node.ValueKind)
        {
            case JsonValueKind.Object:
                if (TypeIncludesObject(node)
                    && (!node.TryGetProperty("properties", out var props) || props.ValueKind != JsonValueKind.Object))
                {
                    Assert.Fail("Object-typed schema node is missing an object-valued \"properties\".");
                }

                if (node.TryGetProperty("properties", out var pObj) && pObj.ValueKind == JsonValueKind.Object)
                {
                    foreach (var child in pObj.EnumerateObject())
                    {
                        AssertToolParametersSchemaHasObjectPropertiesRecursively(child.Value);
                    }
                }

                foreach (var prop in node.EnumerateObject())
                {
                    if (string.Equals(prop.Name, "properties", StringComparison.Ordinal))
                    {
                        continue;
                    }

                    AssertToolParametersSchemaHasObjectPropertiesRecursively(prop.Value);
                }

                break;
            case JsonValueKind.Array:
                foreach (var item in node.EnumerateArray())
                {
                    AssertToolParametersSchemaHasObjectPropertiesRecursively(item);
                }

                break;
            default:
                break;
        }
    }

    private static bool TypeIncludesObject(JsonElement schemaNode)
    {
        if (!schemaNode.TryGetProperty("type", out var typeProp))
        {
            return false;
        }

        return typeProp.ValueKind switch
        {
            JsonValueKind.String => string.Equals(typeProp.GetString(), "object", StringComparison.Ordinal),
            JsonValueKind.Array => typeProp.EnumerateArray().Any(element =>
                element.ValueKind == JsonValueKind.String
                && string.Equals(element.GetString(), "object", StringComparison.Ordinal)),
            _ => false
        };
    }

    private sealed class CapturingHttpMessageHandler : HttpMessageHandler
    {
        private readonly string _responseJson;

        public CapturingHttpMessageHandler(string responseJson)
        {
            _responseJson = responseJson;
        }

        public string? LastRequestBody { get; private set; }

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            LastRequestBody = request.Content is null
                ? null
                : await request.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);

            return new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(_responseJson, Encoding.UTF8, "application/json")
            };
        }
    }
}
