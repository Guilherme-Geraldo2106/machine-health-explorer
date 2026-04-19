using System.Text.Json.Nodes;

namespace MachineHealthExplorer.Agent.Serialization;

/// <summary>
/// Ensures JSON Schemas sent to OpenAI-compatible tool APIs (including LM Studio) declare
/// <c>properties</c> for every <c>type: "object"</c> node. Some servers reject payloads where
/// <c>function.parameters</c> is an object schema without <c>properties</c>, including nested definitions.
/// </summary>
public static class OpenAiCompatibleToolParametersNormalizer
{
    public static JsonNode PrepareToolParametersSchema(JsonNode schemaRoot)
    {
        ArgumentNullException.ThrowIfNull(schemaRoot);
        NormalizeRecursive(schemaRoot);
        return schemaRoot;
    }

    private static void NormalizeRecursive(JsonNode? node)
    {
        switch (node)
        {
            case JsonObject obj:
                EnsurePropertiesWhenObjectType(obj);
                foreach (var pair in obj.ToList())
                {
                    NormalizeRecursive(pair.Value);
                }

                break;
            case JsonArray arr:
                foreach (var item in arr)
                {
                    NormalizeRecursive(item);
                }

                break;
        }
    }

    private static void EnsurePropertiesWhenObjectType(JsonObject obj)
    {
        if (!TypeIncludesObject(obj))
        {
            return;
        }

        if (!obj.TryGetPropertyValue("properties", out var props) || props is not JsonObject)
        {
            obj["properties"] = new JsonObject();
        }
    }

    private static bool TypeIncludesObject(JsonObject obj)
    {
        if (!obj.TryGetPropertyValue("type", out var typeNode) || typeNode is null)
        {
            return false;
        }

        return TypeNodeIsOrIncludesObject(typeNode);
    }

    private static bool TypeNodeIsOrIncludesObject(JsonNode typeNode)
    {
        switch (typeNode)
        {
            case JsonValue value when value.TryGetValue<string>(out var s):
                return string.Equals(s, "object", StringComparison.Ordinal);
            case JsonArray array:
                foreach (var item in array)
                {
                    if (item is JsonValue v && v.TryGetValue<string>(out var element)
                        && string.Equals(element, "object", StringComparison.Ordinal))
                    {
                        return true;
                    }
                }

                return false;
            default:
                return false;
        }
    }
}
