using System.Text.Json;
using System.Text.Json.Serialization;

namespace MachineHealthExplorer.Agent.Serialization;

internal static class AgentToolJsonDeserializer
{
    private static readonly JsonSerializerOptions StrictOptions = CreateStrictOptions();

    private static JsonSerializerOptions CreateStrictOptions()
    {
        var options = new JsonSerializerOptions(AgentJsonSerializer.Options)
        {
            UnmappedMemberHandling = JsonUnmappedMemberHandling.Disallow
        };
        return options;
    }

    public static bool TryDeserialize<T>(string json, out T? value, out string? errorMessage)
    {
        value = default;
        errorMessage = null;
        try
        {
            value = JsonSerializer.Deserialize<T>(json, StrictOptions);
            if (value is null)
            {
                errorMessage = $"Could not deserialize '{typeof(T).Name}' (null result).";
                return false;
            }

            return true;
        }
        catch (JsonException exception)
        {
            errorMessage = exception.Message;
            return false;
        }
    }
}
