using System.Text.Json;
using System.Text.Json.Serialization;

namespace MachineHealthExplorer.Agent.Serialization;

internal static class AgentJsonSerializer
{
    public static JsonSerializerOptions Options { get; } = CreateOptions();

    public static T Deserialize<T>(string json)
        => JsonSerializer.Deserialize<T>(string.IsNullOrWhiteSpace(json) ? "{}" : json, Options)
            ?? throw new InvalidOperationException($"Could not deserialize '{typeof(T).Name}'.");

    public static string Serialize(object? value)
        => JsonSerializer.Serialize(value, Options);

    private static JsonSerializerOptions CreateOptions()
    {
        var options = new JsonSerializerOptions(JsonSerializerDefaults.Web)
        {
            PropertyNameCaseInsensitive = true,
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
            WriteIndented = false
        };

        options.Converters.Add(new JsonStringEnumConverter());
        options.Converters.Add(new FilterExpressionJsonConverter());
        return options;
    }
}
