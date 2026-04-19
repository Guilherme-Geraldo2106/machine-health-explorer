using System.Text.Json;
using System.Text.Json.Serialization;

namespace MachineHealthExplorer.Logging.Serialization;

internal static class ChatSessionLogJson
{
    internal static readonly JsonSerializerOptions Options = new()
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        WriteIndented = false
    };

    public static string SerializeLine<T>(T value)
        => JsonSerializer.Serialize(value, Options);
}
