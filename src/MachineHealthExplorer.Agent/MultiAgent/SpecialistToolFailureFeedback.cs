using MachineHealthExplorer.Agent.Models;
using MachineHealthExplorer.Agent.Serialization;

namespace MachineHealthExplorer.Agent.MultiAgent;

internal static class SpecialistToolFailureFeedback
{
    public static string BuildToolResultJson(
        AgentToolCall toolCall,
        AgentToolExecutionRecord execution,
        IReadOnlyList<AgentToolDefinition> catalog)
    {
        var schema = string.Empty;
        foreach (var tool in catalog)
        {
            if (tool.Name.Equals(toolCall.Name, StringComparison.OrdinalIgnoreCase))
            {
                schema = tool.ParametersJsonSchema ?? string.Empty;
                break;
            }
        }

        var raw = execution.ResultJson ?? string.Empty;
        var errorMessage = PreferFullStructuredToolErrorPayload(raw, out var fullPayload)
            ? fullPayload
            : (TryReadErrorProperty(raw) ?? raw);

        return AgentJsonSerializer.Serialize(new
        {
            tool_error = true,
            tool = toolCall.Name,
            arguments_json = toolCall.ArgumentsJson ?? "{}",
            message = errorMessage,
            expected_parameters_json_schema = string.IsNullOrWhiteSpace(schema) ? "{}" : schema,
            instruction =
                "Your previous tool call failed validation or execution. Read 'message' and 'expected_parameters_json_schema', then emit a corrected tool call. Do not answer the end user in natural language until tools succeed."
        });
    }

    /// <summary>
    /// When the tool runtime already returned a compact structured payload (e.g. surface validation),
    /// surface the entire JSON to the model rather than only the human-readable <c>error</c> string.
    /// </summary>
    private static bool PreferFullStructuredToolErrorPayload(string raw, out string fullPayload)
    {
        fullPayload = raw;
        if (string.IsNullOrWhiteSpace(raw))
        {
            return false;
        }

        try
        {
            using var document = System.Text.Json.JsonDocument.Parse(raw);
            var root = document.RootElement;
            if (root.TryGetProperty("tool_error", out var te)
                && te.ValueKind is System.Text.Json.JsonValueKind.True
                && root.TryGetProperty("error_kind", out var ek)
                && ek.ValueKind is System.Text.Json.JsonValueKind.String)
            {
                return true;
            }
        }
        catch
        {
            return false;
        }

        return false;
    }

    private static string? TryReadErrorProperty(string? resultJson)
    {
        if (string.IsNullOrWhiteSpace(resultJson))
        {
            return null;
        }

        try
        {
            using var document = System.Text.Json.JsonDocument.Parse(resultJson);
            if (document.RootElement.TryGetProperty("error", out var error)
                && error.ValueKind is System.Text.Json.JsonValueKind.String)
            {
                return error.GetString();
            }
        }
        catch
        {
            return null;
        }

        return null;
    }
}
