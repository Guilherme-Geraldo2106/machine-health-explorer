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

        var errorMessage = TryReadErrorProperty(execution.ResultJson) ?? execution.ResultJson ?? string.Empty;

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
