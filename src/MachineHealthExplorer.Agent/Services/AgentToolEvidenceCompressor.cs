using MachineHealthExplorer.Agent.Serialization;
using System.Text.Json;

namespace MachineHealthExplorer.Agent.Services;

internal static class AgentToolEvidenceCompressor
{
    public static string BuildToolMessageContent(string toolName, string resultJson, int maxChars)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(toolName);

        var safeMax = Math.Clamp(maxChars, 256, 200_000);
        var normalized = string.IsNullOrWhiteSpace(resultJson) ? "{}" : resultJson.Trim();
        var preview = normalized.Length <= safeMax
            ? normalized
            : string.Concat(normalized.AsSpan(0, safeMax), "…");

        var envelope = new
        {
            schema = "mhe_tool_evidence_v1",
            tool = toolName,
            preview,
            original_length = normalized.Length
        };

        return JsonSerializer.Serialize(envelope, AgentJsonSerializer.Options);
    }
}
