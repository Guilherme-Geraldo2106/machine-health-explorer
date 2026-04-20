using MachineHealthExplorer.Agent.Models;
using System.Text.Json;

namespace MachineHealthExplorer.Agent.MultiAgent;

internal static class SpecialistStructuredOutputParser
{
    public static AgentStructuredSpecialistOutput? TryParse(AgentSpecialistKind kind, string? content)
    {
        if (string.IsNullOrWhiteSpace(content))
        {
            return null;
        }

        try
        {
            var json = ExtractJsonObject(content);
            using var document = JsonDocument.Parse(json);
            var root = document.RootElement;

            var columns = ReadStringArray(root, "relevantColumns");
            var ambiguities = ReadStringArray(root, "ambiguities");
            var observations = ReadStringArray(root, "objectiveObservations");
            var caveats = ReadStringArray(root, "hypothesesOrCaveats");
            var reportSections = ReadStringArray(root, "reportSections");
            var analystNotes = ReadString(root, "analystNotes");

            var evidences = ReadEvidences(root);
            var metrics = ReadMetrics(root);

            return new AgentStructuredSpecialistOutput(
                kind,
                columns,
                ambiguities,
                evidences,
                metrics,
                observations,
                caveats,
                reportSections,
                analystNotes);
        }
        catch
        {
            return null;
        }
    }

    public static AgentStructuredSpecialistOutput FromToolFallback(
        AgentSpecialistKind kind,
        IReadOnlyList<AgentToolExecutionRecord> toolExecutions,
        int digestMaxChars)
    {
        var evidences = new List<AgentEvidence>();
        foreach (var execution in toolExecutions)
        {
            if (execution.IsError)
            {
                evidences.Add(new AgentEvidence(execution.ToolName, $"error:{execution.ResultJson}", null));
                continue;
            }

            var body = execution.ResultJson ?? string.Empty;
            var fragment = body.Length <= digestMaxChars ? body : string.Concat(body.AsSpan(0, digestMaxChars), "…");
            evidences.Add(new AgentEvidence(execution.ToolName, $"{execution.ToolName} output captured.", fragment));
        }

        return new AgentStructuredSpecialistOutput(
            kind,
            RelevantColumns: Array.Empty<string>(),
            Ambiguities: Array.Empty<string>(),
            evidences,
            KeyMetrics: new Dictionary<string, decimal>(StringComparer.OrdinalIgnoreCase),
            ObjectiveObservations: Array.Empty<string>(),
            HypothesesOrCaveats: Array.Empty<string>(),
            ReportSections: Array.Empty<string>(),
            AnalystNotes: "Structured synthesis unavailable; using tool-output fallbacks.");
    }

    private static IReadOnlyList<AgentEvidence> ReadEvidences(JsonElement root)
    {
        if (!root.TryGetProperty("evidences", out var array) || array.ValueKind != JsonValueKind.Array)
        {
            return Array.Empty<AgentEvidence>();
        }

        var list = new List<AgentEvidence>();
        foreach (var element in array.EnumerateArray().Take(24))
        {
            var tool = ReadString(element, "sourceTool");
            if (string.IsNullOrWhiteSpace(tool))
            {
                tool = ReadString(element, "tool");
            }

            var summary = ReadString(element, "summary");
            string? fragment = null;
            if (element.TryGetProperty("supportingJsonFragment", out var frag) && frag.ValueKind is not JsonValueKind.Null)
            {
                fragment = frag.ValueKind == JsonValueKind.String ? frag.GetString() : frag.ToString();
            }

            if (!string.IsNullOrWhiteSpace(tool) || !string.IsNullOrWhiteSpace(summary))
            {
                list.Add(new AgentEvidence(tool, summary, fragment));
            }
        }

        return list;
    }

    private static IReadOnlyDictionary<string, decimal> ReadMetrics(JsonElement root)
    {
        if (!root.TryGetProperty("keyMetrics", out var metrics) || metrics.ValueKind != JsonValueKind.Object)
        {
            return new Dictionary<string, decimal>(StringComparer.OrdinalIgnoreCase);
        }

        var dict = new Dictionary<string, decimal>(StringComparer.OrdinalIgnoreCase);
        foreach (var property in metrics.EnumerateObject())
        {
            if (property.Value.ValueKind == JsonValueKind.Number && property.Value.TryGetDecimal(out var dec))
            {
                dict[property.Name] = dec;
            }
        }

        return dict;
    }

    private static IReadOnlyList<string> ReadStringArray(JsonElement root, string name)
    {
        if (!root.TryGetProperty(name, out var property) || property.ValueKind != JsonValueKind.Array)
        {
            return Array.Empty<string>();
        }

        return property.EnumerateArray()
            .Select(value => value.GetString() ?? string.Empty)
            .Where(value => !string.IsNullOrWhiteSpace(value))
            .Take(48)
            .ToArray();
    }

    private static string ReadString(JsonElement root, string name)
    {
        if (!root.TryGetProperty(name, out var property))
        {
            return string.Empty;
        }

        return property.ValueKind switch
        {
            JsonValueKind.String => property.GetString() ?? string.Empty,
            _ => property.ToString() ?? string.Empty
        };
    }

    private static string ExtractJsonObject(string content)
    {
        var trimmed = content.Trim();
        var start = trimmed.IndexOf('{');
        var end = trimmed.LastIndexOf('}');
        if (start >= 0 && end > start)
        {
            return trimmed[start..(end + 1)];
        }

        return trimmed;
    }
}
