using MachineHealthExplorer.Agent.Models;
using System.Text.Json;

namespace MachineHealthExplorer.Agent.MultiAgent;

internal static class SchemaColumnNamesFromToolExecutions
{
    public static IReadOnlyList<string> Extract(IEnumerable<AgentToolExecutionRecord> executions)
    {
        var set = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var execution in executions)
        {
            if (execution.IsError
                || !execution.ToolName.Equals("get_schema", StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            TryAddColumnsFromSchemaJson(execution.ResultJson, set);
        }

        return set.OrderBy(name => name, StringComparer.OrdinalIgnoreCase).ToArray();
    }

    private static void TryAddColumnsFromSchemaJson(string? json, HashSet<string> sink)
    {
        if (string.IsNullOrWhiteSpace(json))
        {
            return;
        }

        try
        {
            using var document = JsonDocument.Parse(json);
            var root = document.RootElement;
            if (!root.TryGetProperty("columns", out var columns) || columns.ValueKind != JsonValueKind.Array)
            {
                return;
            }

            foreach (var column in columns.EnumerateArray())
            {
                if (column.ValueKind != JsonValueKind.Object)
                {
                    continue;
                }

                if (column.TryGetProperty("name", out var nameEl) && nameEl.ValueKind == JsonValueKind.String)
                {
                    var name = nameEl.GetString();
                    if (!string.IsNullOrWhiteSpace(name))
                    {
                        sink.Add(name.Trim());
                    }
                }
            }
        }
        catch (JsonException)
        {
            // ignore malformed tool payloads
        }
    }
}
