using MachineHealthExplorer.Agent.Models;
using MachineHealthExplorer.Agent.Serialization;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace MachineHealthExplorer.Agent.Services;

internal static class AgentToolEvidenceCompressor
{
    private static readonly HashSet<string> StructuralTabularTools = new(StringComparer.OrdinalIgnoreCase)
    {
        "group_and_aggregate",
        "query_rows"
    };

    private static readonly HashSet<string> StructuralSchemaTools = new(StringComparer.OrdinalIgnoreCase)
    {
        "get_schema"
    };

    public static int ComputeMaxToolEvidenceChars(AgentOptions options, int estimatedPromptTokens)
    {
        var remaining = AgentContextBudgetEstimator.EstimateRemainingContextTokens(
            options,
            estimatedPromptTokens,
            reasoningPressureSteps: 0,
            lastUsage: null);
        var divisor = Math.Max(2, options.ContextBudgetCharsPerToken);
        if (remaining <= 0)
        {
            return Math.Clamp(remaining * divisor / 4, 256, options.MaxToolEvidenceContentChars);
        }

        if (remaining <= 256)
        {
            return (int)Math.Clamp(remaining * divisor / 2, 256, options.MaxToolEvidenceContentChars);
        }

        if (remaining <= 1024)
        {
            return (int)Math.Clamp(remaining * divisor, 768, options.MaxToolEvidenceContentChars);
        }

        return options.MaxToolEvidenceContentChars;
    }

    public static string BuildToolMessageContent(string toolName, string resultJson, int maxChars)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(toolName);

        var safeMax = Math.Clamp(maxChars, 256, 200_000);
        var normalized = string.IsNullOrWhiteSpace(resultJson) ? "{}" : resultJson.Trim();

        if (StructuralSchemaTools.Contains(toolName))
        {
            normalized = TryNormalizeGetSchemaPayloadJson(normalized) ?? normalized;
        }

        string body;
        if (normalized.Length <= safeMax)
        {
            body = normalized;
        }
        else
        {
            var structural = TryStructuralTabularCompact(toolName, normalized, safeMax)
                ?? TryStructuralSchemaCompact(toolName, normalized, safeMax);
            body = structural ?? string.Concat(normalized.AsSpan(0, safeMax), "…");
            if (body.Length > safeMax)
            {
                body = string.Concat(body.AsSpan(0, safeMax), "…");
            }
        }

        var envelope = new
        {
            schema = "mhe_tool_evidence_v1",
            tool = toolName,
            preview = body,
            original_length = normalized.Length
        };

        return JsonSerializer.Serialize(envelope, AgentJsonSerializer.Options);
    }

    public static bool CompactToolMessagesInConversation(List<AgentConversationMessage> messages, int newPreviewMaxChars)
    {
        var safeMax = Math.Clamp(newPreviewMaxChars, 256, 200_000);
        var changed = false;
        for (var index = 0; index < messages.Count; index++)
        {
            var message = messages[index];
            if (message.Role != AgentConversationRole.Tool || string.IsNullOrWhiteSpace(message.Content))
            {
                continue;
            }

            var updated = ShrinkSingleToolMessageContent(message.Content, safeMax);
            if (updated != message.Content)
            {
                messages[index] = message with { Content = updated };
                changed = true;
            }
        }

        return changed;
    }

    private static string ShrinkSingleToolMessageContent(string content, int safeMax)
    {
        try
        {
            var node = JsonNode.Parse(content);
            if (node is not JsonObject obj)
            {
                return content;
            }

            if (!string.Equals(obj["schema"]?.GetValue<string>(), "mhe_tool_evidence_v1", StringComparison.Ordinal))
            {
                return content;
            }

            var preview = obj["preview"]?.GetValue<string>() ?? string.Empty;
            if (preview.Length <= safeMax)
            {
                return content;
            }

            var toolName = obj["tool"]?.GetValue<string>() ?? string.Empty;
            var slimPreview = StructuralSchemaTools.Contains(toolName)
                ? TryNormalizeGetSchemaPayloadJson(preview) ?? preview
                : preview;
            var nextPreview = TryStructuralTabularCompact(toolName, slimPreview, safeMax)
                ?? TryStructuralSchemaCompact(toolName, slimPreview, safeMax)
                ?? string.Concat(slimPreview.AsSpan(0, safeMax), "…");
            if (nextPreview.Length > safeMax)
            {
                nextPreview = string.Concat(nextPreview.AsSpan(0, safeMax), "…");
            }

            obj["preview"] = nextPreview;
            obj["original_length"] = preview.Length;
            return obj.ToJsonString(AgentJsonSerializer.Options);
        }
        catch (JsonException)
        {
            return content.Length > safeMax + 128
                ? string.Concat(content.AsSpan(0, safeMax), "…")
                : content;
        }
    }

    private static string? TryNormalizeGetSchemaPayloadJson(string json)
    {
        try
        {
            var node = JsonNode.Parse(json);
            if (node is not JsonObject root)
            {
                return null;
            }

            var columnsNode = FindPropertyIgnoreCase(root, "columns")?.AsArray();
            if (columnsNode is null || columnsNode.Count == 0)
            {
                return null;
            }

            var compactColumns = new JsonArray();
            foreach (var column in columnsNode)
            {
                if (column is not JsonObject colObj)
                {
                    continue;
                }

                var name = FindPropertyIgnoreCase(colObj, "name")?.GetValue<string>() ?? string.Empty;
                if (string.IsNullOrWhiteSpace(name))
                {
                    continue;
                }

                var dataType = FindPropertyIgnoreCase(colObj, "dataType")?.GetValue<string>() ?? string.Empty;
                var isNumeric = FindPropertyIgnoreCase(colObj, "isNumeric")?.GetValue<bool>() ?? false;
                var isCategorical = FindPropertyIgnoreCase(colObj, "isCategorical")?.GetValue<bool>() ?? false;
                compactColumns.Add(new JsonObject
                {
                    ["name"] = name,
                    ["dataType"] = dataType,
                    ["isNumeric"] = isNumeric,
                    ["isCategorical"] = isCategorical
                });
            }

            if (compactColumns.Count == 0)
            {
                return null;
            }

            var output = new JsonObject
            {
                ["datasetName"] = FindPropertyIgnoreCase(root, "datasetName")?.GetValue<string>() ?? string.Empty,
                ["rowCount"] = FindPropertyIgnoreCase(root, "rowCount")?.GetValue<int>() ?? 0,
                ["columns"] = compactColumns
            };

            return output.ToJsonString(AgentJsonSerializer.Options);
        }
        catch (JsonException)
        {
            return null;
        }
    }

    private static string? TryStructuralSchemaCompact(string toolName, string json, int maxChars)
    {
        if (!StructuralSchemaTools.Contains(toolName))
        {
            return null;
        }

        var working = TryNormalizeGetSchemaPayloadJson(json) ?? json;
        if (working.Length <= maxChars)
        {
            return working;
        }

        try
        {
            for (var pass = 0; pass < 8; pass++)
            {
                var node = JsonNode.Parse(working);
                if (node is not JsonObject root)
                {
                    return null;
                }

                var columns = FindPropertyIgnoreCase(root, "columns")?.AsArray();
                if (columns is null || columns.Count == 0)
                {
                    return null;
                }

                foreach (var column in columns)
                {
                    if (column is not JsonObject colObj)
                    {
                        continue;
                    }

                    if (pass >= 1)
                    {
                        colObj.Remove("dataType");
                    }

                    if (pass >= 2)
                    {
                        colObj.Remove("isNumeric");
                    }

                    if (pass >= 3)
                    {
                        colObj.Remove("isCategorical");
                    }
                }

                if (pass >= 4)
                {
                    root.Remove("rowCount");
                }

                if (pass >= 5)
                {
                    root.Remove("datasetName");
                }

                working = root.ToJsonString(AgentJsonSerializer.Options);
                if (working.Length <= maxChars)
                {
                    return working;
                }
            }

            return null;
        }
        catch (JsonException)
        {
            return null;
        }
    }

    private static JsonNode? FindPropertyIgnoreCase(JsonObject obj, string propertyName)
    {
        foreach (var pair in obj)
        {
            if (string.Equals(pair.Key, propertyName, StringComparison.OrdinalIgnoreCase))
            {
                return pair.Value;
            }
        }

        return null;
    }

    /// <summary>
    /// For large tabular tool JSON, preserve head/tail rows and counts instead of truncating raw JSON at an arbitrary byte offset.
    /// </summary>
    private static string? TryStructuralTabularCompact(string toolName, string json, int maxChars)
    {
        if (!StructuralTabularTools.Contains(toolName))
        {
            return null;
        }

        JsonObject? root;
        try
        {
            root = JsonNode.Parse(json) as JsonObject;
        }
        catch (JsonException)
        {
            return null;
        }

        if (root is null)
        {
            return null;
        }

        if (root["error"] is not null)
        {
            return null;
        }

        if (root["rows"] is not JsonArray rows || rows.Count == 0)
        {
            return null;
        }

        var totalRows = rows.Count;
        if (json.Length <= maxChars)
        {
            return null;
        }

        if (totalRows <= 64)
        {
            var full = new JsonObject();
            foreach (var pair in root)
            {
                full[pair.Key] = pair.Key == "rows" ? rows.DeepClone() : pair.Value?.DeepClone();
            }

            var fullText = full.ToJsonString(AgentJsonSerializer.Options);
            if (fullText.Length <= maxChars)
            {
                return fullText;
            }
        }

        var head = 18;
        var tail = 12;
        while (head >= 1)
        {
            var compact = new JsonObject();
            foreach (var pair in root)
            {
                if (pair.Key == "rows")
                {
                    continue;
                }

                compact[pair.Key] = pair.Value?.DeepClone();
            }

            var first = new JsonArray();
            var headCount = Math.Min(head, totalRows);
            for (var i = 0; i < headCount; i++)
            {
                first.Add(rows[i]?.DeepClone());
            }

            var last = new JsonArray();
            var tailCount = Math.Min(tail, Math.Max(0, totalRows - headCount));
            for (var i = totalRows - tailCount; i < totalRows; i++)
            {
                last.Add(rows[i]?.DeepClone());
            }

            compact["rowsFirst"] = first;
            compact["rowsLast"] = last;
            var omitted = Math.Max(0, totalRows - first.Count - last.Count);
            compact["omittedMiddleRowCount"] = omitted;

            var text = compact.ToJsonString(AgentJsonSerializer.Options);
            if (text.Length <= maxChars || head <= 1)
            {
                return text;
            }

            head = Math.Max(1, head - 4);
            tail = Math.Max(1, tail - 3);
        }

        return null;
    }
}
