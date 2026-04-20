namespace MachineHealthExplorer.Agent.Services;

/// <summary>
/// Normalizes memory-worker model output so <see cref="AgentEphemeralWorkerRunner"/> can parse JSON
/// even when the model wraps the payload in markdown fences.
/// </summary>
internal static class AgentMemoryWorkerJsonNormalizer
{
    /// <summary>
    /// Strips markdown code fences and returns the substring from the first '{' to the last '}' inclusive.
    /// Returns empty string when no object span is found.
    /// </summary>
    internal static string PrepareMemoryWorkerJson(string? raw)
    {
        if (string.IsNullOrWhiteSpace(raw))
        {
            return string.Empty;
        }

        var text = raw.Trim();
        text = StripLeadingMarkdownFence(text);
        text = StripTrailingMarkdownFence(text.Trim());
        text = text.Trim();
        return ExtractJsonObject(text);
    }

    private static string StripLeadingMarkdownFence(string text)
    {
        if (!text.StartsWith("```", StringComparison.Ordinal))
        {
            return text;
        }

        var newline = text.IndexOf('\n', 3);
        if (newline < 0)
        {
            return text;
        }

        return text.AsSpan(newline + 1).TrimStart().ToString();
    }

    private static string StripTrailingMarkdownFence(string text)
    {
        while (text.EndsWith("```", StringComparison.Ordinal))
        {
            var idx = text.LastIndexOf("```", StringComparison.Ordinal);
            if (idx < 0)
            {
                break;
            }

            text = text[..idx].TrimEnd();
        }

        return text;
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
