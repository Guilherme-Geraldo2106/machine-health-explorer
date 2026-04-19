using MachineHealthExplorer.Agent.Models;
using System.Text;

namespace MachineHealthExplorer.Agent.Services;

/// <summary>
/// Recovers tool calls embedded in <c>message.content</c> when the OpenAI-compatible API returns empty <c>tool_calls</c>.
/// </summary>
internal static class AgentPseudoToolCallExtractor
{
    private const string SymmetricMarker = "<|tool_call|>";
    private const string OpenAsymmetricMarker = "<|tool_call>";
    private const string CloseVariantToolCall = "<tool_call|>";
    private const string CloseVariantSlash = "</tool_call>";
    private const string CloseVariantPipedSlash = "<|/tool_call|>";

    private static readonly string[] OrphanToolCallMarkers =
    [
        CloseVariantPipedSlash,
        SymmetricMarker,
        OpenAsymmetricMarker,
        CloseVariantToolCall,
        CloseVariantSlash
    ];

    /// <summary>
    /// Removes pseudo-tool-call delimiter blocks from user-visible text (no catalog required).
    /// </summary>
    public static string RemoveDelimitedPseudoToolCallBlocks(string content)
    {
        if (string.IsNullOrWhiteSpace(content))
        {
            return content;
        }

        var spans = CollectDelimitedPseudoToolCallBlockSpans(content);
        return RemoveSpans(content, spans) ?? string.Empty;
    }

    /// <summary>
    /// Strips known internal tool-call marker fragments that may remain after removing <c>call:...</c> bodies.
    /// </summary>
    public static string RemoveOrphanInternalToolCallMarkers(string content)
    {
        if (string.IsNullOrWhiteSpace(content))
        {
            return content;
        }

        var text = content;
        for (var guard = 0; guard < 64; guard++)
        {
            var before = text;
            foreach (var marker in OrphanToolCallMarkers.OrderByDescending(marker => marker.Length))
            {
                text = text.Replace(marker, string.Empty, StringComparison.OrdinalIgnoreCase);
            }

            if (string.Equals(text, before, StringComparison.Ordinal))
            {
                break;
            }
        }

        return text;
    }

    /// <summary>
    /// Removes <c>call: name{...}</c> directives (balanced JSON object) from visible text.
    /// </summary>
    public static string RemoveBareCallDirectives(string content)
    {
        if (string.IsNullOrWhiteSpace(content))
        {
            return content;
        }

        var spans = new List<(int Start, int End)>();
        const string prefix = "call:";
        var searchFrom = 0;
        while (searchFrom < content.Length)
        {
            var callIdx = content.IndexOf(prefix, searchFrom, StringComparison.OrdinalIgnoreCase);
            if (callIdx < 0)
            {
                break;
            }

            if (callIdx > 0 && IsIdentChar(content[callIdx - 1]))
            {
                searchFrom = callIdx + prefix.Length;
                continue;
            }

            var afterPrefix = callIdx + prefix.Length;
            while (afterPrefix < content.Length && char.IsWhiteSpace(content[afterPrefix]))
            {
                afterPrefix++;
            }

            var nameEnd = afterPrefix;
            while (nameEnd < content.Length && (char.IsLetterOrDigit(content[nameEnd]) || content[nameEnd] == '_'))
            {
                nameEnd++;
            }

            if (nameEnd == afterPrefix)
            {
                searchFrom = afterPrefix + 1;
                continue;
            }

            var braceScan = nameEnd;
            while (braceScan < content.Length && char.IsWhiteSpace(content[braceScan]))
            {
                braceScan++;
            }

            if (braceScan >= content.Length || content[braceScan] != '{')
            {
                searchFrom = nameEnd;
                continue;
            }

            if (!TryReadBalancedJsonObject(content, braceScan, out var argsEndExclusive, out _))
            {
                searchFrom = braceScan + 1;
                continue;
            }

            if (!OverlapsExisting(spans, callIdx, argsEndExclusive))
            {
                spans.Add((callIdx, argsEndExclusive));
            }

            searchFrom = argsEndExclusive;
        }

        return RemoveSpans(content, spans) ?? string.Empty;
    }

    /// <summary>
    /// Extracts pseudo tool calls, removes their spans from content, and returns canonical registered names.
    /// </summary>
    public static (IReadOnlyList<AgentToolCall> Calls, string? StrippedContent) Extract(
        string? content,
        IReadOnlyList<string> registeredToolNames)
    {
        if (string.IsNullOrWhiteSpace(content))
        {
            return (Array.Empty<AgentToolCall>(), content);
        }

        if (registeredToolNames.Count == 0)
        {
            return (Array.Empty<AgentToolCall>(), content);
        }

        var spans = new List<(int Start, int End)>();
        var calls = new List<AgentToolCall>();

        ExtractFromDelimitedBlocks(content, registeredToolNames, spans, calls);
        ExtractBareCallSyntax(content, registeredToolNames, spans, calls);

        var stripped = RemoveSpans(content, spans);
        return (calls, stripped);
    }

    private static List<(int Start, int End)> CollectDelimitedPseudoToolCallBlockSpans(string content)
        => EnumerateDelimitedPseudoToolCallBlocks(content).Select(block => (block.Open, block.EndExclusive)).ToList();

    private static List<(int Open, int OpenLen, int CloseStart, int EndExclusive)> EnumerateDelimitedPseudoToolCallBlocks(string content)
    {
        var blocks = new List<(int Open, int OpenLen, int CloseStart, int EndExclusive)>();
        var index = 0;
        while (TryFindNextDelimiterOpen(content, index, out var open, out var openLen))
        {
            var innerStart = open + openLen;
            var close = FindClosingDelimiter(content, innerStart);
            if (close < 0)
            {
                break;
            }

            var closeLen = GetDelimiterLength(content, close);
            if (closeLen == 0)
            {
                index = innerStart;
                continue;
            }

            blocks.Add((open, openLen, close, close + closeLen));
            index = close + closeLen;
        }

        return blocks;
    }

    private static bool TryFindNextDelimiterOpen(string content, int from, out int openIndex, out int openLength)
    {
        openIndex = -1;
        openLength = 0;

        var symmetric = content.IndexOf(SymmetricMarker, from, StringComparison.Ordinal);
        var asymmetric = content.IndexOf(OpenAsymmetricMarker, from, StringComparison.Ordinal);

        var candidates = new List<(int idx, int len)>();
        if (symmetric >= 0)
        {
            candidates.Add((symmetric, SymmetricMarker.Length));
        }

        if (asymmetric >= 0)
        {
            candidates.Add((asymmetric, OpenAsymmetricMarker.Length));
        }

        if (candidates.Count == 0)
        {
            return false;
        }

        var minIdx = candidates.Min(c => c.idx);
        var best = candidates.Where(c => c.idx == minIdx).OrderByDescending(c => c.len).First();

        openIndex = best.idx;
        openLength = best.len;
        return true;
    }

    private static void ExtractFromDelimitedBlocks(
        string text,
        IReadOnlyList<string> registeredToolNames,
        List<(int Start, int End)> spans,
        List<AgentToolCall> calls)
    {
        foreach (var (open, openLen, closeStart, endExclusive) in EnumerateDelimitedPseudoToolCallBlocks(text))
        {
            if (OverlapsExisting(spans, open, endExclusive))
            {
                continue;
            }

            var inner = text.AsSpan(open + openLen, closeStart - (open + openLen));
            if (TryParseCallDirective(inner, registeredToolNames, out var name, out var args))
            {
                calls.Add(CreateCall(name, args));
            }

            spans.Add((open, endExclusive));
        }
    }

    private static int FindClosingDelimiter(string text, int innerStart)
    {
        var symmetricSecond = text.IndexOf(SymmetricMarker, innerStart, StringComparison.Ordinal);
        var a = text.IndexOf(CloseVariantToolCall, innerStart, StringComparison.Ordinal);
        var b = text.IndexOf(CloseVariantSlash, innerStart, StringComparison.OrdinalIgnoreCase);
        var c = text.IndexOf(CloseVariantPipedSlash, innerStart, StringComparison.OrdinalIgnoreCase);

        var candidates = new[] { symmetricSecond, a, b, c }.Where(i => i >= 0).ToArray();
        return candidates.Length == 0 ? -1 : candidates.Min();
    }

    private static int GetDelimiterLength(string text, int closeIndex)
    {
        if (closeIndex + SymmetricMarker.Length <= text.Length
            && text.AsSpan(closeIndex, SymmetricMarker.Length).SequenceEqual(SymmetricMarker.AsSpan()))
        {
            return SymmetricMarker.Length;
        }

        if (closeIndex + CloseVariantToolCall.Length <= text.Length
            && text.AsSpan(closeIndex, CloseVariantToolCall.Length).SequenceEqual(CloseVariantToolCall.AsSpan()))
        {
            return CloseVariantToolCall.Length;
        }

        if (closeIndex + CloseVariantSlash.Length <= text.Length
            && string.Equals(text.Substring(closeIndex, CloseVariantSlash.Length), CloseVariantSlash, StringComparison.OrdinalIgnoreCase))
        {
            return CloseVariantSlash.Length;
        }

        if (closeIndex + CloseVariantPipedSlash.Length <= text.Length
            && text.AsSpan(closeIndex, CloseVariantPipedSlash.Length).SequenceEqual(CloseVariantPipedSlash.AsSpan()))
        {
            return CloseVariantPipedSlash.Length;
        }

        return 0;
    }

    private static void ExtractBareCallSyntax(
        string text,
        IReadOnlyList<string> registeredToolNames,
        List<(int Start, int End)> spans,
        List<AgentToolCall> calls)
    {
        const string prefix = "call:";
        var searchFrom = 0;
        while (searchFrom < text.Length)
        {
            var callIdx = text.IndexOf(prefix, searchFrom, StringComparison.OrdinalIgnoreCase);
            if (callIdx < 0)
            {
                break;
            }

            if (callIdx > 0 && IsIdentChar(text[callIdx - 1]))
            {
                searchFrom = callIdx + prefix.Length;
                continue;
            }

            var afterPrefix = callIdx + prefix.Length;
            while (afterPrefix < text.Length && char.IsWhiteSpace(text[afterPrefix]))
            {
                afterPrefix++;
            }

            var nameStart = afterPrefix;
            var nameEnd = nameStart;
            while (nameEnd < text.Length && (char.IsLetterOrDigit(text[nameEnd]) || text[nameEnd] == '_'))
            {
                nameEnd++;
            }

            if (nameEnd == nameStart)
            {
                searchFrom = afterPrefix + 1;
                continue;
            }

            var rawName = text[nameStart..nameEnd];
            var braceScan = nameEnd;
            while (braceScan < text.Length && char.IsWhiteSpace(text[braceScan]))
            {
                braceScan++;
            }

            if (braceScan >= text.Length || text[braceScan] != '{')
            {
                searchFrom = nameEnd;
                continue;
            }

            if (!TryReadBalancedJsonObject(text, braceScan, out var argsEndExclusive, out var argsJson))
            {
                searchFrom = braceScan + 1;
                continue;
            }

            var resolved = AgentToolInvocationCanonicalizer.TryResolveRegisteredName(rawName, registeredToolNames);
            if (resolved is not null && !OverlapsExisting(spans, callIdx, argsEndExclusive))
            {
                spans.Add((callIdx, argsEndExclusive));
                calls.Add(CreateCall(resolved, argsJson));
            }

            searchFrom = argsEndExclusive;
        }
    }

    private static bool OverlapsExisting(List<(int Start, int End)> spans, int start, int end)
    {
        foreach (var (s, e) in spans)
        {
            if (start < e && end > s)
            {
                return true;
            }
        }

        return false;
    }

    private static bool TryParseCallDirective(
        ReadOnlySpan<char> inner,
        IReadOnlyList<string> registeredToolNames,
        out string resolvedName,
        out string argumentsJson)
    {
        resolvedName = string.Empty;
        argumentsJson = "{}";

        var s = inner.Trim().ToString();
        if (s.StartsWith("call:", StringComparison.OrdinalIgnoreCase))
        {
            s = s["call:".Length..].TrimStart();
        }

        var nameEnd = 0;
        while (nameEnd < s.Length && (char.IsLetterOrDigit(s[nameEnd]) || s[nameEnd] == '_'))
        {
            nameEnd++;
        }

        if (nameEnd == 0)
        {
            return false;
        }

        var rawName = s[..nameEnd];
        var braceAt = nameEnd;
        while (braceAt < s.Length && char.IsWhiteSpace(s[braceAt]))
        {
            braceAt++;
        }

        if (braceAt >= s.Length || s[braceAt] != '{')
        {
            return false;
        }

        if (!TryReadBalancedJsonObject(s, braceAt, out _, out argumentsJson))
        {
            return false;
        }

        var resolved = AgentToolInvocationCanonicalizer.TryResolveRegisteredName(rawName, registeredToolNames);
        if (resolved is null)
        {
            return false;
        }

        resolvedName = resolved;
        return true;
    }

    private static bool TryReadBalancedJsonObject(string text, int openBraceIndex, out int endExclusive, out string json)
    {
        json = "{}";
        endExclusive = openBraceIndex;

        if (openBraceIndex < 0 || openBraceIndex >= text.Length || text[openBraceIndex] != '{')
        {
            return false;
        }

        var depth = 0;
        var inString = false;
        var escape = false;

        for (var i = openBraceIndex; i < text.Length; i++)
        {
            var c = text[i];

            if (inString)
            {
                if (escape)
                {
                    escape = false;
                }
                else if (c == '\\')
                {
                    escape = true;
                }
                else if (c == '"')
                {
                    inString = false;
                }

                continue;
            }

            if (c == '"')
            {
                inString = true;
                continue;
            }

            if (c == '{')
            {
                depth++;
            }
            else if (c == '}')
            {
                depth--;
                if (depth == 0)
                {
                    endExclusive = i + 1;
                    json = text[openBraceIndex..endExclusive];
                    return true;
                }
            }
        }

        return false;
    }

    private static AgentToolCall CreateCall(string name, string argumentsJson)
        => new()
        {
            Id = $"pseudo_{Guid.NewGuid():N}",
            Name = name,
            ArgumentsJson = string.IsNullOrWhiteSpace(argumentsJson) ? "{}" : argumentsJson.Trim()
        };

    private static string? RemoveSpans(string text, List<(int Start, int End)> spans)
    {
        if (spans.Count == 0)
        {
            return text;
        }

        var sb = new StringBuilder(text.Length);
        var cursor = 0;
        foreach (var (start, end) in spans.OrderBy(s => s.Start))
        {
            if (start > cursor)
            {
                sb.Append(text.AsSpan(cursor, start - cursor));
            }

            cursor = Math.Max(cursor, end);
        }

        if (cursor < text.Length)
        {
            sb.Append(text.AsSpan(cursor));
        }

        var result = sb.ToString().Trim();
        return string.IsNullOrEmpty(result) ? null : result;
    }

    private static bool IsIdentChar(char c) => char.IsLetterOrDigit(c) || c == '_';
}
