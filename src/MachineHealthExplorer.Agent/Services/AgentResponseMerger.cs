namespace MachineHealthExplorer.Agent.Services;

internal static class AgentResponseMerger
{
    public static string MergeAssistantFragments(string accumulated, string nextFragment)
    {
        if (string.IsNullOrEmpty(nextFragment))
        {
            return accumulated;
        }

        if (string.IsNullOrEmpty(accumulated))
        {
            return nextFragment.TrimStart();
        }

        var a = accumulated;
        var b = nextFragment.TrimStart();

        if (b.Length == 0)
        {
            return a;
        }

        if (a.Contains(b, StringComparison.Ordinal))
        {
            return a;
        }

        if (b.StartsWith(a, StringComparison.Ordinal))
        {
            return b;
        }

        var maxOverlap = Math.Min(a.Length, b.Length);
        for (var overlap = maxOverlap; overlap >= 1; overlap--)
        {
            if (overlap > a.Length || overlap > b.Length)
            {
                continue;
            }

            var aSuffix = a[^overlap..];
            if (b.StartsWith(aSuffix, StringComparison.Ordinal))
            {
                return string.Concat(a.AsSpan(), b.AsSpan(aSuffix.Length));
            }
        }

        var aTrimmedEnd = a.TrimEnd();
        var bTrimmedStart = b.TrimStart();
        if (aTrimmedEnd.EndsWith(bTrimmedStart[..Math.Min(bTrimmedStart.Length, 64)], StringComparison.Ordinal))
        {
            return a;
        }

        var needsSpace = !char.IsWhiteSpace(a[^1]) && !char.IsWhiteSpace(b[0]);
        return needsSpace ? $"{a} {b}" : $"{a}{b}";
    }
}
