namespace MachineHealthExplorer.Agent.Models;

public sealed record AgentToolEvidenceDigest
{
    public string ToolName { get; init; } = string.Empty;
    public string Digest { get; init; } = string.Empty;
}

public sealed record AgentConversationMemory
{
    public string CurrentUserIntent { get; init; } = string.Empty;
    public IReadOnlyList<string> PendingQuestions { get; init; } = Array.Empty<string>();
    public IReadOnlyList<string> ConfirmedFacts { get; init; } = Array.Empty<string>();
    public IReadOnlyList<AgentToolEvidenceDigest> ToolEvidenceDigests { get; init; } = Array.Empty<AgentToolEvidenceDigest>();
    public string LanguagePreference { get; init; } = string.Empty;
    public string RollingSummary { get; init; } = string.Empty;
    public DateTimeOffset LastUpdatedUtc { get; init; } = DateTimeOffset.UtcNow;

    public AgentConversationMemory WithRollingSummary(string rollingSummary, int maxLength)
    {
        var trimmed = TrimToMax(rollingSummary, maxLength);
        return this with
        {
            RollingSummary = trimmed,
            LastUpdatedUtc = DateTimeOffset.UtcNow
        };
    }

    public AgentConversationMemory WithMergedFacts(IEnumerable<string> facts, int maxLength)
    {
        var merged = ConfirmedFacts.Concat(facts).Where(f => !string.IsNullOrWhiteSpace(f)).Distinct(StringComparer.OrdinalIgnoreCase).ToArray();
        var rolling = RollingSummary;
        if (merged.Length > 0)
        {
            var factsBlock = string.Join(" | ", merged.TakeLast(12));
            rolling = TrimToMax(string.IsNullOrWhiteSpace(rolling) ? factsBlock : $"{rolling}\n{factsBlock}", maxLength);
        }

        return this with
        {
            ConfirmedFacts = merged.TakeLast(24).ToArray(),
            RollingSummary = rolling,
            LastUpdatedUtc = DateTimeOffset.UtcNow
        };
    }

    /// <summary>
    /// Appends tool evidence digests without mutating <see cref="RollingSummary"/> (rolling text comes from compaction / memory worker only).
    /// Dedupes by tool name + digest prefix and keeps the most recent <paramref name="maxStoredDigests"/> entries.
    /// </summary>
    public AgentConversationMemory WithToolEvidence(
        IEnumerable<AgentToolEvidenceDigest> digests,
        int digestMaxChars,
        int maxStoredDigests)
    {
        static string TrimDigest(string text, int max)
        {
            if (max <= 0 || string.IsNullOrEmpty(text))
            {
                return string.Empty;
            }

            return text.Length <= max ? text : string.Concat(text.AsSpan(0, max), "…");
        }

        static string DedupeKey(AgentToolEvidenceDigest d)
        {
            var tool = (d.ToolName ?? string.Empty).Trim();
            var body = (d.Digest ?? string.Empty).Trim();
            var prefix = body.Length <= 96 ? body : body[..96];
            return $"{tool}\u001f{prefix}";
        }

        var merged = new List<AgentToolEvidenceDigest>(ToolEvidenceDigests);
        foreach (var digest in digests)
        {
            if (string.IsNullOrWhiteSpace(digest.ToolName) && string.IsNullOrWhiteSpace(digest.Digest))
            {
                continue;
            }

            merged.Add(new AgentToolEvidenceDigest
            {
                ToolName = digest.ToolName.Trim(),
                Digest = TrimDigest(digest.Digest ?? string.Empty, digestMaxChars)
            });
        }

        var dedupedLastWins = new Dictionary<string, AgentToolEvidenceDigest>(StringComparer.Ordinal);
        foreach (var entry in merged)
        {
            dedupedLastWins[DedupeKey(entry)] = entry;
        }

        var kept = dedupedLastWins.Values.ToList();
        var max = Math.Max(1, maxStoredDigests);
        if (kept.Count > max)
        {
            kept = kept.TakeLast(max).ToList();
        }

        return this with
        {
            ToolEvidenceDigests = kept,
            LastUpdatedUtc = DateTimeOffset.UtcNow
        };
    }

    private static string TrimToMax(string value, int maxLength)
    {
        if (maxLength <= 0 || string.IsNullOrEmpty(value))
        {
            return string.Empty;
        }

        if (value.Length <= maxLength)
        {
            return value;
        }

        return value[^maxLength..];
    }
}
