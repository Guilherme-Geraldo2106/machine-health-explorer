using MachineHealthExplorer.Agent.Models;
using System.Text.Json;
using System.Text.RegularExpressions;

namespace MachineHealthExplorer.Agent.Services;

internal static class AgentVisibleResponseNormalizer
{
    private static readonly Regex FencedJsonBlock = new(
        @"^\s*```(?:json)?\s*(?:\r?\n)?(?<body>[\s\S]*?)\s*```\s*$",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Compiled);

    public static bool NeedsVisibleRecovery(AgentModelResponse response)
    {
        if (response.ToolCalls.Count > 0)
        {
            return false;
        }

        if (IsUserVisibleAssistantText(response.Content))
        {
            return false;
        }

        if (!string.IsNullOrWhiteSpace(response.ReasoningContent))
        {
            return true;
        }

        if (AgentFinishReason.IsTruncated(response.FinishReason))
        {
            return true;
        }

        return HasInvisibleAssistantArtifacts(response.Content);
    }

    /// <summary>
    /// True when the model emitted non-empty surface text that is not user-visible after stripping internal artifacts.
    /// </summary>
    public static bool HasInvisibleAssistantArtifacts(string? content)
    {
        if (string.IsNullOrWhiteSpace(content))
        {
            return false;
        }

        return StripInternalAssistantSurface(content) is null;
    }

    /// <summary>
    /// Removes internal pseudo-tool-call markers and worker-only fenced JSON from assistant text persisted or shown to the user.
    /// </summary>
    public static string? StripInternalAssistantSurface(string? content)
    {
        if (content is null)
        {
            return null;
        }

        if (string.IsNullOrWhiteSpace(content))
        {
            return null;
        }

        var text = AgentPseudoToolCallExtractor.RemoveDelimitedPseudoToolCallBlocks(content);
        text = AgentPseudoToolCallExtractor.RemoveBareCallDirectives(text).Trim();
        text = AgentPseudoToolCallExtractor.RemoveOrphanInternalToolCallMarkers(text).Trim();
        if (string.IsNullOrWhiteSpace(text))
        {
            return null;
        }

        if (LooksLikeWorkerMemoryJsonEnvelope(text))
        {
            return null;
        }

        var unfenced = TryUnfenceJsonOnly(text);
        if (unfenced is not null && LooksLikeWorkerMemoryJsonEnvelope(unfenced))
        {
            return null;
        }

        return string.IsNullOrWhiteSpace(text) ? null : text.Trim();
    }

    public static bool IsUserVisibleAssistantText(string? content)
    {
        var stripped = StripInternalAssistantSurface(content);
        return !string.IsNullOrWhiteSpace(stripped);
    }

    private static string? TryUnfenceJsonOnly(string text)
    {
        var match = FencedJsonBlock.Match(text);
        return match.Success ? match.Groups["body"].Value.Trim() : null;
    }

    private static bool LooksLikeWorkerMemoryJsonEnvelope(string text)
    {
        var trimmed = text.Trim();
        if (trimmed.Length == 0 || trimmed[0] != '{')
        {
            return false;
        }

        try
        {
            using var document = JsonDocument.Parse(trimmed);
            var root = document.RootElement;
            if (root.ValueKind != JsonValueKind.Object)
            {
                return false;
            }

            return PropertyExists(root, "currentUserIntent")
                   || PropertyExists(root, "CurrentUserIntent")
                   || PropertyExists(root, "pendingQuestions")
                   || PropertyExists(root, "PendingQuestions")
                   || PropertyExists(root, "toolHighlights")
                   || PropertyExists(root, "ToolEvidenceDigests");
        }
        catch
        {
            return false;
        }
    }

    private static bool PropertyExists(JsonElement root, string name)
        => root.TryGetProperty(name, out _) ||
           root.EnumerateObject().Any(p => string.Equals(p.Name, name, StringComparison.OrdinalIgnoreCase));
}
