using System.Collections.Frozen;

namespace MachineHealthExplorer.Agent.Services;

/// <summary>
/// Maps model-invented or legacy tool names to catalog names used by <see cref="DatasetAgentToolRuntime"/>.
/// </summary>
internal static class AgentToolInvocationCanonicalizer
{
    private static readonly FrozenDictionary<string, string> KnownAliases =
        new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["query_schema"] = "get_schema",
            ["describe_schema"] = "get_schema",
            ["schema_query"] = "get_schema",
            ["get_dataset_schema"] = "get_schema",
            ["list_columns"] = "get_schema",
            ["show_schema"] = "get_schema"
        }.ToFrozenDictionary(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// Applies known aliases. Does not validate against the live catalog.
    /// </summary>
    public static string ApplyKnownAliases(string toolName)
    {
        if (string.IsNullOrWhiteSpace(toolName))
        {
            return toolName;
        }

        var trimmed = toolName.Trim();
        return KnownAliases.TryGetValue(trimmed, out var mapped) ? mapped : trimmed;
    }

    /// <summary>
    /// Resolves a raw model tool name to a registered tool name, or <c>null</c> if it does not exist in <paramref name="registeredToolNames"/>.
    /// </summary>
    public static string? TryResolveRegisteredName(string rawName, IReadOnlyCollection<string> registeredToolNames)
    {
        if (string.IsNullOrWhiteSpace(rawName) || registeredToolNames.Count == 0)
        {
            return null;
        }

        var afterAlias = ApplyKnownAliases(rawName.Trim());
        foreach (var registered in registeredToolNames)
        {
            if (registered.Equals(afterAlias, StringComparison.OrdinalIgnoreCase))
            {
                return registered;
            }
        }

        return null;
    }
}
