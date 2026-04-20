using MachineHealthExplorer.Agent.Models;

namespace MachineHealthExplorer.Agent.MultiAgent;

internal static class SpecialistToolAllowlists
{
    private static readonly string[] Discovery =
    [
        "get_schema",
        "describe_dataset",
        "search_columns",
        "get_distinct_values",
        "profile_columns"
    ];

    private static readonly string[] QueryAnalysis =
    [
        "get_schema",
        "search_columns",
        "get_distinct_values",
        "profile_columns",
        "query_rows",
        "group_and_aggregate"
    ];

    private static readonly string[] FailureAnalysis =
    [
        "get_schema",
        "search_columns",
        "get_distinct_values",
        "profile_columns",
        "query_rows",
        "group_and_aggregate"
    ];

    private static readonly string[] Reporting =
    [
        "get_schema",
        "search_columns",
        "profile_columns",
        "query_rows",
        "group_and_aggregate"
    ];

    public static IReadOnlyList<string> ForSpecialist(AgentSpecialistKind kind)
        => kind switch
        {
            AgentSpecialistKind.Discovery => Discovery,
            AgentSpecialistKind.QueryAnalysis => QueryAnalysis,
            AgentSpecialistKind.FailureAnalysis => FailureAnalysis,
            AgentSpecialistKind.Reporting => Reporting,
            _ => Array.Empty<string>()
        };

    public static bool IsSpecialistTool(AgentSpecialistKind kind, string toolName)
    {
        foreach (var allowed in ForSpecialist(kind))
        {
            if (allowed.Equals(toolName, StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }
        }

        return false;
    }
}
