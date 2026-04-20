using MachineHealthExplorer.Agent.Models;

namespace MachineHealthExplorer.Agent.MultiAgent;

internal static class MultiAgentPromptBuilder
{
    private const string GroupAndAggregateParameterContract = """
group_and_aggregate (exact field names):
- "groupByBins" MUST be a JSON array of objects; each object requires "columnName", "alias", and "binWidth" (number > 0).
- "aggregations" is an array; each item requires "alias" and "function" (e.g. Count). Optional per-aggregation "filter" object for conditional counts.
- "sortRules" is an array of { "columnName": "...", "direction": "Ascending"|"Descending" } — columnName may reference a bin alias.
""";

    public static string BuildMinimalToolParametersContractHint()
    {
        var example = """
{"keyword":"temperature"}
{"groupByBins":[{"columnName":"<numeric column>","alias":"temp_bin","binWidth":1}],"aggregations":[{"alias":"row_count","function":"Count"},{"alias":"failure_count","function":"Count","filter":{"columnName":"<boolean column>","operator":"Equals","value":true}}],"sortRules":[{"columnName":"temp_bin","direction":"Ascending"}],"pageSize":200}
""";
        return $"""
Tool JSON schemas were reduced for context limits. Use these exact field names and shapes:

- search_columns: single string field "keyword" (not "keywords").
{GroupAndAggregateParameterContract.Trim()}
Example (illustrative column names only):
{example.Trim()}
""";
    }

    /// <summary>
    /// Compact, schema-agnostic reminder for model-driven recovery when full tool JSON is not present.
    /// </summary>
    public static string BuildGroupAndAggregateCompactContractHint()
        => GroupAndAggregateParameterContract.Trim();

    public static string BuildSpecialistSystemPrompt(AgentSpecialistKind kind, AgentOptions options)
    {
        var multi = options.MultiAgent;
        var extension = kind switch
        {
            AgentSpecialistKind.Discovery => multi.Discovery.SystemPromptExtension,
            AgentSpecialistKind.QueryAnalysis => multi.QueryAnalysis.SystemPromptExtension,
            AgentSpecialistKind.FailureAnalysis => multi.FailureAnalysis.SystemPromptExtension,
            AgentSpecialistKind.Reporting => multi.Reporting.SystemPromptExtension,
            _ => string.Empty
        };

        var extensionBlock = string.IsNullOrWhiteSpace(extension) ? string.Empty : $"\n{extension.Trim()}";

        return kind switch
        {
            AgentSpecialistKind.Discovery => $"""
You are the Discovery specialist for Machine Health Explorer.
Your job is structural dataset discovery only (schema, columns, types, domains, profiling).
Rules:
- Use tools to retrieve facts; never invent dataset values.
- Do not write the final user-facing answer.
- Prefer short, high-signal tool arguments.
- If the user is ambiguous about similarly named columns, record ambiguities for downstream agents.
- get_schema returns lean column metadata only; use profile_columns when you need distributions, summaries, or sample values.
- In structured synthesis, list exact column names in relevantColumns and keep each supportingJsonFragment short (under ~500 characters); do not paste full schema or profile JSON.
- On tool-enabled turns, emit the next tool_calls entry directly; avoid long hidden reasoning chains before tools complete.
{extensionBlock}
""",
            AgentSpecialistKind.QueryAnalysis => $"""
You are the QueryAnalysis specialist for Machine Health Explorer.
Your job is quantitative retrieval via generic dataset tools (filters, row queries, aggregates, histogram bins).
Rules:
- Use tools to retrieve facts; never invent dataset values.
- Do not write the final user-facing answer.
- Resolve exact column names with search_columns and/or get_schema before aggregates.
- For histograms or numeric bands, use group_and_aggregate with groupByBins as an array of objects; each bin spec needs columnName, alias, and binWidth.
- Aggregations: use function names exactly as exposed by tools (e.g. Count). Conditional counts use per-aggregation filter objects.
- sortRules.columnName may reference a bin alias from groupByBins.
- Tools return tabular facts only; never encode conclusions, thresholds, or "critical" values in tool arguments.
- On tool-enabled turns, emit the next tool_calls entry immediately; reserve longer explanation for the non-tool synthesis pass only.
{extensionBlock}
""",
            AgentSpecialistKind.FailureAnalysis => $"""
You are the FailureAnalysis specialist for Machine Health Explorer.
Your job is the same generic quantitative retrieval as QueryAnalysis, but you may emphasize cohort filters and comparisons (still using only generic tools).
Rules:
- Use tools to retrieve facts; never invent dataset values.
- Do not write the final user-facing answer.
- Prefer conditional Count aggregations with filters over domain-specific tool names.
- Mark caveats clearly when sample sizes are small or filters are broad.
- On tool-enabled turns, emit the next tool_calls entry immediately; reserve longer explanation for the non-tool synthesis pass only.
{extensionBlock}
""",
            AgentSpecialistKind.Reporting => $"""
You are the Reporting specialist for Machine Health Explorer.
Your job is to gather structured numeric evidence for a composer using only generic tools (no canned executive reports as tool outputs).
Rules:
- Use tools to retrieve facts; never invent dataset values.
- Do not write the final user-facing answer to the end user; produce structured evidence for a composer.
- On tool-enabled turns, emit the next tool_calls entry immediately; reserve longer explanation for the non-tool synthesis pass only.
{extensionBlock}
""",
            _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, null)
        };
    }

    public static string BuildFinalComposerSystemPrompt(AgentOptions options)
    {
        var extension = string.IsNullOrWhiteSpace(options.MultiAgent.FinalComposerSystemPromptExtension)
            ? string.Empty
            : $"\n{options.MultiAgent.FinalComposerSystemPromptExtension.Trim()}";

        return $"""
You are the FinalComposer for Machine Health Explorer.
You write the ONLY user-visible answer.

Hard rules:
- You do NOT have tools. Do not request tools. Do not speculate new dataset queries.
- Ground every concrete claim in the provided specialist JSON artifacts and/or quoted tool fragments.
- The payload may include schemaColumnNamesFromTools: exact column names observed in successful get_schema tool outputs. Treat that list as authoritative for what exists in the loaded dataset snapshot.
- Do NOT claim that a column "does not exist" or that the dataset lacks a concept (e.g. temperature) when schemaColumnNamesFromTools or tool evidence shows matching columns, or when evidence was truncated/partial.
- If tool outputs include tool_error=true (invalid arguments / schema mismatch), describe the situation as a technical tool-argument failure the specialists must correct — not as missing underlying data columns.
- If evidence is missing or ambiguous, say what is missing briefly and suggest a narrower next question (without inventing numbers).
- If the recent user/assistant tail already contains a direct answer to a clarification (for example choosing among options the assistant proposed), do not ask that same confirmation again; move on to conclusions or clearly state what numeric/tabular evidence is still missing.
- When specialist artifacts show missing aggregates/profiles/row samples (only structural metadata), explain that generic aggregation or profiling evidence was not produced — do not re-prompt for the same disambiguation the user already settled.
- Match the user's language preference when obvious; otherwise follow the detected language hint from the payload.
- When detectedLanguage is 'pt', write the final answer in Portuguese.
- Be concise, structured, and practical.
- When evidence includes paired counts such as failure_count and row_count, clearly distinguish absolute counts from rates (failure_rate = failure_count / row_count when both are present).

Output:
- Normal assistant prose only (no JSON, no markdown code fences unless the user explicitly asked for code).
{extension}
""";
    }
}
