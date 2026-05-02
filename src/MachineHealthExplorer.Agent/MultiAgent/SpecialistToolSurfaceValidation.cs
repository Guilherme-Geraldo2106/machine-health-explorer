using MachineHealthExplorer.Agent.Models;
using MachineHealthExplorer.Agent.Serialization;
using MachineHealthExplorer.Agent.Services;

namespace MachineHealthExplorer.Agent.MultiAgent;

internal static class SpecialistToolSurfaceValidation
{
    public static string BuildOutOfSurfaceToolResultJson(
        string requestedToolName,
        IReadOnlyList<string> specialistAllowedToolNames,
        IReadOnlyList<AgentToolDefinition> exposedToolsThisTurn,
        IReadOnlyList<AgentToolDefinition> scopedCatalogForSchemas,
        bool useFullToolSchemas)
    {
        var exposed = exposedToolsThisTurn
            .Select(t => t.Name)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(n => n, StringComparer.OrdinalIgnoreCase)
            .ToArray();

        var allowed = specialistAllowedToolNames
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(n => n, StringComparer.OrdinalIgnoreCase)
            .ToArray();

        var compactContract = useFullToolSchemas
            ? MultiAgentPromptBuilder.BuildGroupAndAggregateCompactContractHint()
            : AgentPromptBudgetGuard.CompactPlain(MultiAgentPromptBuilder.BuildMinimalToolParametersContractHint(), 900);

        var schemaDigest = BuildCompactToolSchemasDigest(scopedCatalogForSchemas);
        var schemaHint = FindParametersSchema(requestedToolName, scopedCatalogForSchemas);
        return AgentJsonSerializer.Serialize(new
        {
            tool_error = true,
            error =
                "Requested tool was not part of exposed_tools_this_turn for this round; pick one exposed tool and retry with valid arguments.",
            error_kind = "tool_not_on_exposed_surface_this_turn",
            requested_tool = requestedToolName,
            specialist_allowed_tools = allowed,
            exposed_tools_this_turn = exposed,
            compact_contract = compactContract,
            schemas_digest = schemaDigest,
            requested_tool_parameters_schema_hint = string.IsNullOrWhiteSpace(schemaHint) ? null : schemaHint,
            instruction =
                "Use only a tool name from exposed_tools_this_turn for this round (must also appear in specialist_allowed_tools). Match arguments to compact_contract / schemas_digest, then emit a valid tool_calls entry."
        });
    }

    public static string BuildToolNotOnSpecialistAllowlistResultJson(
        string requestedToolName,
        IReadOnlyList<string> specialistAllowlistToolNames,
        IReadOnlyList<AgentToolDefinition> exposedToolsThisTurn,
        IReadOnlyList<AgentToolDefinition> scopedCatalogForSchemas,
        bool useFullToolSchemas)
    {
        var allowed = specialistAllowlistToolNames
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(n => n, StringComparer.OrdinalIgnoreCase)
            .ToArray();

        var exposed = exposedToolsThisTurn
            .Select(t => t.Name)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(n => n, StringComparer.OrdinalIgnoreCase)
            .ToArray();

        var compactContract = useFullToolSchemas
            ? MultiAgentPromptBuilder.BuildGroupAndAggregateCompactContractHint()
            : AgentPromptBudgetGuard.CompactPlain(MultiAgentPromptBuilder.BuildMinimalToolParametersContractHint(), 900);

        var schemaDigest = BuildCompactToolSchemasDigest(scopedCatalogForSchemas);

        return AgentJsonSerializer.Serialize(new
        {
            tool_error = true,
            error =
                "Requested tool name is not available for this specialist allowlist. Pick one specialist_allowed_tools entry that appears in exposed_tools_this_turn, match arguments to compact_contract / schemas_digest, then emit a valid tool_calls entry.",
            error_kind = "tool_not_on_specialist_allowlist",
            requested_tool = requestedToolName,
            specialist_allowed_tools = allowed,
            exposed_tools_this_turn = exposed,
            compact_contract = compactContract,
            schemas_digest = schemaDigest,
            instruction =
                "Call only tools from specialist_allowed_tools. For this turn, the model only had exposed_tools_this_turn; intersection of those sets is the valid name set. Fix the tool name and arguments, then retry."
        });
    }

    private static string BuildCompactToolSchemasDigest(IReadOnlyList<AgentToolDefinition> catalog)
    {
        if (catalog.Count == 0)
        {
            return string.Empty;
        }

        var items = new List<string>(Math.Min(32, catalog.Count));
        foreach (var tool in catalog.OrderBy(t => t.Name, StringComparer.OrdinalIgnoreCase))
        {
            var schema = string.IsNullOrWhiteSpace(tool.ParametersJsonSchema)
                ? "{}"
                : AgentPromptBudgetGuard.CompactPlain(tool.ParametersJsonSchema, 700);
            items.Add($"{tool.Name}:{schema}");
        }

        return string.Join(" | ", items);
    }

    private static string? FindParametersSchema(string toolName, IReadOnlyList<AgentToolDefinition> catalog)
    {
        foreach (var tool in catalog)
        {
            if (tool.Name.Equals(toolName, StringComparison.OrdinalIgnoreCase))
            {
                var schema = tool.ParametersJsonSchema;
                return string.IsNullOrWhiteSpace(schema)
                    ? null
                    : AgentPromptBudgetGuard.CompactPlain(schema, 1600);
            }
        }

        return null;
    }
}
