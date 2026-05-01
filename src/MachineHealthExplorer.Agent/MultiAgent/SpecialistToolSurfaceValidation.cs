using MachineHealthExplorer.Agent.Models;
using MachineHealthExplorer.Agent.Serialization;
using MachineHealthExplorer.Agent.Services;

namespace MachineHealthExplorer.Agent.MultiAgent;

internal static class SpecialistToolSurfaceValidation
{
    public static string BuildOutOfSurfaceToolResultJson(
        string requestedToolName,
        IReadOnlyList<AgentToolDefinition> exposedTools,
        IReadOnlyList<AgentToolDefinition> scopedCatalogForSchemas,
        bool useFullToolSchemas)
    {
        var exposed = exposedTools
            .Select(t => t.Name)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(n => n, StringComparer.OrdinalIgnoreCase)
            .ToArray();

        var compactContract = useFullToolSchemas
            ? MultiAgentPromptBuilder.BuildGroupAndAggregateCompactContractHint()
            : AgentPromptBudgetGuard.CompactPlain(MultiAgentPromptBuilder.BuildMinimalToolParametersContractHint(), 900);

        var schemaHint = FindParametersSchema(requestedToolName, scopedCatalogForSchemas);
        return AgentJsonSerializer.Serialize(new
        {
            tool_error = true,
            error =
                "Requested tool was not part of exposed_tools_this_turn for this round; pick one exposed tool and retry with valid arguments.",
            error_kind = "tool_not_on_exposed_surface_this_turn",
            requested_tool = requestedToolName,
            exposed_tools_this_turn = exposed,
            compact_contract = compactContract,
            requested_tool_parameters_schema_hint = string.IsNullOrWhiteSpace(schemaHint) ? null : schemaHint,
            instruction =
                "This tool name was not among exposed_tools_this_turn for this model round. Pick one exposed tool only, match arguments to compact_contract (and full schemas when present), then emit a valid tool_calls entry."
        });
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
