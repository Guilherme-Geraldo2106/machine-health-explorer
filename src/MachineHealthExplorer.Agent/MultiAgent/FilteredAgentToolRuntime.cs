using MachineHealthExplorer.Agent.Abstractions;
using MachineHealthExplorer.Agent.Models;

namespace MachineHealthExplorer.Agent.MultiAgent;

internal sealed class FilteredAgentToolRuntime : IAgentToolRuntime
{
    private readonly IAgentToolRuntime _inner;
    private readonly HashSet<string> _allowed;
    private readonly string _scopeLabel;

    public FilteredAgentToolRuntime(IAgentToolRuntime inner, IEnumerable<string> allowedToolNames, string scopeLabel)
    {
        _inner = inner ?? throw new ArgumentNullException(nameof(inner));
        ArgumentNullException.ThrowIfNull(allowedToolNames);
        _scopeLabel = scopeLabel ?? string.Empty;
        _allowed = new HashSet<string>(allowedToolNames, StringComparer.OrdinalIgnoreCase);
    }

    public IReadOnlyList<AgentToolDefinition> GetTools()
        => _inner.GetTools()
            .Where(tool => _allowed.Contains(tool.Name))
            .ToArray();

    public Task<AgentToolExecutionRecord> ExecuteAsync(string toolName, string argumentsJson, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(toolName);
        if (!_allowed.Contains(toolName))
        {
            var safeTool = toolName.Replace("\"", "'", StringComparison.Ordinal);
            var safeScope = _scopeLabel.Replace("\"", "'", StringComparison.Ordinal);
            return Task.FromResult(new AgentToolExecutionRecord
            {
                ToolName = toolName,
                ArgumentsJson = argumentsJson ?? "{}",
                ResultJson = $"{{\"error\":\"Tool '{safeTool}' is not allowed for scope '{safeScope}'.\"}}",
                IsError = true
            });
        }

        return _inner.ExecuteAsync(toolName, argumentsJson, cancellationToken);
    }
}
