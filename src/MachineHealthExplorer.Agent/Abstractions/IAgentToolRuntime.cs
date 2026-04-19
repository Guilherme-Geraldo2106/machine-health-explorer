using MachineHealthExplorer.Agent.Models;

namespace MachineHealthExplorer.Agent.Abstractions;

public interface IAgentToolRuntime
{
    IReadOnlyList<AgentToolDefinition> GetTools();
    Task<AgentToolExecutionRecord> ExecuteAsync(string toolName, string argumentsJson, CancellationToken cancellationToken = default);
}
