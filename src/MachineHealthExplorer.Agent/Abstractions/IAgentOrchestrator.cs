using MachineHealthExplorer.Agent.Models;

namespace MachineHealthExplorer.Agent.Abstractions;

public interface IAgentOrchestrator
{
    Task<IReadOnlyList<ToolRegistrationDescriptor>> DescribeToolsAsync(CancellationToken cancellationToken = default);
    Task<AgentExecutionResult> RunAsync(AgentExecutionContext context, CancellationToken cancellationToken = default);
}
