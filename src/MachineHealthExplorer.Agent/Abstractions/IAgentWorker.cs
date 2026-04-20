using MachineHealthExplorer.Agent.Models;

namespace MachineHealthExplorer.Agent.Abstractions;

public interface IAgentWorker
{
    Task<AgentTaskResult> ExecuteAsync(AgentTaskRequest request, CancellationToken cancellationToken = default);
}
