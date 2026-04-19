using MachineHealthExplorer.Agent.Models;

namespace MachineHealthExplorer.Agent.Abstractions;

public interface IAgentChatClient
{
    Task<AgentModelResponse> CompleteAsync(AgentModelRequest request, CancellationToken cancellationToken = default);
    Task<IReadOnlyList<string>> GetAvailableModelsAsync(CancellationToken cancellationToken = default);
}
