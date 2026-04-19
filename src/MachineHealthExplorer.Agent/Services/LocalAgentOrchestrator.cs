using MachineHealthExplorer.Agent.Abstractions;
using MachineHealthExplorer.Agent.Models;
using MachineHealthExplorer.Tools.Abstractions;

namespace MachineHealthExplorer.Agent.Services;

public sealed class LocalAgentOrchestrator : IAgentOrchestrator
{
    private readonly IDatasetToolCatalog _toolCatalog;

    public LocalAgentOrchestrator(IDatasetToolCatalog toolCatalog)
    {
        _toolCatalog = toolCatalog ?? throw new ArgumentNullException(nameof(toolCatalog));
    }

    public Task<IReadOnlyList<ToolRegistrationDescriptor>> DescribeToolsAsync(CancellationToken cancellationToken = default)
        => Task.FromResult<IReadOnlyList<ToolRegistrationDescriptor>>(
            _toolCatalog.GetTools()
                .Select(tool => new ToolRegistrationDescriptor
                {
                    Name = tool.Name,
                    Description = tool.Description,
                    InputHints = tool.InputHints,
                    ExamplePrompts = tool.ExamplePrompts
                })
                .ToArray());

    public async Task<AgentExecutionResult> RunAsync(AgentExecutionContext context, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(context);

        var availableTools = await DescribeToolsAsync(cancellationToken).ConfigureAwait(false);
        return new AgentExecutionResult
        {
            IsImplemented = false,
            Message = "Microsoft Agent Framework integration is still a placeholder, but the reusable analytics services and centralized tool catalog are already separated so future MAF tool registration can reuse the same contracts.",
            AvailableTools = availableTools
        };
    }
}
