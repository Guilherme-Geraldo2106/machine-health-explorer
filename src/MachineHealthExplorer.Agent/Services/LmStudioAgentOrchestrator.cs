using MachineHealthExplorer.Agent.Abstractions;
using MachineHealthExplorer.Agent.Models;
using MachineHealthExplorer.Agent.MultiAgent;
using Microsoft.Extensions.Logging;

namespace MachineHealthExplorer.Agent.Services;

public sealed class LmStudioAgentOrchestrator : IAgentOrchestrator
{
    private readonly MultiAgentSessionEngine _engine;

    public LmStudioAgentOrchestrator(
        AgentOptions options,
        IAgentChatClient chatClient,
        IAgentToolRuntime toolRuntime,
        ILoggerFactory loggerFactory)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(chatClient);
        ArgumentNullException.ThrowIfNull(toolRuntime);
        ArgumentNullException.ThrowIfNull(loggerFactory);
        var engineLogger = loggerFactory.CreateLogger("MachineHealthExplorer.Agent.MultiAgentSessionEngine");
        _engine = new MultiAgentSessionEngine(options, chatClient, toolRuntime, engineLogger);
    }

    public Task<IReadOnlyList<ToolRegistrationDescriptor>> DescribeToolsAsync(CancellationToken cancellationToken = default)
        => _engine.DescribeToolsAsync(cancellationToken);

    public Task<AgentExecutionResult> RunAsync(AgentExecutionContext context, CancellationToken cancellationToken = default)
        => _engine.RunAsync(context, cancellationToken);
}
