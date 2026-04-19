using MachineHealthExplorer.Agent.Abstractions;
using MachineHealthExplorer.Agent.Models;
using Microsoft.Extensions.Logging;

namespace MachineHealthExplorer.Agent.Services;

public sealed class LmStudioAgentOrchestrator : IAgentOrchestrator
{
    private readonly AgentSessionEngine _engine;

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
        var engineLogger = loggerFactory.CreateLogger("MachineHealthExplorer.Agent.AgentSessionEngine");
        _engine = new AgentSessionEngine(options, chatClient, toolRuntime, engineLogger);
    }

    public Task<IReadOnlyList<ToolRegistrationDescriptor>> DescribeToolsAsync(CancellationToken cancellationToken = default)
        => _engine.DescribeToolsAsync(cancellationToken);

    public Task<AgentExecutionResult> RunAsync(AgentExecutionContext context, CancellationToken cancellationToken = default)
        => _engine.RunAsync(context, cancellationToken);
}
