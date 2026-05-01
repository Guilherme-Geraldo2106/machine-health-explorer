using MachineHealthExplorer.Agent.Abstractions;
using MachineHealthExplorer.Agent.Models;
using MachineHealthExplorer.Domain.Abstractions;
using MachineHealthExplorer.Domain.Models;
using MachineHealthExplorer.Logging.Abstractions;
using MachineHealthExplorer.Tools.Abstractions;
using Microsoft.Extensions.Hosting;

namespace MachineHealthExplorer.Host;

public sealed class ConsoleHostedService : BackgroundService
{
    private readonly IDatasetToolService _toolService;
    private readonly IDatasetAnalyticsEngine _datasetAnalyticsEngine;
    private readonly IMachineHealthAnalyticsService _machineHealthAnalytics;
    private readonly IAgentOrchestrator _agentOrchestrator;
    private readonly IHostApplicationLifetime _hostApplicationLifetime;
    private readonly AgentOptions _agentOptions;
    private readonly IChatSessionManager _chatSessionManager;
    private readonly IReadOnlyDictionary<string, ConsoleCommandDefinition> _commands;
    private List<AgentConversationMessage> _conversationHistory = [];
    private AgentConversationMemory? _conversationMemory;

    public ConsoleHostedService(
        IDatasetToolService toolService,
        IDatasetAnalyticsEngine datasetAnalyticsEngine,
        IMachineHealthAnalyticsService machineHealthAnalytics,
        IAgentOrchestrator agentOrchestrator,
        AgentOptions agentOptions,
        IChatSessionManager chatSessionManager,
        IHostApplicationLifetime hostApplicationLifetime)
    {
        _toolService = toolService ?? throw new ArgumentNullException(nameof(toolService));
        _datasetAnalyticsEngine = datasetAnalyticsEngine ?? throw new ArgumentNullException(nameof(datasetAnalyticsEngine));
        _machineHealthAnalytics = machineHealthAnalytics ?? throw new ArgumentNullException(nameof(machineHealthAnalytics));
        _agentOrchestrator = agentOrchestrator ?? throw new ArgumentNullException(nameof(agentOrchestrator));
        _agentOptions = agentOptions ?? throw new ArgumentNullException(nameof(agentOptions));
        _chatSessionManager = chatSessionManager ?? throw new ArgumentNullException(nameof(chatSessionManager));
        _hostApplicationLifetime = hostApplicationLifetime ?? throw new ArgumentNullException(nameof(hostApplicationLifetime));
        _commands = CreateCommands();
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            await RunDemoAsync(stoppingToken).ConfigureAwait(false);
            await RunInteractiveLoopAsync(stoppingToken).ConfigureAwait(false);
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            Console.WriteLine("Fatal error while running the host:");
            Console.WriteLine(exception);
        }
        finally
        {
            _hostApplicationLifetime.StopApplication();
        }
    }

    private async Task RunDemoAsync(CancellationToken cancellationToken)
    {
        var datasetDescription = await _toolService.DescribeDatasetAsync(cancellationToken).ConfigureAwait(false);

        Console.WriteLine("MachineHealthExplorer");
        Console.WriteLine("=====================");
        Console.WriteLine($"Dataset ready: {datasetDescription.DatasetName}");
        Console.WriteLine($"Rows: {datasetDescription.RowCount} | Columns: {datasetDescription.ColumnCount}");
        Console.WriteLine($"Agent backend: {_agentOptions.Provider} @ {_agentOptions.BaseUrl}");
        Console.WriteLine($"Model: {(string.IsNullOrWhiteSpace(_agentOptions.Model) ? "auto-discover from LM Studio" : _agentOptions.Model)}");
        Console.WriteLine();

        Console.WriteLine("Dataset highlights");
        Console.WriteLine("------------------");
        foreach (var highlight in datasetDescription.Highlights)
        {
            Console.WriteLine($"- {highlight}");
        }

        Console.WriteLine();
        Console.WriteLine("Type a natural-language question to chat with the agent.");
        Console.WriteLine("Type 'help' for console commands, 'reset' for a new chat session, 'clear' to clear the screen, or 'exit' to quit.");
        Console.WriteLine();
    }

    private async Task RunInteractiveLoopAsync(CancellationToken cancellationToken)
    {
        _chatSessionManager.StartNewSession();
        PrintCurrentChatLogPath();
        while (!cancellationToken.IsCancellationRequested)
        {
            Console.Write("> ");
            var input = Console.ReadLine();
            if (input is null)
            {
                break;
            }

            var command = input.Trim();
            if (string.IsNullOrWhiteSpace(command))
            {
                continue;
            }

            if (command.Equals("exit", StringComparison.OrdinalIgnoreCase)
                || command.Equals("quit", StringComparison.OrdinalIgnoreCase))
            {
                break;
            }

            try
            {
                await HandleCommandAsync(command, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception exception) when (exception is not OperationCanceledException)
            {
                Console.WriteLine($"Command failed: {exception.Message}");
            }

            Console.WriteLine();
        }
    }

    private async Task HandleCommandAsync(string input, CancellationToken cancellationToken)
    {
        var tokens = input.Split(' ', 2, StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        var commandName = tokens[0];
        var arguments = tokens.Length > 1 ? tokens[1] : null;

        if (_commands.TryGetValue(commandName, out var command))
        {
            await command.Handler(arguments, cancellationToken).ConfigureAwait(false);
            return;
        }

        await RunAgentChatAsync(input, cancellationToken).ConfigureAwait(false);
    }

    private IReadOnlyDictionary<string, ConsoleCommandDefinition> CreateCommands()
    {
        var commands = new[]
        {
            new ConsoleCommandDefinition(
                "help",
                "Show the available console commands.",
                "help",
                (arguments, cancellationToken) =>
                {
                    PrintHelp();
                    return Task.CompletedTask;
                }),
            new ConsoleCommandDefinition(
                "reset",
                "Clear the current chat history and start a new logging session (new log file).",
                "reset",
                (arguments, cancellationToken) =>
                {
                    _conversationHistory = [];
                    _conversationMemory = null;
                    _chatSessionManager.StartNewSession();
                    Console.WriteLine("Conversation history cleared and a new chat session was started.");
                    PrintCurrentChatLogPath();
                    return Task.CompletedTask;
                }),
            new ConsoleCommandDefinition(
                "clear",
                "Clear the console screen without changing chat history or logs.",
                "clear",
                (arguments, cancellationToken) =>
                {
                    Console.Clear();
                    return Task.CompletedTask;
                }),
            new ConsoleCommandDefinition(
                "delete-logs",
                "Delete all JSONL files in the dedicated chat logs folder and start a fresh logging session.",
                "delete-logs",
                async (arguments, cancellationToken) =>
                {
                    await _chatSessionManager.DeleteAllLogFilesAsync(cancellationToken).ConfigureAwait(false);
                    _chatSessionManager.StartNewSession();
                    Console.WriteLine("Chat log files were deleted and a new logging session was started.");
                    PrintCurrentChatLogPath();
                }),
            new ConsoleCommandDefinition(
                "highlights",
                "Show the dataset summary and key highlights.",
                "highlights",
                (arguments, cancellationToken) => PrintDatasetHighlightsAsync(cancellationToken)),
            new ConsoleCommandDefinition(
                "schema",
                "List the inferred schema with types and cardinality hints.",
                "schema",
                (arguments, cancellationToken) => PrintSchemaAsync(cancellationToken)),
            new ConsoleCommandDefinition(
                "failures",
                "Show the reusable failure analysis summary.",
                "failures",
                (arguments, cancellationToken) => PrintFailureAnalysisAsync(cancellationToken)),
            new ConsoleCommandDefinition(
                "compare",
                "Compare failed rows against healthy rows.",
                "compare",
                (arguments, cancellationToken) => PrintFailureComparisonAsync(cancellationToken)),
            new ConsoleCommandDefinition(
                "report",
                "Build the machine-health executive report.",
                "report",
                (arguments, cancellationToken) => PrintExecutiveReportAsync(cancellationToken)),
            new ConsoleCommandDefinition(
                "examples",
                "List reusable multi-filter, grouping, and comparison examples.",
                "examples",
                (arguments, cancellationToken) => PrintExamplesAsync(cancellationToken)),
            new ConsoleCommandDefinition(
                "example",
                "Run a reusable example by name.",
                "example <name>",
                (arguments, cancellationToken) => RunNamedExampleAsync(arguments, cancellationToken)),
            new ConsoleCommandDefinition(
                "query",
                "Run the reusable multi-filter row-query example.",
                "query",
                (arguments, cancellationToken) => RunNamedExampleAsync("high-stress-failures", cancellationToken)),
            new ConsoleCommandDefinition(
                "group",
                "Run the reusable grouping example.",
                "group",
                (arguments, cancellationToken) => RunNamedExampleAsync("failure-rate-by-type", cancellationToken)),
            new ConsoleCommandDefinition(
                "search",
                "Search schema columns by keyword.",
                "search <keyword>",
                (arguments, cancellationToken) => SearchColumnsAsync(arguments, cancellationToken)),
            new ConsoleCommandDefinition(
                "tools",
                "List the tools exposed to the agent.",
                "tools",
                (arguments, cancellationToken) => PrintToolCatalogAsync(cancellationToken)),
            new ConsoleCommandDefinition(
                "agent",
                "Show the agent configuration and registered tools.",
                "agent",
                (arguments, cancellationToken) => PrintAgentStateAsync(cancellationToken))
        };

        return commands.ToDictionary(command => command.Name, StringComparer.OrdinalIgnoreCase);
    }

    private void PrintHelp()
    {
        Console.WriteLine("Commands:");
        foreach (var command in _commands.Values.OrderBy(command => command.Name, StringComparer.OrdinalIgnoreCase))
        {
            Console.WriteLine($"- {command.Usage}: {command.Description}");
        }

        Console.WriteLine("- exit: Stop the host.");
        Console.WriteLine("- Any other input is sent to the LM Studio agent.");
    }

    private void PrintCurrentChatLogPath()
    {
        var session = _chatSessionManager.CurrentSession;
        if (session is not null)
        {
            Console.WriteLine($"Chat log JSONL: {session.LogFilePath}");
        }
    }

    private async Task PrintDatasetHighlightsAsync(CancellationToken cancellationToken)
    {
        var description = await _toolService.DescribeDatasetAsync(cancellationToken).ConfigureAwait(false);
        Console.WriteLine(description.DatasetName);
        foreach (var highlight in description.Highlights)
        {
            Console.WriteLine($"- {highlight}");
        }
    }

    private async Task PrintSchemaAsync(CancellationToken cancellationToken)
    {
        var schema = await _toolService.GetSchemaAsync(cancellationToken).ConfigureAwait(false);
        foreach (var column in schema.Columns)
        {
            Console.WriteLine($"- {column.Name} | {column.DataType} | nullable={column.IsNullable} | cardinality={column.CardinalityHint}");
        }
    }

    private async Task PrintFailureAnalysisAsync(CancellationToken cancellationToken)
    {
        var summary = await _machineHealthAnalytics.GetFailureAnalysisAsync(cancellationToken).ConfigureAwait(false);
        PrintFailureAnalysis(summary);
    }

    private async Task PrintFailureComparisonAsync(CancellationToken cancellationToken)
    {
        var comparison = await _machineHealthAnalytics.CompareFailureCohortsAsync(cancellationToken).ConfigureAwait(false);
        PrintComparison(comparison);
    }

    private async Task PrintExecutiveReportAsync(CancellationToken cancellationToken)
    {
        var report = await _machineHealthAnalytics.BuildExecutiveReportAsync(cancellationToken).ConfigureAwait(false);
        PrintReport(report);
    }

    private async Task PrintExamplesAsync(CancellationToken cancellationToken)
    {
        var examples = await _machineHealthAnalytics.GetAnalysisExamplesAsync(cancellationToken).ConfigureAwait(false);
        foreach (var example in examples)
        {
            Console.WriteLine($"- {example.Name}: {example.Description}");
            Console.WriteLine($"  prompt: {example.SuggestedPrompt}");
        }
    }

    private async Task RunNamedExampleAsync(string? name, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            Console.WriteLine("Example name is required. Use 'examples' to see the available options.");
            return;
        }

        var examples = await _machineHealthAnalytics.GetAnalysisExamplesAsync(cancellationToken).ConfigureAwait(false);
        var example = examples.FirstOrDefault(candidate => candidate.Name.Equals(name.Trim(), StringComparison.OrdinalIgnoreCase));
        if (example is null)
        {
            Console.WriteLine($"Unknown example '{name}'.");
            Console.WriteLine("Available examples:");
            foreach (var candidate in examples)
            {
                Console.WriteLine($"- {candidate.Name}");
            }

            return;
        }

        await RunAnalysisExampleAsync(example, cancellationToken).ConfigureAwait(false);
    }

    private async Task SearchColumnsAsync(string? keyword, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(keyword))
        {
            Console.WriteLine("A search keyword is required.");
            return;
        }

        var result = await _toolService.SearchColumnsAsync(keyword, cancellationToken).ConfigureAwait(false);
        foreach (var match in result.Matches)
        {
            Console.WriteLine($"- {match.ColumnName}: {match.MatchReason}");
        }
    }

    private async Task PrintAgentStateAsync(CancellationToken cancellationToken)
    {
        var tools = await _agentOrchestrator.DescribeToolsAsync(cancellationToken).ConfigureAwait(false);
        Console.WriteLine($"Provider: {_agentOptions.Provider}");
        Console.WriteLine($"Base URL: {_agentOptions.BaseUrl}");
        Console.WriteLine($"Model: {(string.IsNullOrWhiteSpace(_agentOptions.Model) ? "auto-discover from LM Studio" : _agentOptions.Model)}");
        Console.WriteLine($"Conversation turns stored: {_conversationHistory.Count(message => message.Role is AgentConversationRole.User or AgentConversationRole.Assistant)}");
        Console.WriteLine("Registered tools:");
        foreach (var tool in tools.OrderBy(tool => tool.Name, StringComparer.OrdinalIgnoreCase))
        {
            Console.WriteLine($"- {tool.Name}: {tool.Description}");
        }
    }

    private async Task RunAnalysisExampleAsync(AnalysisExample example, CancellationToken cancellationToken)
    {
        Console.WriteLine(example.Description);
        if (!string.IsNullOrWhiteSpace(example.SuggestedPrompt))
        {
            Console.WriteLine($"Prompt hint: {example.SuggestedPrompt}");
        }

        switch (example.Kind)
        {
            case AnalysisExampleKind.RowQuery when example.RowQuery is not null:
                PrintRows(await _toolService.QueryRowsAsync(example.RowQuery, cancellationToken).ConfigureAwait(false));
                break;
            case AnalysisExampleKind.GroupAggregation when example.GroupAggregationQuery is not null:
                PrintGroups(await _toolService.GroupAndAggregateAsync(example.GroupAggregationQuery, cancellationToken).ConfigureAwait(false));
                break;
            case AnalysisExampleKind.SubsetComparison when example.ComparisonQuery is not null:
                PrintComparison(await _datasetAnalyticsEngine.CompareSubsetsAsync(example.ComparisonQuery, cancellationToken).ConfigureAwait(false));
                break;
            default:
                Console.WriteLine("The example is not configured correctly.");
                break;
        }
    }

    private static void PrintRows(QueryResult result)
    {
        Console.WriteLine($"Total rows: {result.TotalCount}");
        foreach (var row in result.Rows)
        {
            Console.WriteLine(string.Join(" | ", result.Columns.Select(column => $"{column}={row.Values.GetValueOrDefault(column)}")));
        }
    }

    private static void PrintGroups(GroupAggregationResult result)
    {
        Console.WriteLine($"Groups: {result.TotalGroups} | Scoped rows: {result.ScopedRowCount}");
        foreach (var row in result.Rows)
        {
            Console.WriteLine(string.Join(" | ", result.Columns.Select(column => $"{column}={row.Values.GetValueOrDefault(column)}")));
        }
    }

    private static void PrintFailureAnalysis(FailureAnalysisSummary summary)
    {
        Console.WriteLine($"Indicator: {summary.FailureIndicatorColumn}");
        Console.WriteLine($"Failures: {summary.FailureCount}");
        Console.WriteLine($"Healthy: {summary.HealthyCount}");
        Console.WriteLine($"Failure rate: {summary.FailureRate:P2}");
        foreach (var mode in summary.FailureModes.Take(5))
        {
            Console.WriteLine($"- {mode.Value}: {mode.Count} rows ({mode.Percentage:P2})");
        }
    }

    private static void PrintComparison(SubsetComparisonResult result)
    {
        Console.WriteLine($"{result.Left.Label}: {result.Left.RowCount}");
        Console.WriteLine($"{result.Right.Label}: {result.Right.RowCount}");

        foreach (var metric in result.NumericComparisons.Take(5))
        {
            Console.WriteLine(
                $"- {metric.ColumnName}: left avg={metric.Left.Average?.ToString("F3") ?? "n/a"}, right avg={metric.Right.Average?.ToString("F3") ?? "n/a"}, delta={metric.AverageDelta?.ToString("F3") ?? "n/a"}");
        }

        foreach (var metric in result.CategoricalComparisons.Take(2))
        {
            var leftValues = string.Join(", ", metric.LeftTopValues.Take(3).Select(value => $"{value.Value} ({value.Count})"));
            var rightValues = string.Join(", ", metric.RightTopValues.Take(3).Select(value => $"{value.Value} ({value.Count})"));
            Console.WriteLine($"- {metric.ColumnName}: left top={leftValues}; right top={rightValues}");
        }
    }

    private static void PrintReport(DatasetReport report)
    {
        Console.WriteLine(report.Title);
        Console.WriteLine(report.Summary);
        foreach (var section in report.Sections)
        {
            Console.WriteLine($"[{section.Heading}]");
            Console.WriteLine(section.Content);
        }
    }

    private async Task PrintToolCatalogAsync(CancellationToken cancellationToken)
    {
        var tools = await _agentOrchestrator.DescribeToolsAsync(cancellationToken).ConfigureAwait(false);
        foreach (var tool in tools.OrderBy(tool => tool.Name, StringComparer.OrdinalIgnoreCase))
        {
            Console.WriteLine($"- {tool.Name}: {tool.Description}");
        }
    }

    private async Task RunAgentChatAsync(string userInput, CancellationToken cancellationToken)
    {
        try
        {
            var result = await _agentOrchestrator.RunAsync(new AgentExecutionContext
            {
                UserInput = userInput,
                ConversationHistory = _conversationHistory,
                ConversationMemory = _conversationMemory
            }, cancellationToken).ConfigureAwait(false);

            _conversationHistory = result.UpdatedConversation.ToList();
            _conversationMemory = result.UpdatedConversationMemory ?? _conversationMemory;

            if (result.ToolExecutions.Count > 0)
            {
                Console.WriteLine($"[tools] {string.Join(", ", result.ToolExecutions.Select(tool => tool.ToolName).Distinct(StringComparer.OrdinalIgnoreCase))}");
            }

            Console.WriteLine(result.Message);
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            Console.WriteLine("Agent request failed.");
            Console.WriteLine(exception.Message);
            if (IsLikelyLmStudioInvalidToolParametersSchema(exception.Message))
            {
                Console.WriteLine(
                    "Dica: o LM Studio devolveu HTTP 400 por JSON Schema inválido nas ferramentas (muitas vezes falta de \"properties\" em algum objeto do schema, inclusive aninhado). " +
                    "Confira o evento JSONL \"lm_studio.chat_completions.request\" desta sessão para inspecionar tools[].function.parameters.");
            }
            else
            {
                Console.WriteLine("Make sure LM Studio is running, the local server is enabled, and a model with tool support is available.");
            }
        }
    }

    private sealed record ConsoleCommandDefinition(
        string Name,
        string Description,
        string Usage,
        Func<string?, CancellationToken, Task> Handler);

    private static bool IsLikelyLmStudioInvalidToolParametersSchema(string message)
    {
        if (string.IsNullOrWhiteSpace(message))
        {
            return false;
        }

        var looksLikeHttp400 = message.Contains("failed with 400", StringComparison.OrdinalIgnoreCase)
            || message.Contains("(400)", StringComparison.Ordinal);

        if (!looksLikeHttp400)
        {
            return false;
        }

        return message.Contains("parameters", StringComparison.OrdinalIgnoreCase)
            && message.Contains("properties", StringComparison.OrdinalIgnoreCase);
    }
}
