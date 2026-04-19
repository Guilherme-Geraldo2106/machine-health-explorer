using MachineHealthExplorer.Agent.Abstractions;
using MachineHealthExplorer.Agent.Models;
using MachineHealthExplorer.Agent.Services;
using MachineHealthExplorer.Data.Infrastructure;
using MachineHealthExplorer.Data.Querying;
using MachineHealthExplorer.Data.Services;
using MachineHealthExplorer.Domain.Abstractions;
using MachineHealthExplorer.Host;
using MachineHealthExplorer.Logging;
using MachineHealthExplorer.Logging.Abstractions;
using MachineHealthExplorer.Logging.Services;
using MachineHealthExplorer.Tools.Abstractions;
using MachineHealthExplorer.Tools.Services;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

var builder = Host.CreateApplicationBuilder(args);

// Console: hide verbose agent diagnostics (Information). Chat JSONL is written by ChatSessionManager to disk, not via ILogger.
builder.Logging.AddFilter("MachineHealthExplorer.Agent", LogLevel.Warning);

var candidateConfigPaths = new[]
{
    Path.Combine(AppContext.BaseDirectory, "appsettings.json"),
    Path.Combine(builder.Environment.ContentRootPath, "appsettings.json"),
    Path.Combine(builder.Environment.ContentRootPath, "src", "MachineHealthExplorer.Host", "appsettings.json")
};

foreach (var path in candidateConfigPaths.Distinct(StringComparer.OrdinalIgnoreCase))
{
    if (File.Exists(path))
    {
        builder.Configuration.AddJsonFile(path, optional: false, reloadOnChange: false);
    }
}

var datasetOptions = builder.Configuration.GetSection("Dataset").Get<DatasetOptions>() ?? new DatasetOptions();
var agentOptions = builder.Configuration.GetSection("Agent").Get<AgentOptions>() ?? new AgentOptions();
var chatSessionLoggingOptions = builder.Configuration.GetSection(ChatSessionLoggingOptions.SectionName).Get<ChatSessionLoggingOptions>() ?? new ChatSessionLoggingOptions();
if (string.IsNullOrWhiteSpace(chatSessionLoggingOptions.LogsDirectory))
{
    chatSessionLoggingOptions.LogsDirectory = Path.GetFullPath(
        Path.Combine(builder.Environment.ContentRootPath, "..", "MachineHealthExplorer.Logging", "logs"));
}

builder.Services.AddSingleton(datasetOptions);
builder.Services.AddSingleton(agentOptions);
builder.Services.AddSingleton(chatSessionLoggingOptions);
builder.Services.AddSingleton<ChatSessionManager>();
builder.Services.AddSingleton<IChatSessionManager>(sp => sp.GetRequiredService<ChatSessionManager>());
builder.Services.AddSingleton<IChatSessionLogger>(sp => sp.GetRequiredService<ChatSessionManager>());
builder.Services.AddSingleton<CsvDatasetRepository>();
builder.Services.AddSingleton<IDatasetRepository>(provider => provider.GetRequiredService<CsvDatasetRepository>());
builder.Services.AddSingleton<IDatasetAnalyticsEngine, DatasetAnalyticsEngine>();
builder.Services.AddSingleton<IDatasetSchemaProvider>(provider => provider.GetRequiredService<IDatasetAnalyticsEngine>());
builder.Services.AddSingleton<IDatasetQueryEngine>(provider => provider.GetRequiredService<IDatasetAnalyticsEngine>());
builder.Services.AddSingleton<IDatasetAnalyticsService>(provider => provider.GetRequiredService<IDatasetAnalyticsEngine>());
builder.Services.AddSingleton<IMachineHealthAnalyticsService, MachineHealthAnalyticsService>();
builder.Services.AddSingleton<IDatasetToolService, DatasetToolService>();
builder.Services.AddSingleton<IDatasetToolCatalog, DatasetToolCatalog>();
builder.Services.AddSingleton<IAgentChatClient>(sp => new LmStudioChatClient(
    sp.GetRequiredService<AgentOptions>(),
    httpClient: null,
    sp.GetRequiredService<IChatSessionLogger>()));
builder.Services.AddSingleton<IAgentToolRuntime, DatasetAgentToolRuntime>();
builder.Services.AddSingleton<IAgentOrchestrator, LmStudioAgentOrchestrator>();
builder.Services.AddHostedService<ConsoleHostedService>();

await builder.Build().RunAsync().ConfigureAwait(false);
