using MachineHealthExplorer.Agent.Models;
using MachineHealthExplorer.Agent.Services;
using MachineHealthExplorer.Logging;
using MachineHealthExplorer.Logging.Models;
using MachineHealthExplorer.Logging.Services;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Text.Json;

namespace MachineHealthExplorer.Tests;

public sealed class ChatSessionLoggingTests
{
    [Fact]
    public void ChatSessionManager_StartNewSession_CreatesDedicatedLogFile()
    {
        using var temp = CreateTempLogsRoot(out var manager);
        manager.StartNewSession();

        var session = manager.CurrentSession;
        Assert.NotNull(session);
        Assert.True(File.Exists(session!.LogFilePath), "O arquivo de log da sessão deve existir.");
        Assert.EndsWith(".jsonl", session.LogFilePath, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void ChatSessionManager_Append_WritesMultipleEventsToSameSessionFile()
    {
        using var temp = CreateTempLogsRoot(out var manager);
        manager.StartNewSession();
        var path = manager.CurrentSession!.LogFilePath;

        manager.Append(CreateSampleEvent("test.one", "outbound"));
        manager.Append(CreateSampleEvent("test.two", "inbound"));

        var lines = File.ReadAllLines(path);
        Assert.Equal(2, lines.Length);
        Assert.Contains("test.one", lines[0], StringComparison.Ordinal);
        Assert.Contains("test.two", lines[1], StringComparison.Ordinal);
    }

    [Fact]
    public void ChatSessionManager_Reset_StartsNewSessionWithNewFile()
    {
        using var temp = CreateTempLogsRoot(out var manager);
        manager.StartNewSession();
        var firstPath = manager.CurrentSession!.LogFilePath;
        manager.Append(CreateSampleEvent("before.reset", "outbound"));

        manager.StartNewSession();
        var secondPath = manager.CurrentSession!.LogFilePath;

        Assert.False(string.Equals(firstPath, secondPath, StringComparison.OrdinalIgnoreCase));
        manager.Append(CreateSampleEvent("after.reset", "outbound"));

        Assert.Single(File.ReadAllLines(firstPath));
        Assert.Single(File.ReadAllLines(secondPath));
    }

    [Fact]
    public async Task ChatSessionManager_DeleteAllLogFilesAsync_DeletesOnlyConfiguredLogsDirectory()
    {
        using var logsTemp = CreateTempLogsRoot(out var manager);
        var otherDir = Path.Combine(Path.GetTempPath(), "mhe-other-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(otherDir);
        var otherFile = Path.Combine(otherDir, "keep-me.txt");
        await File.WriteAllTextAsync(otherFile, "x");

        manager.StartNewSession();
        var sessionFile = manager.CurrentSession!.LogFilePath;
        await File.WriteAllTextAsync(Path.Combine(logsTemp.Path, "extra.jsonl"), "{}");

        await manager.DeleteAllLogFilesAsync();

        Assert.False(File.Exists(sessionFile));
        Assert.False(File.Exists(Path.Combine(logsTemp.Path, "extra.jsonl")));
        Assert.True(File.Exists(otherFile));
        Assert.Equal("x", await File.ReadAllTextAsync(otherFile));

        Directory.Delete(otherDir, recursive: true);
    }

    [Fact]
    public async Task ChatSessionManager_DeleteAllLogFilesAsync_PreservesGitKeepMarker()
    {
        using var temp = CreateTempLogsRoot(out var manager);
        var gitKeep = Path.Combine(temp.Path, ".gitkeep");
        File.WriteAllText(gitKeep, string.Empty);
        File.WriteAllText(Path.Combine(temp.Path, "chat-test.jsonl"), "{}");

        await manager.DeleteAllLogFilesAsync();

        Assert.True(File.Exists(gitKeep));
        Assert.False(File.Exists(Path.Combine(temp.Path, "chat-test.jsonl")));
    }

    [Fact]
    public void ChatSessionManager_AppendTwiceWithoutSessionRotation_MirrorsClearCommandBehavior()
    {
        using var temp = CreateTempLogsRoot(out var manager);
        manager.StartNewSession();
        var path = manager.CurrentSession!.LogFilePath;

        manager.Append(CreateSampleEvent("a", "outbound"));
        manager.Append(CreateSampleEvent("b", "inbound"));

        Assert.Equal(2, File.ReadAllLines(path).Length);
        Assert.Equal(path, manager.CurrentSession!.LogFilePath, StringComparer.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task LmStudioChatClient_LogsRequestAndResponse_OnSuccessfulHttpExchange()
    {
        const string responseJson = """
        {
          "model": "google/gemma-4-e4b",
          "choices": [
            {
              "message": { "role": "assistant", "content": "ok" },
              "finish_reason": "stop"
            }
          ]
        }
        """;

        using var temp = CreateTempLogsRoot(out var manager);
        manager.StartNewSession();
        var logPath = manager.CurrentSession!.LogFilePath;

        using var httpClient = new HttpClient(new StubHttpMessageHandler(responseJson))
        {
            BaseAddress = new Uri("http://127.0.0.1:1234/v1/")
        };

        using var chatClient = new LmStudioChatClient(
            new AgentOptions { BaseUrl = "http://127.0.0.1:1234/v1", Model = "google/gemma-4-e4b" },
            httpClient,
            manager);

        _ = await chatClient.CompleteAsync(new AgentModelRequest
        {
            Model = "google/gemma-4-e4b",
            SystemPrompt = "sys",
            Messages = [new AgentConversationMessage { Role = AgentConversationRole.User, Content = "hi" }]
        });

        var lines = File.ReadAllLines(logPath);
        Assert.Equal(2, lines.Length);
        Assert.Equal("lm_studio.chat_completions.request", ReadEventType(lines[0]));
        Assert.Equal("outbound", ReadDirection(lines[0]));
        Assert.Equal("lm_studio.chat_completions.response", ReadEventType(lines[1]));
        Assert.Equal("inbound", ReadDirection(lines[1]));
        Assert.False(string.IsNullOrWhiteSpace(ReadSessionId(lines[0])));
        Assert.Equal(ReadSessionId(lines[0]), ReadSessionId(lines[1]), StringComparer.Ordinal);
    }

    [Fact]
    public async Task LmStudioChatClient_LogsError_OnHttpFailure()
    {
        using var temp = CreateTempLogsRoot(out var manager);
        manager.StartNewSession();
        var logPath = manager.CurrentSession!.LogFilePath;

        using var httpClient = new HttpClient(new StubHttpMessageHandler("{}", HttpStatusCode.BadRequest))
        {
            BaseAddress = new Uri("http://127.0.0.1:1234/v1/")
        };

        using var chatClient = new LmStudioChatClient(
            new AgentOptions { BaseUrl = "http://127.0.0.1:1234/v1", Model = "m" },
            httpClient,
            manager);

        await Assert.ThrowsAsync<InvalidOperationException>(() => chatClient.CompleteAsync(new AgentModelRequest
        {
            Model = "m",
            SystemPrompt = "sys",
            Messages = [new AgentConversationMessage { Role = AgentConversationRole.User, Content = "hi" }]
        }));

        var lines = File.ReadAllLines(logPath);
        Assert.Equal(2, lines.Length);
        Assert.Equal("lm_studio.chat_completions.request", ReadEventType(lines[0]));
        Assert.Equal("lm_studio.chat_completions.error", ReadEventType(lines[1]));
        Assert.Equal("error", ReadDirection(lines[1]));
        Assert.Equal(400, ReadHttpStatus(lines[1]));
    }

    [Fact]
    public async Task LmStudioChatClient_LogsError_OnTransportException()
    {
        using var temp = CreateTempLogsRoot(out var manager);
        manager.StartNewSession();
        var logPath = manager.CurrentSession!.LogFilePath;

        using var httpClient = new HttpClient(new ThrowingHttpMessageHandler())
        {
            BaseAddress = new Uri("http://127.0.0.1:1234/v1/")
        };

        using var chatClient = new LmStudioChatClient(
            new AgentOptions { BaseUrl = "http://127.0.0.1:1234/v1", Model = "m" },
            httpClient,
            manager);

        await Assert.ThrowsAsync<HttpRequestException>(() => chatClient.CompleteAsync(new AgentModelRequest
        {
            Model = "m",
            SystemPrompt = "sys",
            Messages = [new AgentConversationMessage { Role = AgentConversationRole.User, Content = "hi" }]
        }));

        var lines = File.ReadAllLines(logPath);
        Assert.Equal(2, lines.Length);
        Assert.Equal("lm_studio.chat_completions.request", ReadEventType(lines[0]));
        Assert.Equal("lm_studio.chat_completions.error", ReadEventType(lines[1]));
        Assert.Equal("error", ReadDirection(lines[1]));
    }

    private static ChatSessionLogEvent CreateSampleEvent(string eventType, string direction)
        => new(
            default,
            string.Empty,
            eventType,
            direction,
            "test-model",
            null,
            null,
            "{}",
            null,
            null,
            null);

    private static TempLogsRoot CreateTempLogsRoot(out ChatSessionManager manager)
    {
        var root = new TempLogsRoot();
        manager = new ChatSessionManager(new ChatSessionLoggingOptions { LogsDirectory = root.Path });
        return root;
    }

    private static string ReadEventType(string jsonLine)
        => JsonDocument.Parse(jsonLine).RootElement.GetProperty("eventType").GetString() ?? string.Empty;

    private static string ReadDirection(string jsonLine)
        => JsonDocument.Parse(jsonLine).RootElement.GetProperty("direction").GetString() ?? string.Empty;

    private static string ReadSessionId(string jsonLine)
        => JsonDocument.Parse(jsonLine).RootElement.GetProperty("sessionId").GetString() ?? string.Empty;

    private static int ReadHttpStatus(string jsonLine)
        => JsonDocument.Parse(jsonLine).RootElement.GetProperty("httpStatusCode").GetInt32();

    private sealed class TempLogsRoot : IDisposable
    {
        public string Path { get; }

        public TempLogsRoot()
        {
            Path = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "mhe-chat-logs-" + Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(Path);
        }

        public void Dispose()
        {
            try
            {
                if (Directory.Exists(Path))
                {
                    Directory.Delete(Path, recursive: true);
                }
            }
            catch
            {
            }
        }
    }

    private sealed class StubHttpMessageHandler : HttpMessageHandler
    {
        private readonly string _responseJson;
        private readonly HttpStatusCode _statusCode;

        public StubHttpMessageHandler(string responseJson, HttpStatusCode statusCode = HttpStatusCode.OK)
        {
            _responseJson = responseJson;
            _statusCode = statusCode;
        }

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
            => Task.FromResult(new HttpResponseMessage(_statusCode)
            {
                Content = new StringContent(_responseJson, Encoding.UTF8, "application/json")
            });
    }

    private sealed class ThrowingHttpMessageHandler : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
            => throw new HttpRequestException("network failure");
    }
}
