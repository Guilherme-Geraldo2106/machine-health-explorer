using MachineHealthExplorer.Logging.Abstractions;
using MachineHealthExplorer.Logging.Models;
using MachineHealthExplorer.Logging.Serialization;

namespace MachineHealthExplorer.Logging.Services;

public sealed class ChatSessionManager : IChatSessionManager, IChatSessionLogger
{
    private readonly object _sync = new();
    private readonly string _logsRootFullPath;
    private ChatSessionInfo? _currentSession;

    public ChatSessionManager(ChatSessionLoggingOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        if (string.IsNullOrWhiteSpace(options.LogsDirectory))
        {
            throw new ArgumentException("LogsDirectory must be configured.", nameof(options));
        }

        _logsRootFullPath = Path.GetFullPath(options.LogsDirectory);
    }

    public ChatSessionInfo? CurrentSession
    {
        get
        {
            lock (_sync)
            {
                return _currentSession;
            }
        }
    }

    public void StartNewSession()
    {
        lock (_sync)
        {
            Directory.CreateDirectory(_logsRootFullPath);
            var sessionId = Guid.NewGuid().ToString("N");
            var stamp = DateTimeOffset.UtcNow.ToString("yyyyMMdd-HHmmss", System.Globalization.CultureInfo.InvariantCulture);
            var fileName = $"chat-{stamp}-{sessionId}.jsonl";
            var logPath = Path.Combine(_logsRootFullPath, fileName);
            File.WriteAllText(logPath, string.Empty);
            _currentSession = new ChatSessionInfo(sessionId, logPath);
        }
    }

    public void Append(ChatSessionLogEvent logEvent)
    {
        lock (_sync)
        {
            if (_currentSession is null)
            {
                return;
            }

            var finalized = logEvent with
            {
                SessionId = _currentSession.SessionId,
                Timestamp = DateTimeOffset.UtcNow
            };

            var line = ChatSessionLogJson.SerializeLine(finalized);
            File.AppendAllText(_currentSession.LogFilePath, line + Environment.NewLine);
        }
    }

    public Task DeleteAllLogFilesAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_sync)
        {
            Directory.CreateDirectory(_logsRootFullPath);
            foreach (var file in Directory.EnumerateFiles(_logsRootFullPath))
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (string.Equals(Path.GetFileName(file), ".gitkeep", StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                File.Delete(file);
            }

            _currentSession = null;
        }

        return Task.CompletedTask;
    }
}
