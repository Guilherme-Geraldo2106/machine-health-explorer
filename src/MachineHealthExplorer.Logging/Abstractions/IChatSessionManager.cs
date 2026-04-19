namespace MachineHealthExplorer.Logging.Abstractions;

public interface IChatSessionManager
{
    ChatSessionInfo? CurrentSession { get; }

    void StartNewSession();

    Task DeleteAllLogFilesAsync(CancellationToken cancellationToken = default);
}

public sealed record ChatSessionInfo(string SessionId, string LogFilePath);
