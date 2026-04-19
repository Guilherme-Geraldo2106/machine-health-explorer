namespace MachineHealthExplorer.Logging;

public sealed class ChatSessionLoggingOptions
{
    public const string SectionName = "ChatSessionLogging";

    /// <summary>
    /// Absolute or app-relative directory for JSONL session logs. When empty, the host resolves a default under the Logging project.
    /// </summary>
    public string LogsDirectory { get; set; } = string.Empty;
}
