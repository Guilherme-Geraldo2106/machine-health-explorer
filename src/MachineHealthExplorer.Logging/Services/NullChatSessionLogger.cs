using MachineHealthExplorer.Logging.Abstractions;
using MachineHealthExplorer.Logging.Models;

namespace MachineHealthExplorer.Logging.Services;

public sealed class NullChatSessionLogger : IChatSessionLogger
{
    public static readonly NullChatSessionLogger Instance = new();

    private NullChatSessionLogger()
    {
    }

    public void Append(ChatSessionLogEvent logEvent)
    {
    }
}
