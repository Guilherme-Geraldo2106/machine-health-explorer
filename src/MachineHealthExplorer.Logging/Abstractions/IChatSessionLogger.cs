using MachineHealthExplorer.Logging.Models;

namespace MachineHealthExplorer.Logging.Abstractions;

public interface IChatSessionLogger
{
    void Append(ChatSessionLogEvent logEvent);
}
