using MachineHealthExplorer.Agent.Models;

namespace MachineHealthExplorer.Agent.Abstractions;

public sealed record FinalComposerInput(
    string OriginalUserQuestion,
    string DetectedLanguage,
    string? ConversationRollingSummary,
    IReadOnlyList<AgentTaskResult> SpecialistResults,
    IReadOnlyList<AgentConversationMessage> RecentUserAssistantTail,
    IReadOnlyList<string>? SchemaColumnNamesFromTools = null);

public sealed record FinalComposerLlmTurn(
    string SystemPrompt,
    IReadOnlyList<AgentConversationMessage> PrefixMessages,
    AgentModelResponse FirstResponse);

public interface IFinalResponseComposer
{
    Task<FinalComposerLlmTurn> ComposeFirstResponseAsync(
        FinalComposerInput input,
        string model,
        AgentConversationMemory memory,
        CancellationToken cancellationToken = default);
}
