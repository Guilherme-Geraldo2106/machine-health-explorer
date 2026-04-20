namespace MachineHealthExplorer.Agent.Models;

public enum AgentSpecialistKind
{
    Discovery = 0,
    QueryAnalysis = 1,
    FailureAnalysis = 2,
    Reporting = 3
}

public sealed record AgentDispatchStep(
    AgentSpecialistKind SpecialistKind,
    string Reason,
    int ParallelGroup,
    /// <summary>
    /// When false, the specialist may stop after structural tools (schema/search) without requiring aggregates/profiles/row samples.
    /// </summary>
    bool ExpectsDatasetQueryEvidence = true);

public sealed record AgentDispatchPlan(
    IReadOnlyList<AgentDispatchStep> Steps,
    string CoordinatorNotes,
    bool UsedLlmPlanner);

/// <summary>
/// Outcome of coordinator planning when <see cref="MultiAgentOrchestrationOptions.EnableCoordinatorLlmPlanning"/> is true.
/// </summary>
public sealed record AgentCoordinatorPlanningResult(
    bool Success,
    AgentDispatchPlan Plan,
    string? UserVisibleFailureMessage);

public sealed record AgentEvidence(
    string SourceTool,
    string Summary,
    string? SupportingJsonFragment);

public sealed record AgentStructuredSpecialistOutput(
    AgentSpecialistKind SpecialistKind,
    IReadOnlyList<string> RelevantColumns,
    IReadOnlyList<string> Ambiguities,
    IReadOnlyList<AgentEvidence> Evidences,
    IReadOnlyDictionary<string, decimal> KeyMetrics,
    IReadOnlyList<string> ObjectiveObservations,
    IReadOnlyList<string> HypothesesOrCaveats,
    IReadOnlyList<string> ReportSections,
    string AnalystNotes);

public sealed record AgentTaskRequest(
    AgentSpecialistKind SpecialistKind,
    string UserQuestion,
    string DispatchReason,
    AgentConversationMemory CoordinatorMemory,
    IReadOnlyList<AgentConversationMessage> MinimalTranscriptTail,
    IReadOnlyList<AgentToolDefinition> AllowedTools,
    string Model,
    string SpecialistSystemPrompt,
    bool UseFullToolSchemas = true,
    int? ToolTurnMaxOutputTokensCap = null,
    bool ExpectsDatasetQueryEvidence = true);

public sealed record AgentTaskResult(
    AgentSpecialistKind SpecialistKind,
    bool Success,
    string? FailureMessage,
    IReadOnlyList<AgentToolExecutionRecord> ToolExecutions,
    AgentStructuredSpecialistOutput StructuredOutput,
    IReadOnlyList<AgentConversationMessage> SpecialistScratchTranscript);
