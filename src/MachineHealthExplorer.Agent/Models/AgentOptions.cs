namespace MachineHealthExplorer.Agent.Models;

public sealed record SpecialistAgentOptions
{
    public bool Enabled { get; init; } = true;
    /// <summary>
    /// Optional extra system prompt appended after the built-in specialist instructions.
    /// </summary>
    public string SystemPromptExtension { get; init; } = string.Empty;
}

public sealed record MultiAgentOrchestrationOptions
{
    /// <summary>
    /// When false, the coordinator uses deterministic routing heuristics (useful for tests and predictable behavior).
    /// Production hosts typically override this to true in appsettings.json.
    /// </summary>
    public bool EnableCoordinatorLlmPlanning { get; init; }
    public int CoordinatorPlannerMaxOutputTokens { get; init; } = 640;
    /// <summary>
    /// When LLM coordinator JSON is invalid or truncated, retry up to this many total attempts (including the first).
    /// </summary>
    public int CoordinatorPlannerMaxRecoveryAttempts { get; init; } = 3;
    /// <summary>
    /// Max output tokens for coordinator retries after a failed/invalid plan (keeps JSON compact on small contexts).
    /// </summary>
    public int CoordinatorPlannerRecoveryMaxOutputTokens { get; init; } = 384;
    /// <summary>
    /// Max characters for conversation tail embedded in coordinator user prompts on recovery attempts.
    /// </summary>
    public int CoordinatorPlannerRecoveryMaxTailChars { get; init; } = 1800;
    public int SpecialistMaxToolIterations { get; init; } = 5;
    /// <summary>
    /// Upper bound on max_tokens for specialist tool-call turns (choosing arguments / tool_calls).
    /// Keeps small-context hosts from reserving most of the window for completion.
    /// </summary>
    public int SpecialistToolCallMaxOutputTokens { get; init; } = 512;
    public int SpecialistSynthesisMaxOutputTokens { get; init; } = 900;
    /// <summary>
    /// Minimum max_tokens target for specialist tool-call turns (before capping by <see cref="SpecialistToolCallMaxOutputTokens"/>).
    /// </summary>
    public int ToolTurnMinOutputTokens { get; init; } = 512;
    /// <summary>
    /// Reasoning headroom reserved only when sizing tool-call turns (keeps reasoning-heavy models from starving tool_calls JSON).
    /// </summary>
    public int ToolTurnReasoningReserveTokens { get; init; } = 128;
    /// <summary>
    /// Minimum safe max_tokens for tool-enabled model calls; below this the worker must recover context or fail without calling.
    /// </summary>
    public int ToolTurnSafeMinMaxOutputTokens { get; init; } = 128;
    /// <summary>
    /// Max passes of the generic context-budget recovery ladder (compact → scratch → narrow tools) per specialist loop.
    /// </summary>
    public int SpecialistContextBudgetRecoveryMaxPasses { get; init; } = 8;
    /// <summary>
    /// When true, recovery turns may set tool_choice=required (OpenAI-compatible) so the model must emit a tool call.
    /// Disable for backends that reject <c>required</c>.
    /// </summary>
    public bool SpecialistRecoveryPreferToolChoiceRequired { get; init; } = true;
    /// <summary>
    /// When false, backends that reject <c>tool_choice=required</c> will not receive forced tool calls on recovery turns.
    /// </summary>
    public bool SpecialistProviderSupportsToolChoiceRequired { get; init; } = true;
    /// <summary>
    /// When true, each specialist tool-enabled turn first runs a compact JSON planner (no tool schemas) to shrink the exposed tool surface.
    /// </summary>
    public bool EnableSpecialistToolSelectionPlanning { get; init; } = true;
    /// <summary>
    /// Max output tokens for the specialist tool-selection planner (JSON only).
    /// </summary>
    public int SpecialistToolSelectionPlannerMaxOutputTokens { get; init; } = 320;
    /// <summary>
    /// Max output tokens for planner retries after invalid or truncated JSON.
    /// </summary>
    public int SpecialistToolSelectionPlannerRecoveryMaxOutputTokens { get; init; } = 200;
    /// <summary>
    /// Total planner attempts including the first when JSON is invalid, truncated, or contractually unusable.
    /// </summary>
    public int SpecialistToolSelectionPlannerMaxRecoveryAttempts { get; init; } = 2;
    /// <summary>
    /// Extra user turns asking for dataset query evidence when only structural tools ran but the dispatch expects metrics.
    /// </summary>
    public int SpecialistMaxStructuralEvidenceRecoveryUserTurns { get; init; }
    public SpecialistAgentOptions Discovery { get; init; } = new();
    public SpecialistAgentOptions QueryAnalysis { get; init; } = new();
    public SpecialistAgentOptions FailureAnalysis { get; init; } = new();
    public SpecialistAgentOptions Reporting { get; init; } = new();
    public string CoordinatorSystemPromptExtension { get; init; } = string.Empty;
    public string FinalComposerSystemPromptExtension { get; init; } = string.Empty;
}

public sealed record AgentOptions
{
    public string Provider { get; init; } = "LMStudio";
    public string BaseUrl { get; init; } = "http://127.0.0.1:1234/v1";
    public string Model { get; init; } = string.Empty;
    public string ApiKey { get; init; } = string.Empty;
    public double Temperature { get; init; } = 0.1;
    public int MaxOutputTokens { get; init; } = 4096;
    /// <summary>
    /// Minimum value passed as max_tokens for assistant completions when sizing against host context.
    /// Helps models that emit long reasoning_content (e.g. Gemma in LM Studio) still leave room for user-visible content.
    /// </summary>
    public int MinAssistantCompletionTokens { get; init; } = 448;
    public int MaxToolIterations { get; init; } = 6;
    public string SystemPrompt { get; init; } = string.Empty;
    public int MaxContinuationRounds { get; init; } = 8;
    public int MaxConversationMessages { get; init; } = 40;
    public bool EnableContextCompaction { get; init; } = true;
    public bool EnableWorkerPasses { get; init; } = false;
    /// <summary>
    /// When true, runs the structured memory worker LLM pass after the final user-visible assistant message.
    /// When false (default), memory is refreshed only after tool execution rounds, reducing latency and cost.
    /// </summary>
    public bool EnableMemoryWorkerAfterFinalAnswer { get; init; }
    public int MaxWorkerPasses { get; init; } = 3;
    public int MemorySummaryMaxLength { get; init; } = 6000;
    /// <summary>Maximum characters stored per tool digest in <see cref="AgentConversationMemory.ToolEvidenceDigests"/>.</summary>
    public int MemoryEvidenceDigestMaxChars { get; init; } = 320;
    /// <summary>Maximum number of tool digest entries kept in memory.</summary>
    public int MemoryToolEvidenceMaxDigests { get; init; } = 6;
    public int CompactionKeepRecentMessages { get; init; } = 12;
    public double WorkerTemperature { get; init; } = 0.2;
    public int WorkerMaxOutputTokens { get; init; } = 900;
    /// <summary>
    /// Reserved headroom subtracted from the effective host context when sizing worker prompts (tokens, heuristic).
    /// </summary>
    public int WorkerPromptReserveTokens { get; init; } = 320;
    /// <summary>
    /// Hard cap on the host model context (e.g. LM Studio n_ctx). When 0, <see cref="ContextSlotTokens"/> is used.
    /// Set this to match the loaded model context to avoid 400 errors from oversized prompts.
    /// </summary>
    public int HostContextTokens { get; init; }
    public int ContextSlotTokens { get; init; } = 4096;
    public int ContextSafetyMarginTokens { get; init; } = 384;
    public int ReasoningReserveTokens { get; init; } = 768;
    /// <summary>Chars per token heuristic; lower values budget prompts more conservatively.</summary>
    public int ContextBudgetCharsPerToken { get; init; } = 3;
    public bool EnableTokenBudgetCompaction { get; init; } = true;
    public bool EnableDynamicToolScoping { get; init; } = false;
    public bool EnableToolPlannerPass { get; init; } = false;
    public int ToolPlannerMaxOutputTokens { get; init; } = 640;
    public int MaxToolEvidenceContentChars { get; init; } = 2400;
    public int ToolPlannerMaxNamedTools { get; init; } = 16;
    public int MaxToolSchemaCharsPerTool { get; init; } = 900;
    public MultiAgentOrchestrationOptions MultiAgent { get; init; } = new();
}
