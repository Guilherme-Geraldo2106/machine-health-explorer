namespace MachineHealthExplorer.Agent.Models;

public sealed record AgentOptions
{
    public string Provider { get; init; } = "LMStudio";
    public string BaseUrl { get; init; } = "http://127.0.0.1:1234/v1";
    public string Model { get; init; } = string.Empty;
    public string ApiKey { get; init; } = string.Empty;
    public double Temperature { get; init; } = 0.1;
    public int MaxOutputTokens { get; init; } = 4096;
    public int MaxToolIterations { get; init; } = 6;
    public string SystemPrompt { get; init; } = string.Empty;
    public int MaxContinuationRounds { get; init; } = 8;
    public int MaxConversationMessages { get; init; } = 40;
    public bool EnableContextCompaction { get; init; } = true;
    public bool EnableWorkerPasses { get; init; } = false;
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
}
