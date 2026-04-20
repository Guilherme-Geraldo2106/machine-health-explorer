using MachineHealthExplorer.Agent.Abstractions;
using MachineHealthExplorer.Agent.Models;
using MachineHealthExplorer.Agent.Services;
using Microsoft.Extensions.Logging;

namespace MachineHealthExplorer.Agent.MultiAgent;

internal sealed class SpecialistToolAgentWorker : IAgentWorker
{
    private const string PseudoToolSyntaxRecoveryMessage =
        "Structured synthesis attempted tool-like syntax, which is invalid when tools are disabled. Return to tool-backed work: emit exactly one valid tool_calls entry on the next tool-enabled turn, or reply with exactly: DONE_NO_MORE_TOOLS";

    private const string CompactDoneNoMoreToolsToken = "DONE_NO_MORE_TOOLS";

    private const string ContextBudgetToolCallFailureMessage =
        "Context budget exhausted before a valid tool call could be produced.";

    private readonly AgentOptions _options;
    private readonly IAgentToolRuntime _toolRuntime;
    private readonly IAgentChatClient _chatClient;
    private readonly ILogger _logger;

    public SpecialistToolAgentWorker(
        AgentOptions options,
        IAgentToolRuntime toolRuntime,
        IAgentChatClient chatClient,
        ILogger logger)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _toolRuntime = toolRuntime ?? throw new ArgumentNullException(nameof(toolRuntime));
        _chatClient = chatClient ?? throw new ArgumentNullException(nameof(chatClient));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public Task<AgentTaskResult> ExecuteAsync(AgentTaskRequest request, CancellationToken cancellationToken = default)
        => ExecuteCoreAsync(request, cancellationToken);

    private async Task<AgentTaskResult> ExecuteCoreAsync(AgentTaskRequest request, CancellationToken cancellationToken)
    {
        var kind = request.SpecialistKind;
        string[] allowedNames = request.AllowedTools.Count > 0
            ? request.AllowedTools.Select(tool => tool.Name).Distinct(StringComparer.OrdinalIgnoreCase).ToArray()
            : SpecialistToolAllowlists.ForSpecialist(kind).ToArray();
        var scopedRuntime = new FilteredAgentToolRuntime(_toolRuntime, allowedNames, kind.ToString());
        var scopedTools = scopedRuntime.GetTools();

        _logger.LogInformation(
            "Specialist {Specialist} start: allowed_tools=[{Tools}] dispatch_reason={Reason}",
            kind,
            string.Join(", ", allowedNames),
            AgentPromptBudgetGuard.CompactPlain(request.DispatchReason, 400));

        var conversation = new List<AgentConversationMessage>
        {
            new()
            {
                Role = AgentConversationRole.User,
                Content = BuildSpecialistUserMessage(request)
            }
        };

        var executedTools = new List<AgentToolExecutionRecord>();
        var model = request.Model;
        var useMinimalToolSchemas = !request.UseFullToolSchemas;
        var contextLengthRecoveryAttempts = 0;
        var injectedMinimalContractAfterContextFallback = false;
        var structuralEvidenceRecoveriesRemaining = Math.Max(0, _options.MultiAgent.SpecialistMaxStructuralEvidenceRecoveryUserTurns);
        var encounteredTruncationWithoutTools = false;
        var scratchRecoveryApplied = false;
        var evidenceSurfaceNarrowed = false;
        var requireToolCallNextTurn = false;
        AgentTokenUsage? lastToolTurnUsage = null;

        if (!request.UseFullToolSchemas)
        {
            conversation.Add(new AgentConversationMessage
            {
                Role = AgentConversationRole.User,
                Content = MultiAgentPromptBuilder.BuildMinimalToolParametersContractHint()
            });
        }

        var safeFloor = Math.Clamp(_options.MultiAgent.ToolTurnSafeMinMaxOutputTokens, 96, 8192);
        var maxIterations = Math.Max(1, _options.MultiAgent.SpecialistMaxToolIterations);
        for (var iteration = 0; iteration < maxIterations; iteration++)
        {
            var estimatedPrompt = AgentContextBudgetEstimator.EstimatePromptTokens(
                _options,
                request.SpecialistSystemPrompt,
                conversation,
                scopedTools);
            var toolTurnMaxOut = ComputeToolCallMaxOutputTokens(estimatedPrompt, request, lastToolTurnUsage);

            if (scopedTools.Count > 0 && toolTurnMaxOut < safeFloor)
            {
                if (!TryElevateToolTurnMaxOutputTokens(
                        ref conversation,
                        ref allowedNames,
                        ref scopedRuntime,
                        ref scopedTools,
                        request,
                        executedTools,
                        useMinimalToolSchemas,
                        kind,
                        ref scratchRecoveryApplied,
                        ref evidenceSurfaceNarrowed,
                        ref requireToolCallNextTurn,
                        lastToolTurnUsage,
                        safeFloor))
                {
                    var budgetExhaustedOutput = SpecialistStructuredOutputParser.FromToolFallback(
                        kind,
                        executedTools,
                        digestMaxChars: Math.Clamp(_options.MemoryEvidenceDigestMaxChars, 96, 1200));
                    return new AgentTaskResult(
                        kind,
                        Success: false,
                        FailureMessage: ContextBudgetToolCallFailureMessage,
                        executedTools,
                        budgetExhaustedOutput,
                        conversation.ToArray());
                }

                estimatedPrompt = AgentContextBudgetEstimator.EstimatePromptTokens(
                    _options,
                    request.SpecialistSystemPrompt,
                    conversation,
                    scopedTools);
                toolTurnMaxOut = ComputeToolCallMaxOutputTokens(estimatedPrompt, request, lastToolTurnUsage);
            }

            var maxOut = AgentContextBudgetEstimator.ComputeEffectiveMaxOutputTokens(_options, estimatedPrompt, reasoningPressureSteps: 0, lastUsage: null);
            var remainingToolTurn = AgentContextBudgetEstimator.EstimateRemainingContextTokensForToolTurn(
                _options,
                estimatedPrompt,
                lastToolTurnUsage);

            if (scopedTools.Count > 0 && toolTurnMaxOut < safeFloor)
            {
                var budgetExhaustedOutput = SpecialistStructuredOutputParser.FromToolFallback(
                    kind,
                    executedTools,
                    digestMaxChars: Math.Clamp(_options.MemoryEvidenceDigestMaxChars, 96, 1200));
                return new AgentTaskResult(
                    kind,
                    Success: false,
                    FailureMessage: ContextBudgetToolCallFailureMessage,
                    executedTools,
                    budgetExhaustedOutput,
                    conversation.ToArray());
            }

            if ((toolTurnMaxOut <= 64 || remainingToolTurn <= 48) && executedTools.Count > 0)
            {
                var targetChars = Math.Max(
                    256,
                    AgentToolEvidenceCompressor.ComputeMaxToolEvidenceChars(_options, estimatedPrompt) / 2);
                if (AgentToolEvidenceCompressor.CompactToolMessagesInConversation(conversation, targetChars))
                {
                    iteration--;
                    continue;
                }

                var fallbackOutput = SpecialistStructuredOutputParser.FromToolFallback(
                    kind,
                    executedTools,
                    digestMaxChars: Math.Clamp(_options.MemoryEvidenceDigestMaxChars, 96, 1200));
                return new AgentTaskResult(
                    kind,
                    Success: true,
                    FailureMessage: "Insufficient context budget after tool execution; returning structured tool fallback.",
                    executedTools,
                    fallbackOutput,
                    conversation.ToArray());
            }

            _logger.LogInformation(
                "Specialist {Specialist} turn {Iteration}: prompt_est~{PromptEst} max_out={MaxOut} tool_turn_max_out={ToolTurnMax} minimal_schemas={Minimal} tools_exposed={ToolCount}",
                kind,
                iteration,
                estimatedPrompt,
                maxOut,
                toolTurnMaxOut,
                useMinimalToolSchemas,
                scopedTools.Count);

            var requireToolCallThisTurn = requireToolCallNextTurn && _options.MultiAgent.SpecialistRecoveryPreferToolChoiceRequired;

            AgentModelResponse response;
            try
            {
                response = await _chatClient.CompleteAsync(new AgentModelRequest
                {
                    Model = model,
                    SystemPrompt = request.SpecialistSystemPrompt,
                    Messages = conversation,
                    Tools = scopedTools,
                    Temperature = _options.Temperature,
                    MaxOutputTokens = toolTurnMaxOut,
                    EnableTools = scopedTools.Count > 0,
                    UseMinimalToolSchemas = useMinimalToolSchemas,
                    RequireToolCall = requireToolCallThisTurn
                }, cancellationToken).ConfigureAwait(false);
            }
            catch (AgentModelBackendException ex) when (ex.IsContextLengthExceeded)
            {
                contextLengthRecoveryAttempts++;
                _logger.LogWarning(
                    ex,
                    "Specialist {Specialist} exceeded context on iteration {Iteration} attempt={Attempt} minimal_schemas={Minimal}.",
                    kind,
                    iteration,
                    contextLengthRecoveryAttempts,
                    useMinimalToolSchemas);

                if (!useMinimalToolSchemas)
                {
                    useMinimalToolSchemas = true;
                    if (!injectedMinimalContractAfterContextFallback)
                    {
                        injectedMinimalContractAfterContextFallback = true;
                        conversation.Add(new AgentConversationMessage
                        {
                            Role = AgentConversationRole.User,
                            Content = MultiAgentPromptBuilder.BuildMinimalToolParametersContractHint()
                        });
                    }

                    iteration--;
                    continue;
                }

                var targetChars = Math.Max(
                    256,
                    AgentToolEvidenceCompressor.ComputeMaxToolEvidenceChars(_options, estimatedPrompt) / 3);
                if (contextLengthRecoveryAttempts <= 6
                    && AgentToolEvidenceCompressor.CompactToolMessagesInConversation(conversation, targetChars))
                {
                    iteration--;
                    continue;
                }

                var fallbackOutput = SpecialistStructuredOutputParser.FromToolFallback(
                    kind,
                    executedTools,
                    digestMaxChars: Math.Clamp(_options.MemoryEvidenceDigestMaxChars, 96, 1200));
                return new AgentTaskResult(
                    kind,
                    Success: executedTools.Count > 0,
                    FailureMessage: ex.Message,
                    executedTools,
                    fallbackOutput,
                    conversation.ToArray());
            }

            model = string.IsNullOrWhiteSpace(response.Model) ? model : response.Model;
            response = FilterToolCallsToKnownCatalog(response, scopedTools);
            lastToolTurnUsage = response.Usage;

            if (response.ToolCalls.Count > 0)
            {
                requireToolCallNextTurn = false;
                conversation.Add(new AgentConversationMessage
                {
                    Role = AgentConversationRole.Assistant,
                    Content = AgentVisibleResponseNormalizer.StripInternalAssistantSurface(response.Content),
                    ToolCalls = response.ToolCalls
                });

                foreach (var toolCall in response.ToolCalls)
                {
                    var execution = await scopedRuntime.ExecuteAsync(toolCall.Name, toolCall.ArgumentsJson, cancellationToken).ConfigureAwait(false);
                    executedTools.Add(execution);
                    _logger.LogInformation(
                        "Specialist {Specialist} executed tool {Tool} is_error={IsError}",
                        kind,
                        execution.ToolName,
                        execution.IsError);

                    var evidenceBudget = AgentToolEvidenceCompressor.ComputeMaxToolEvidenceChars(
                        _options,
                        AgentContextBudgetEstimator.EstimatePromptTokens(
                            _options,
                            request.SpecialistSystemPrompt,
                            conversation,
                            scopedTools));
                    var toolMessageContent = execution.IsError
                        ? SpecialistToolFailureFeedback.BuildToolResultJson(toolCall, execution, scopedTools)
                        : AgentToolEvidenceCompressor.BuildToolMessageContent(
                            toolCall.Name,
                            execution.ResultJson,
                            evidenceBudget);

                    conversation.Add(new AgentConversationMessage
                    {
                        Role = AgentConversationRole.Tool,
                        Name = toolCall.Name,
                        ToolCallId = toolCall.Id,
                        Content = toolMessageContent
                    });
                }

                continue;
            }

            var truncatedWithoutTools = AgentFinishReason.IsTruncated(response.FinishReason);
            if (truncatedWithoutTools)
            {
                encounteredTruncationWithoutTools = true;
                conversation.Add(new AgentConversationMessage
                {
                    Role = AgentConversationRole.User,
                    Content = BuildLengthTruncationRecoveryUserMessage(executedTools)
                });
                requireToolCallNextTurn = true;
                continue;
            }

            if (SpecialistDatasetEvidencePolicy.ShouldPromptForMoreDatasetQueryEvidence(request, executedTools)
                && structuralEvidenceRecoveriesRemaining > 0
                && !IsCompactDoneNoMoreToolsSignal(response))
            {
                structuralEvidenceRecoveriesRemaining--;
                AppendAssistantTurnFromModel(conversation, response);
                conversation.Add(new AgentConversationMessage
                {
                    Role = AgentConversationRole.User,
                    Content = BuildDatasetStructuralRecoveryUserContent(request.UseFullToolSchemas)
                });
                requireToolCallNextTurn = true;
                continue;
            }

            AppendAssistantTurnFromModel(conversation, response);

            var insufficientDatasetEvidenceForSynthesis =
                request.ExpectsDatasetQueryEvidence
                && SpecialistDatasetEvidencePolicy.AllowlistOffersDatasetQueryEvidence(request.AllowedTools)
                && !SpecialistDatasetEvidencePolicy.HasSuccessfulDatasetQueryEvidence(executedTools);

            var (synthesisRetryToolLoop, structuredMaybe) = await TrySynthesizeStructuredOutputAsync(
                    kind,
                    model,
                    request,
                    conversation,
                    executedTools,
                    insufficientDatasetEvidenceForSynthesis,
                    cancellationToken)
                .ConfigureAwait(false);

            if (synthesisRetryToolLoop || structuredMaybe is null)
            {
                if (iteration + 1 >= maxIterations)
                {
                    var pseudoFallback = SpecialistStructuredOutputParser.FromToolFallback(
                        kind,
                        executedTools,
                        digestMaxChars: Math.Clamp(_options.MemoryEvidenceDigestMaxChars, 96, 1200));
                    return new AgentTaskResult(
                        kind,
                        Success: false,
                        FailureMessage:
                        "Specialist synthesis emitted tool-like syntax while tools were disabled, and iteration budget is exhausted.",
                        executedTools,
                        pseudoFallback,
                        conversation.ToArray());
                }

                conversation.Add(new AgentConversationMessage
                {
                    Role = AgentConversationRole.User,
                    Content = PseudoToolSyntaxRecoveryMessage
                });
                continue;
            }

            var structured = structuredMaybe;

            _logger.LogInformation(
                "Specialist {Specialist} completed: tools_used={ToolCount} evidences={EvidenceCount}",
                kind,
                executedTools.Count,
                structured.Evidences.Count);

            return new AgentTaskResult(
                kind,
                Success: true,
                FailureMessage: null,
                executedTools,
                structured,
                conversation.ToArray());
        }

        var exhaustedFailure = encounteredTruncationWithoutTools
            ? "Specialist tool turn was repeatedly truncated before producing required tool evidence."
            : "Specialist tool iteration budget exhausted before producing structured output.";

        var exhaustedOutput = SpecialistStructuredOutputParser.FromToolFallback(
            kind,
            executedTools,
            digestMaxChars: Math.Clamp(_options.MemoryEvidenceDigestMaxChars, 96, 1200));

        return new AgentTaskResult(
            kind,
            Success: executedTools.Count > 0,
            FailureMessage: exhaustedFailure,
            executedTools,
            exhaustedOutput,
            conversation.ToArray());
    }

    private bool TryElevateToolTurnMaxOutputTokens(
        ref List<AgentConversationMessage> conversation,
        ref string[] allowedNames,
        ref FilteredAgentToolRuntime scopedRuntime,
        ref IReadOnlyList<AgentToolDefinition> scopedTools,
        AgentTaskRequest request,
        List<AgentToolExecutionRecord> executedTools,
        bool useMinimalToolSchemas,
        AgentSpecialistKind kind,
        ref bool scratchRecoveryApplied,
        ref bool evidenceSurfaceNarrowed,
        ref bool requireToolCallNextTurn,
        AgentTokenUsage? lastToolTurnUsage,
        int safeFloor)
    {
        var maxPasses = Math.Max(1, _options.MultiAgent.SpecialistContextBudgetRecoveryMaxPasses);
        for (var pass = 0; pass < maxPasses; pass++)
        {
            var estimatedPrompt = AgentContextBudgetEstimator.EstimatePromptTokens(
                _options,
                request.SpecialistSystemPrompt,
                conversation,
                scopedTools);
            var toolTurnMaxOut = ComputeToolCallMaxOutputTokens(estimatedPrompt, request, lastToolTurnUsage);
            if (scopedTools.Count == 0 || toolTurnMaxOut >= safeFloor)
            {
                return true;
            }

            var progressed = false;

            var targetChars = Math.Max(
                96,
                AgentToolEvidenceCompressor.ComputeMaxToolEvidenceChars(_options, estimatedPrompt) / (1 + pass));
            if (AgentToolEvidenceCompressor.CompactToolMessagesInConversation(conversation, targetChars))
            {
                progressed = true;
                estimatedPrompt = AgentContextBudgetEstimator.EstimatePromptTokens(
                    _options,
                    request.SpecialistSystemPrompt,
                    conversation,
                    scopedTools);
                toolTurnMaxOut = ComputeToolCallMaxOutputTokens(estimatedPrompt, request, lastToolTurnUsage);
                if (toolTurnMaxOut >= safeFloor)
                {
                    return true;
                }
            }

            if (!scratchRecoveryApplied)
            {
                conversation = SpecialistToolTurnBudgetRecovery.BuildRecoveryScratchConversation(
                    BuildSpecialistUserMessage(request),
                    executedTools,
                    useMinimalToolSchemas);
                scratchRecoveryApplied = true;
                requireToolCallNextTurn = true;
                progressed = true;
                estimatedPrompt = AgentContextBudgetEstimator.EstimatePromptTokens(
                    _options,
                    request.SpecialistSystemPrompt,
                    conversation,
                    scopedTools);
                toolTurnMaxOut = ComputeToolCallMaxOutputTokens(estimatedPrompt, request, lastToolTurnUsage);
                if (toolTurnMaxOut >= safeFloor)
                {
                    return true;
                }
            }

            if (!evidenceSurfaceNarrowed
                && SpecialistToolTurnBudgetRecovery.ShouldNarrowToEvidenceContinuationSurface(request, executedTools))
            {
                var narrowed = SpecialistToolTurnBudgetRecovery.IntersectAllowedWithEvidenceContinuation(allowedNames);
                if (narrowed.Length < allowedNames.Length)
                {
                    allowedNames = narrowed;
                    scopedRuntime = new FilteredAgentToolRuntime(_toolRuntime, allowedNames, kind.ToString());
                    scopedTools = scopedRuntime.GetTools();
                    evidenceSurfaceNarrowed = true;
                    requireToolCallNextTurn = true;
                    progressed = true;
                    estimatedPrompt = AgentContextBudgetEstimator.EstimatePromptTokens(
                        _options,
                        request.SpecialistSystemPrompt,
                        conversation,
                        scopedTools);
                    toolTurnMaxOut = ComputeToolCallMaxOutputTokens(estimatedPrompt, request, lastToolTurnUsage);
                    if (toolTurnMaxOut >= safeFloor)
                    {
                        return true;
                    }
                }
            }

            if (!progressed)
            {
                break;
            }
        }

        var finalEstimated = AgentContextBudgetEstimator.EstimatePromptTokens(
            _options,
            request.SpecialistSystemPrompt,
            conversation,
            scopedTools);
        var finalOut = ComputeToolCallMaxOutputTokens(finalEstimated, request, lastToolTurnUsage);
        return scopedTools.Count == 0 || finalOut >= safeFloor;
    }

    private static string BuildLengthTruncationRecoveryUserMessage(IReadOnlyList<AgentToolExecutionRecord> executedTools)
    {
        return $"""
The previous tool-enabled assistant turn was truncated before emitting a tool call. Do not paste or replay prior hidden reasoning.

Executed tools so far (compact): {SpecialistToolTurnBudgetRecovery.BuildExecutedToolsSummary(executedTools)}

Continue now: emit exactly one valid tool call if more data is needed, or reply with exactly this single line and nothing else: DONE_NO_MORE_TOOLS
""";
    }

    private static string BuildDatasetStructuralRecoveryUserContent(bool useFullToolSchemas)
    {
        var preamble = """
You only have structural metadata (schema/column search) in the transcript. If a downstream answer still needs aggregates, profiles, distinct values, row samples, or similar dataset-backed evidence, emit exactly one valid tool call now. If no further tools are required, reply with exactly this single line and nothing else: DONE_NO_MORE_TOOLS

""";
        var contract = useFullToolSchemas
            ? MultiAgentPromptBuilder.BuildGroupAndAggregateCompactContractHint()
            : "Follow the earlier reduced-schema user message for exact group_and_aggregate shapes (groupByBins as an array of { columnName, alias, binWidth }; aggregations with Count; conditional counts via per-aggregation filter).";
        return preamble + contract;
    }

    private async Task<(bool RetryToolLoop, AgentStructuredSpecialistOutput? Output)> TrySynthesizeStructuredOutputAsync(
        AgentSpecialistKind kind,
        string model,
        AgentTaskRequest request,
        List<AgentConversationMessage> conversation,
        IReadOnlyList<AgentToolExecutionRecord> toolExecutions,
        bool insufficientDatasetEvidenceForSynthesis,
        CancellationToken cancellationToken)
    {
        var synthesisSystem = BuildSynthesisSystemPrompt(kind, request.SpecialistSystemPrompt);
        var scratch = new List<AgentConversationMessage>(conversation)
        {
            new()
            {
                Role = AgentConversationRole.User,
                Content = BuildSynthesisUserPrompt(kind, insufficientDatasetEvidenceForSynthesis)
            }
        };

        var estimatedPrompt = AgentContextBudgetEstimator.EstimatePromptTokens(_options, synthesisSystem, scratch, Array.Empty<AgentToolDefinition>());
        var maxOut = AgentContextBudgetEstimator.ComputeEffectiveMaxOutputTokens(_options, estimatedPrompt, reasoningPressureSteps: 0, lastUsage: null);
        maxOut = Math.Min(maxOut, Math.Max(256, _options.MultiAgent.SpecialistSynthesisMaxOutputTokens));

        AgentModelResponse response;
        try
        {
            response = await _chatClient.CompleteAsync(new AgentModelRequest
            {
                Model = model,
                SystemPrompt = synthesisSystem,
                Messages = scratch,
                Tools = Array.Empty<AgentToolDefinition>(),
                Temperature = Math.Min(0.25, _options.WorkerTemperature),
                MaxOutputTokens = maxOut,
                EnableTools = false
            }, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogWarning(ex, "Specialist {Specialist} structured synthesis failed; using tool fallbacks.", kind);
            return (
                RetryToolLoop: false,
                SpecialistStructuredOutputParser.FromToolFallback(
                    kind,
                    toolExecutions,
                    digestMaxChars: Math.Clamp(_options.MemoryEvidenceDigestMaxChars, 96, 1200)));
        }

        var surface = AgentToolScopePlanner.CombinePlannerSurface(response.Content, response.ReasoningContent);
        if (AgentPseudoToolCallExtractor.ContentContainsPseudoToolCallArtifacts(surface))
        {
            _logger.LogWarning(
                "Specialist {Specialist} synthesis contained pseudo tool-call syntax; requesting tool-enabled recovery.",
                kind);
            return (RetryToolLoop: true, Output: (AgentStructuredSpecialistOutput?)null);
        }

        var parsed = SpecialistStructuredOutputParser.TryParse(kind, surface);
        if (parsed is not null)
        {
            return (RetryToolLoop: false, parsed);
        }

        return (
            RetryToolLoop: false,
            SpecialistStructuredOutputParser.FromToolFallback(
                kind,
                toolExecutions,
                digestMaxChars: Math.Clamp(_options.MemoryEvidenceDigestMaxChars, 96, 1200)));
    }

    private static void AppendAssistantTurnFromModel(List<AgentConversationMessage> conversation, AgentModelResponse response)
    {
        var surface = AgentToolScopePlanner.CombinePlannerSurface(response.Content, response.ReasoningContent);
        var stripped = AgentVisibleResponseNormalizer.StripInternalAssistantSurface(surface);
        if (string.IsNullOrWhiteSpace(stripped) && string.IsNullOrWhiteSpace(surface))
        {
            return;
        }

        var content = string.IsNullOrWhiteSpace(stripped) ? surface.Trim() : stripped;
        conversation.Add(new AgentConversationMessage
        {
            Role = AgentConversationRole.Assistant,
            Content = content
        });
    }

    private static bool IsCompactDoneNoMoreToolsSignal(AgentModelResponse response)
    {
        var surface = AgentToolScopePlanner.CombinePlannerSurface(response.Content, response.ReasoningContent);
        var stripped = AgentVisibleResponseNormalizer.StripInternalAssistantSurface(surface)?.Trim();
        return stripped is not null
               && stripped.Equals(CompactDoneNoMoreToolsToken, StringComparison.OrdinalIgnoreCase);
    }

    private static string BuildSpecialistUserMessage(AgentTaskRequest request)
    {
        var memory = AgentEphemeralWorkerRunner.FormatMemoryBlock(request.CoordinatorMemory);
        var tailFull = RenderTail(request.MinimalTranscriptTail, maxChars: 2400);
        return $"""
Primary question:
{request.UserQuestion}

Coordinator dispatch reason:
{request.DispatchReason}

Coordinator memory (may be empty):
{memory}

Recent user/assistant tail (may be empty):
{tailFull}

Instructions:
- Use only the allowed tools exposed to you.
- Prefer concise tool arguments; avoid exploratory tool spam.
- Do not write the final user-facing answer; produce tool-backed evidence for a downstream composer.
- On tool-enabled turns, emit the next tool_calls entry immediately; reserve longer explanation for the synthesis pass without tools.
""";
    }

    private int ComputeToolCallMaxOutputTokens(int estimatedPromptTokens, AgentTaskRequest request, AgentTokenUsage? lastUsage)
    {
        var effective = AgentContextBudgetEstimator.ComputeToolTurnEffectiveMaxOutputTokens(
            _options,
            estimatedPromptTokens,
            lastUsage: lastUsage);
        var cap = Math.Max(128, _options.MultiAgent.SpecialistToolCallMaxOutputTokens);
        var merged = Math.Min(effective, cap);
        if (request.ToolTurnMaxOutputTokensCap is { } hardCap)
        {
            merged = Math.Min(merged, Math.Max(96, hardCap));
        }

        return merged;
    }

    private static string BuildSynthesisSystemPrompt(AgentSpecialistKind kind, string specialistSystemPrompt)
    {
        const string jsonShape = """
{
  "relevantColumns": ["string"],
  "ambiguities": ["string"],
  "evidences": [{"sourceTool":"string","summary":"string","supportingJsonFragment":"string|null"}],
  "keyMetrics": {},
  "objectiveObservations": ["string"],
  "hypothesesOrCaveats": ["string"],
  "reportSections": ["string"],
  "analystNotes": "string"
}
""";

        return $"""
You are a structured synthesis worker for specialist={kind}.
You must return ONLY JSON (no markdown fences) with this shape:
{jsonShape.Trim()}

Rules:
- keyMetrics must be an object whose values are numbers only (omit unknowns).
- Do not invent dataset values; ground summaries in the transcript tool outputs.
- Keep lists short and high-signal.
- Keep each supportingJsonFragment under 600 characters when possible.
- Never emit tool_calls, pseudo tool-call markers, or bare call: directives with JSON argument objects.

Specialist policy context (for grounding):
{AgentPromptBudgetGuard.CompactPlain(specialistSystemPrompt, 2400)}
""";
    }

    private static string BuildSynthesisUserPrompt(AgentSpecialistKind kind, bool insufficientDatasetEvidenceForSynthesis)
    {
        var evidenceNote = insufficientDatasetEvidenceForSynthesis
            ? """

Important: The tool transcript lacks successful profile/aggregate/row-sample/distinct outputs. Record that evidence gap explicitly in ambiguities (and hypothesesOrCaveats if needed); do not imply numeric results were computed from those missing tools.
"""
            : string.Empty;

        return $"""
Return the JSON object now for specialist={kind}.
If tool outputs were partial or conflicting, record that in ambiguities and hypothesesOrCaveats.{evidenceNote}
""";
    }

    private static string RenderTail(IReadOnlyList<AgentConversationMessage> tail, int maxChars)
    {
        var builder = new System.Text.StringBuilder();
        foreach (var message in tail.TakeLast(8))
        {
            if (message.Role is AgentConversationRole.User or AgentConversationRole.Assistant)
            {
                builder.AppendLine($"{message.Role}: {message.Content}");
            }

            if (builder.Length >= maxChars)
            {
                break;
            }
        }

        var text = builder.ToString();
        return text.Length <= maxChars ? text : string.Concat(text.AsSpan(0, maxChars), "…");
    }

    private static AgentModelResponse FilterToolCallsToKnownCatalog(
        AgentModelResponse response,
        IReadOnlyList<AgentToolDefinition> catalogTools)
    {
        if (response.ToolCalls.Count == 0)
        {
            return response;
        }

        var known = new HashSet<string>(catalogTools.Select(tool => tool.Name), StringComparer.OrdinalIgnoreCase);
        var filtered = response.ToolCalls.Where(call => known.Contains(call.Name)).ToArray();
        if (filtered.Length == response.ToolCalls.Count)
        {
            return response;
        }

        return response with { ToolCalls = filtered };
    }
}
