using System.Diagnostics;
using MachineHealthExplorer.Agent.Abstractions;
using MachineHealthExplorer.Agent.Models;
using MachineHealthExplorer.Agent.Serialization;
using MachineHealthExplorer.Agent.Services;
using MachineHealthExplorer.Logging.Abstractions;
using MachineHealthExplorer.Logging.Models;
using MachineHealthExplorer.Logging.Services;
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
    private readonly IChatSessionLogger _chatSessionLogger;
    private readonly SpecialistToolSelectionPlanner _specialistToolSelectionPlanner;

    public SpecialistToolAgentWorker(
        AgentOptions options,
        IAgentToolRuntime toolRuntime,
        IAgentChatClient chatClient,
        ILogger logger,
        IChatSessionLogger? chatSessionLogger = null)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _toolRuntime = toolRuntime ?? throw new ArgumentNullException(nameof(toolRuntime));
        _chatClient = chatClient ?? throw new ArgumentNullException(nameof(chatClient));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _chatSessionLogger = chatSessionLogger ?? NullChatSessionLogger.Instance;
        _specialistToolSelectionPlanner = new SpecialistToolSelectionPlanner(options, chatClient, logger);
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
        var injectedMinimalContractAfterPlannerFallback = false;
        var requireToolCallNextTurn = false;
        AgentTokenUsage? lastToolTurnUsage = null;
        var plannerSuppressedForLengthRecovery = false;
        IReadOnlyList<AgentToolDefinition>? recoveryToolSurface = null;
        var toolTurnLengthRecoveriesAttempted = 0;
        var lengthRecoveryWaveExtended = false;

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
        var lengthRecoveryBonusSlots = Math.Max(0, _options.MultiAgent.SpecialistLengthRecoveryBonusIterationSlots);
        var lengthRecoveryBonusGranted = 0;
        for (var iteration = 0;
             iteration < maxIterations + lengthRecoveryBonusGranted && iteration < maxIterations + lengthRecoveryBonusSlots;
             iteration++)
        {
            var lengthRecoveryPlannerRound = plannerSuppressedForLengthRecovery;
            var missingEvidenceKinds = SpecialistDatasetEvidencePolicy.GetMissingRequiredEvidenceKinds(request, executedTools);
            var explicitEvidencePlan = request.RequiredEvidenceKinds is { Count: > 0 };
            var narrowedCatalog = explicitEvidencePlan
                ? SpecialistDatasetEvidencePolicy.FilterToolsSatisfyingAnyKind(scopedTools, missingEvidenceKinds, executedTools)
                : scopedTools;
            var toolsForTurn = explicitEvidencePlan
                               && narrowedCatalog.Count > 0
                               && narrowedCatalog.Count < scopedTools.Count
                               && missingEvidenceKinds.Count > 0
                ? narrowedCatalog
                : scopedTools;

            var catalogToolNames = string.Join(", ", toolsForTurn.Select(t => t.Name));
            var toolsExposedForModelTurn = toolsForTurn;
            if (plannerSuppressedForLengthRecovery && recoveryToolSurface is { Count: > 0 })
            {
                toolsExposedForModelTurn = recoveryToolSurface;
            }

            var useMinimalForToolCallRequest = useMinimalToolSchemas;
            var plannerUsedSuccessfulSubset = false;
            string? plannerFailureDetail = null;

            var promptEstBeforeToolSelection = AgentContextBudgetEstimator.EstimatePromptTokens(
                _options,
                request.SpecialistSystemPrompt,
                conversation,
                scopedTools);

            var skipToolSelectionPlanner =
                requireToolCallNextTurn
                || plannerSuppressedForLengthRecovery
                || ShouldOmitPlannerDueToSmallCatalogAndBudget(
                    request,
                    conversation,
                    toolsForTurn,
                    safeFloor,
                    lastToolTurnUsage,
                    hasExecutedAnyTool: executedTools.Count > 0);

            if (scopedTools.Count > 0 && _options.MultiAgent.EnableSpecialistToolSelectionPlanning && !skipToolSelectionPlanner)
            {
                var memoryCompact = AgentEphemeralWorkerRunner.FormatMemoryBlock(request.CoordinatorMemory);
                var executedSummary = SpecialistToolTurnBudgetRecovery.BuildExecutedToolsSummary(executedTools);
                var compactCatalog = toolsForTurn.Select(t => (t.Name, t.Description)).ToArray();

                var plannerOutcome = await _specialistToolSelectionPlanner.SelectToolsForNextStepAsync(
                        model,
                        kind,
                        request.UserQuestion,
                        request.DispatchReason,
                        memoryCompact,
                        executedSummary,
                        iteration,
                        hasExecutedAnyTool: executedTools.Count > 0,
                        compactCatalog,
                        cancellationToken)
                    .ConfigureAwait(false);

                if (plannerOutcome.Status == SpecialistToolSelectionPlannerStatus.Success && !plannerOutcome.NeedTools)
                {
                    _logger.LogInformation(
                        "Specialist {Specialist} iteration={Iteration} tool_selection_planner need_tools=false reason={Reason} catalog_tools={CatalogTools}",
                        kind,
                        iteration,
                        AgentPromptBudgetGuard.CompactPlain(plannerOutcome.Reason ?? string.Empty, 220),
                        catalogToolNames);

                    if (missingEvidenceKinds.Count > 0
                        && structuralEvidenceRecoveriesRemaining > 0)
                    {
                        structuralEvidenceRecoveriesRemaining--;
                        conversation.Add(new AgentConversationMessage
                        {
                            Role = AgentConversationRole.User,
                            Content = BuildEvidenceKindsModelDrivenRecoveryUserContent(
                                request,
                                executedTools,
                                toolsForTurn.Select(t => t.Name).ToArray(),
                                request.UseFullToolSchemas)
                        });
                        requireToolCallNextTurn = true;
                        continue;
                    }

                    var earlyReturn = await TryReturnAfterStructuredSynthesisAsync(
                            kind,
                            model,
                            request,
                            conversation,
                            executedTools,
                            iteration,
                            maxIterations + lengthRecoveryBonusGranted,
                            cancellationToken)
                        .ConfigureAwait(false);
                    if (earlyReturn is not null)
                    {
                        return earlyReturn;
                    }

                    continue;
                }

                if (plannerOutcome.Status == SpecialistToolSelectionPlannerStatus.Success && plannerOutcome.NeedTools)
                {
                    var mapped = new List<AgentToolDefinition>();
                    foreach (var name in plannerOutcome.ToolNames)
                    {
                        var def = scopedTools.FirstOrDefault(t => t.Name.Equals(name, StringComparison.OrdinalIgnoreCase));
                        if (def is not null)
                        {
                            mapped.Add(def);
                        }
                    }

                    if (mapped.Count > 0)
                    {
                        toolsExposedForModelTurn = mapped;
                        plannerUsedSuccessfulSubset = true;
                    }
                    else
                    {
                        plannerFailureDetail = "planner_tool_names_not_found_in_allowlist";
                    }
                }
                else if (plannerOutcome.Status != SpecialistToolSelectionPlannerStatus.Success
                         || (plannerOutcome.NeedTools && plannerOutcome.ToolNames.Count == 0))
                {
                    plannerFailureDetail = plannerOutcome.ValidationDetail ?? plannerOutcome.Status.ToString();
                }

                if (!plannerUsedSuccessfulSubset)
                {
                    toolsExposedForModelTurn = scopedTools;
                    useMinimalForToolCallRequest = true;
                    if (request.UseFullToolSchemas && !injectedMinimalContractAfterPlannerFallback)
                    {
                        injectedMinimalContractAfterPlannerFallback = true;
                        conversation.Add(new AgentConversationMessage
                        {
                            Role = AgentConversationRole.User,
                            Content = MultiAgentPromptBuilder.BuildMinimalToolParametersContractHint()
                        });
                    }

                    _logger.LogWarning(
                        "Specialist {Specialist} iteration={Iteration} tool_selection_planner_fallback detail={Detail} expose_count={Count} minimal_for_request={Minimal}",
                        kind,
                        iteration,
                        AgentPromptBudgetGuard.CompactPlain(plannerFailureDetail ?? "planner_failed", 300),
                        toolsExposedForModelTurn.Count,
                        useMinimalForToolCallRequest);
                }

                var chosenNames = string.Join(", ", toolsExposedForModelTurn.Select(t => t.Name));
                _logger.LogInformation(
                    "Specialist {Specialist} iteration={Iteration} tool_selection_planner need_tools={NeedTools} reason={Reason} chosen_tools=[{Chosen}] subset={Subset} catalog_tools=[{Catalog}]",
                    kind,
                    iteration,
                    plannerOutcome.NeedTools,
                    AgentPromptBudgetGuard.CompactPlain(plannerOutcome.Reason ?? string.Empty, 220),
                    chosenNames,
                    plannerUsedSuccessfulSubset,
                    catalogToolNames);
            }

            toolsExposedForModelTurn = SpecialistDatasetEvidencePolicy.EnsureStructuralSurfaceWhenSchemaUnsatisfied(
                scopedTools,
                toolsExposedForModelTurn,
                missingEvidenceKinds,
                executedTools);

            var estimatedPrompt = AgentContextBudgetEstimator.EstimatePromptTokens(
                _options,
                request.SpecialistSystemPrompt,
                conversation,
                toolsExposedForModelTurn);
            var toolTurnMaxOut = ComputeToolCallMaxOutputTokens(estimatedPrompt, request, lastToolTurnUsage);

            _logger.LogInformation(
                "Specialist {Specialist} iteration={Iteration} budget prompt_est_before_selection={PromptBefore} prompt_est_after_selection={PromptAfter} tool_turn_max_out={ToolTurnMax}",
                kind,
                iteration,
                promptEstBeforeToolSelection,
                estimatedPrompt,
                toolTurnMaxOut);

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
                        safeFloor,
                        toolsExposedForModelTurn))
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

                scopedTools = scopedRuntime.GetTools();
                if (_options.MultiAgent.EnableSpecialistToolSelectionPlanning)
                {
                    iteration--;
                    continue;
                }

                toolsExposedForModelTurn = scopedTools;
                useMinimalForToolCallRequest = useMinimalToolSchemas;
                estimatedPrompt = AgentContextBudgetEstimator.EstimatePromptTokens(
                    _options,
                    request.SpecialistSystemPrompt,
                    conversation,
                    toolsExposedForModelTurn);
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
                useMinimalForToolCallRequest,
                toolsExposedForModelTurn.Count);

            var recoveryPreferenceOk = _options.MultiAgent.SpecialistRecoveryPreferToolChoiceRequired
                && _options.MultiAgent.SpecialistProviderSupportsToolChoiceRequired;
            var needsDatasetEvidence = missingEvidenceKinds.Count > 0;

            var requireToolCallThisTurn =
                toolsExposedForModelTurn.Count > 0
                && recoveryPreferenceOk
                && (
                    (
                        _options.MultiAgent.EnableSpecialistToolSelectionPlanning
                        && plannerUsedSuccessfulSubset
                        && toolsExposedForModelTurn.Count == 1
                        && (requireToolCallNextTurn || needsDatasetEvidence))
                    || (
                        (!_options.MultiAgent.EnableSpecialistToolSelectionPlanning || !plannerUsedSuccessfulSubset)
                        && requireToolCallNextTurn)
                    || plannerSuppressedForLengthRecovery);

            var disableParallelToolCalls =
                toolsExposedForModelTurn.Count > 0
                && (requireToolCallThisTurn || plannerSuppressedForLengthRecovery || toolsExposedForModelTurn.Count <= 1);

            _logger.LogInformation(
                "Specialist {Specialist} iteration={Iteration} require_tool_call={Require} planner_subset={Subset}",
                kind,
                iteration,
                requireToolCallThisTurn,
                plannerUsedSuccessfulSubset);

            AgentModelResponse response;
            try
            {
                response = await _chatClient.CompleteAsync(new AgentModelRequest
                {
                    Model = model,
                    SystemPrompt = request.SpecialistSystemPrompt,
                    Messages = conversation,
                    Tools = toolsExposedForModelTurn,
                    Temperature = _options.Temperature,
                    MaxOutputTokens = toolTurnMaxOut,
                    EnableTools = toolsExposedForModelTurn.Count > 0,
                    UseMinimalToolSchemas = useMinimalForToolCallRequest,
                    RequireToolCall = requireToolCallThisTurn,
                    ParallelToolCalls = disableParallelToolCalls ? false : null
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
            lastToolTurnUsage = response.Usage;

            var exposedSurfaceNames = new HashSet<string>(
                toolsExposedForModelTurn.Select(t => t.Name),
                StringComparer.OrdinalIgnoreCase);
            var scopedSurfaceNames = new HashSet<string>(
                scopedTools.Select(t => t.Name),
                StringComparer.OrdinalIgnoreCase);
            var allToolCalls = response.ToolCalls.ToArray();
            var scopedCalls = allToolCalls
                .Where(c => scopedSurfaceNames.Contains(c.Name))
                .ToArray();

            if (response.ToolCalls.Count > 0
                && scopedCalls.Length == 0)
            {
                var requestedNames = string.Join(
                    ",",
                    response.ToolCalls.Select(c => c.Name).Distinct(StringComparer.OrdinalIgnoreCase)
                        .OrderBy(n => n, StringComparer.OrdinalIgnoreCase));
                var exposedList = string.Join(
                    ",",
                    toolsExposedForModelTurn.Select(t => t.Name).Distinct(StringComparer.OrdinalIgnoreCase)
                        .OrderBy(n => n, StringComparer.OrdinalIgnoreCase));
                var logPayload = AgentJsonSerializer.Serialize(new
                {
                    requested_tool_calls = response.ToolCalls.Select(c => c.Name).ToArray(),
                    exposed_tools_this_turn = toolsExposedForModelTurn.Select(t => t.Name).ToArray(),
                    specialist_allowed_tools = allowedNames
                });
                _logger.LogWarning(
                    "Specialist {Specialist} iteration={Iteration} tool_calls_not_on_specialist_allowlist payload={Payload}",
                    kind,
                    iteration,
                    AgentPromptBudgetGuard.CompactPlain(logPayload, 2000));
                _chatSessionLogger.Append(new ChatSessionLogEvent(
                    default,
                    string.Empty,
                    "agent.tool_call.rejected_allowlist",
                    "internal",
                    model,
                    response.FinishReason,
                    SummarizeToolCallsForLog(response.ToolCalls),
                    null,
                    null,
                    null,
                    logPayload,
                    null,
                    null));

                conversation.Add(new AgentConversationMessage
                {
                    Role = AgentConversationRole.Assistant,
                    Content = AgentVisibleResponseNormalizer.StripInternalAssistantSurface(response.Content),
                    ToolCalls = allToolCalls
                });

                foreach (var call in allToolCalls)
                {
                    var errorJson = SpecialistToolSurfaceValidation.BuildToolNotOnSpecialistAllowlistResultJson(
                        call.Name,
                        allowedNames,
                        toolsExposedForModelTurn,
                        scopedTools,
                        request.UseFullToolSchemas);
                    conversation.Add(new AgentConversationMessage
                    {
                        Role = AgentConversationRole.Tool,
                        Name = call.Name,
                        ToolCallId = call.Id,
                        Content = errorJson
                    });
                }

                requireToolCallNextTurn = true;
                continue;
            }

            if (scopedCalls.Length > 0)
            {
                requireToolCallNextTurn = false;
                plannerSuppressedForLengthRecovery = false;
                recoveryToolSurface = null;
                toolTurnLengthRecoveriesAttempted = 0;
                encounteredTruncationWithoutTools = false;
                if (lengthRecoveryPlannerRound
                    && lengthRecoveryBonusGranted < lengthRecoveryBonusSlots)
                {
                    lengthRecoveryBonusGranted++;
                }

                conversation.Add(new AgentConversationMessage
                {
                    Role = AgentConversationRole.Assistant,
                    Content = AgentVisibleResponseNormalizer.StripInternalAssistantSurface(response.Content),
                    ToolCalls = allToolCalls
                });

                foreach (var toolCall in allToolCalls)
                {
                    var sw = Stopwatch.StartNew();
                    AgentToolExecutionRecord execution;
                    if (!scopedSurfaceNames.Contains(toolCall.Name))
                    {
                        var rejectionPayload = SpecialistToolSurfaceValidation.BuildToolNotOnSpecialistAllowlistResultJson(
                            toolCall.Name,
                            allowedNames,
                            toolsExposedForModelTurn,
                            scopedTools,
                            request.UseFullToolSchemas);
                        _chatSessionLogger.Append(new ChatSessionLogEvent(
                            default,
                            string.Empty,
                            "agent.tool_call.rejected_allowlist",
                            "internal",
                            model,
                            response.FinishReason,
                            SummarizeToolCallsForLog(new[] { toolCall }),
                            null,
                            null,
                            null,
                            rejectionPayload,
                            null,
                            null));

                        execution = new AgentToolExecutionRecord
                        {
                            ToolName = toolCall.Name,
                            ArgumentsJson = toolCall.ArgumentsJson ?? "{}",
                            IsError = true,
                            ResultJson = rejectionPayload
                        };
                    }
                    else if (!exposedSurfaceNames.Contains(toolCall.Name))
                    {
                        _logger.LogWarning(
                            "Specialist {Specialist} iteration={Iteration} tool_call_rejected_not_on_exposed_surface requested={Tool} exposed=[{Exposed}]",
                            kind,
                            iteration,
                            toolCall.Name,
                            string.Join(", ", toolsExposedForModelTurn.Select(t => t.Name)));

                        var rejectionPayload = SpecialistToolSurfaceValidation.BuildOutOfSurfaceToolResultJson(
                            toolCall.Name,
                            allowedNames,
                            toolsExposedForModelTurn,
                            scopedTools,
                            request.UseFullToolSchemas);
                        _chatSessionLogger.Append(new ChatSessionLogEvent(
                            default,
                            string.Empty,
                            "agent.tool_call.rejected",
                            "internal",
                            model,
                            response.FinishReason,
                            SummarizeToolCallsForLog(new[] { toolCall }),
                            null,
                            null,
                            null,
                            rejectionPayload));

                        execution = new AgentToolExecutionRecord
                        {
                            ToolName = toolCall.Name,
                            ArgumentsJson = toolCall.ArgumentsJson ?? "{}",
                            IsError = true,
                            ResultJson = rejectionPayload
                        };
                    }
                    else
                    {
                        execution = await scopedRuntime.ExecuteAsync(
                                toolCall.Name,
                                toolCall.ArgumentsJson ?? "{}",
                                cancellationToken)
                            .ConfigureAwait(false);
                    }

                    sw.Stop();
                    executedTools.Add(execution);
                    var originalLen = (execution.ResultJson ?? string.Empty).Length;
                    var evidenceBudget = AgentToolEvidenceCompressor.ComputeMaxToolEvidenceChars(
                        _options,
                        AgentContextBudgetEstimator.EstimatePromptTokens(
                            _options,
                            request.SpecialistSystemPrompt,
                            conversation,
                            toolsExposedForModelTurn));
                    var preview = execution.IsError
                        ? SpecialistToolFailureFeedback.BuildToolResultJson(toolCall, execution, scopedTools)
                        : AgentToolEvidenceCompressor.BuildToolMessageContent(
                            toolCall.Name,
                            execution.ResultJson ?? "{}",
                            evidenceBudget,
                            toolCall.ArgumentsJson);
                    _logger.LogInformation(
                        "Specialist {Specialist} executed tool {Tool} is_error={IsError} args_len={ArgsLen} result_len={ResLen} preview_len={PrevLen} elapsed_ms={Ms}",
                        kind,
                        execution.ToolName,
                        execution.IsError,
                        (toolCall.ArgumentsJson ?? "{}").Length,
                        originalLen,
                        preview.Length,
                        sw.Elapsed.TotalMilliseconds);

                    _chatSessionLogger.Append(new ChatSessionLogEvent(
                        default,
                        string.Empty,
                        "agent.tool_execution",
                        "internal",
                        model,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        ToolExecution: new ChatToolExecutionLog(
                            execution.ToolName,
                            (toolCall.ArgumentsJson ?? "{}").Length,
                            originalLen,
                            preview.Length,
                            execution.IsError,
                            sw.Elapsed.TotalMilliseconds)));

                    conversation.Add(new AgentConversationMessage
                    {
                        Role = AgentConversationRole.Tool,
                        Name = toolCall.Name,
                        ToolCallId = toolCall.Id,
                        Content = preview
                    });
                }

                continue;
            }

            var truncatedWithoutTools = AgentFinishReason.IsTruncated(response.FinishReason);
            if (truncatedWithoutTools)
            {
                encounteredTruncationWithoutTools = true;
                var maxLengthRecoveries = Math.Max(0, _options.MultiAgent.SpecialistToolTurnLengthRecoveryMaxAttempts);
                var recoveryWaveAfterCeiling = !lengthRecoveryWaveExtended
                    && toolTurnLengthRecoveriesAttempted >= maxLengthRecoveries
                    && ComputeToolCallMaxOutputTokens(estimatedPrompt, request, lastToolTurnUsage) > toolTurnMaxOut + 32
                    && toolsExposedForModelTurn.Count > 0
                    && iteration + 1 < maxIterations + lengthRecoveryBonusGranted;
                if (toolTurnLengthRecoveriesAttempted < maxLengthRecoveries
                    || recoveryWaveAfterCeiling)
                {
                    if (recoveryWaveAfterCeiling)
                    {
                        lengthRecoveryWaveExtended = true;
                        toolTurnLengthRecoveriesAttempted = 0;
                    }
                    else
                    {
                        toolTurnLengthRecoveriesAttempted++;
                    }

                    recoveryToolSurface = toolsExposedForModelTurn;
                    plannerSuppressedForLengthRecovery = true;
                    var targetChars = Math.Max(
                        256,
                        AgentToolEvidenceCompressor.ComputeMaxToolEvidenceChars(
                            _options,
                            AgentContextBudgetEstimator.EstimatePromptTokens(
                                _options,
                                request.SpecialistSystemPrompt,
                                conversation,
                                toolsExposedForModelTurn)) / 2);
                    AgentToolEvidenceCompressor.CompactToolMessagesInConversation(conversation, targetChars);
                    conversation.Add(new AgentConversationMessage
                    {
                        Role = AgentConversationRole.User,
                        Content = BuildLengthTruncationRecoveryUserMessage(executedTools)
                    });
                    requireToolCallNextTurn = true;
                    continue;
                }

                var truncatedOutput = SpecialistStructuredOutputParser.FromToolFallback(
                    kind,
                    executedTools,
                    digestMaxChars: Math.Clamp(_options.MemoryEvidenceDigestMaxChars, 96, 1200));
                return new AgentTaskResult(
                    kind,
                    Success: executedTools.Count > 0,
                    FailureMessage: executedTools.Count > 0
                        ? null
                        : "Rodada com tools truncada (finish_reason=length) sem tool_calls após recuperação direta; orçamento ou modelo não emitiu JSON de tool a tempo.",
                    executedTools,
                    truncatedOutput,
                    conversation.ToArray());
            }

            if (missingEvidenceKinds.Count > 0
                && structuralEvidenceRecoveriesRemaining > 0
                && !IsCompactDoneNoMoreToolsSignal(response))
            {
                structuralEvidenceRecoveriesRemaining--;
                AppendAssistantTurnFromModel(conversation, response);
                conversation.Add(new AgentConversationMessage
                {
                    Role = AgentConversationRole.User,
                    Content = BuildEvidenceKindsModelDrivenRecoveryUserContent(
                        request,
                        executedTools,
                        toolsForTurn.Select(t => t.Name).ToArray(),
                        request.UseFullToolSchemas)
                });
                requireToolCallNextTurn = true;
                continue;
            }

            if (IsCompactDoneNoMoreToolsSignal(response))
            {
                plannerSuppressedForLengthRecovery = false;
                recoveryToolSurface = null;
            }

            AppendAssistantTurnFromModel(conversation, response);

            var insufficientDatasetEvidenceForSynthesis = missingEvidenceKinds.Count > 0;

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
                if (iteration + 1 >= maxIterations + lengthRecoveryBonusGranted)
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
        int safeFloor,
        IReadOnlyList<AgentToolDefinition> initialToolSurfaceForEstimate)
    {
        var surfaceForEstimate = initialToolSurfaceForEstimate;
        var maxPasses = Math.Max(1, _options.MultiAgent.SpecialistContextBudgetRecoveryMaxPasses);
        for (var pass = 0; pass < maxPasses; pass++)
        {
            var estimatedPrompt = AgentContextBudgetEstimator.EstimatePromptTokens(
                _options,
                request.SpecialistSystemPrompt,
                conversation,
                surfaceForEstimate);
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
                    surfaceForEstimate);
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
                    surfaceForEstimate);
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
                    surfaceForEstimate = scopedTools;
                    evidenceSurfaceNarrowed = true;
                    requireToolCallNextTurn = true;
                    progressed = true;
                    estimatedPrompt = AgentContextBudgetEstimator.EstimatePromptTokens(
                        _options,
                        request.SpecialistSystemPrompt,
                        conversation,
                        surfaceForEstimate);
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
            surfaceForEstimate);
        var finalOut = ComputeToolCallMaxOutputTokens(finalEstimated, request, lastToolTurnUsage);
        return scopedTools.Count == 0 || finalOut >= safeFloor;
    }

    private static string BuildLengthTruncationRecoveryUserMessage(IReadOnlyList<AgentToolExecutionRecord> executedTools)
    {
        return $"""
The previous tool-enabled assistant turn was truncated before emitting a tool call. Do not paste or replay prior hidden reasoning.

Executed tools so far (compact): {SpecialistToolTurnBudgetRecovery.BuildExecutedToolsSummary(executedTools)}

Continue now: prefer one compact tool_calls JSON entry with minimal visible prose if more data is needed; otherwise reply with exactly this single line and nothing else: DONE_NO_MORE_TOOLS
""";
    }

    internal static string BuildEvidenceKindsModelDrivenRecoveryUserContent(
        AgentTaskRequest request,
        IReadOnlyList<AgentToolExecutionRecord> executedTools,
        IReadOnlyList<string> availableToolNames,
        bool useFullToolSchemas)
    {
        var missing = SpecialistDatasetEvidencePolicy.GetMissingRequiredEvidenceKinds(request, executedTools);
        var collected = SpecialistDatasetEvidencePolicy.GetSatisfiedEvidenceKinds(executedTools)
            .OrderBy(k => k.ToString(), StringComparer.Ordinal)
            .Select(k => k.ToString())
            .ToArray();

        var requiredLine = missing.Count > 0
            ? string.Join(", ", missing.Select(m => m.ToString()).Distinct())
            : "(none)";

        var collectedLine = collected.Length > 0
            ? string.Join(", ", collected)
            : "(none)";

        var catalogLine = availableToolNames.Count > 0
            ? string.Join(", ", availableToolNames.Distinct(StringComparer.OrdinalIgnoreCase).OrderBy(n => n, StringComparer.OrdinalIgnoreCase))
            : "(none)";

        var contract = useFullToolSchemas
            ? MultiAgentPromptBuilder.BuildGroupAndAggregateCompactContractHint()
            : "Follow the earlier reduced-schema user message for exact group_and_aggregate shapes (groupByBins as an array of { columnName, alias, binWidth }; Count without per-aggregation filter = all rows in the group; Count with filter = subset count; use separate aggregations/aliases such as row_count and event_count when both are needed).";

        return $"""
Evidence recovery (model-driven routing only; do not answer the end user here).

Still required generic evidence categories (not yet satisfied): {requiredLine}
Already collected generic evidence categories: {collectedLine}
Available generic tool names in this turn (names only): {catalogLine}

Emit exactly one valid tool call using the exposed tools, or reply with exactly this single line and nothing else:
DONE_NO_MORE_TOOLS

If you choose DONE_NO_MORE_TOOLS, add a single technical justification line explaining why no further tool is appropriate.

{contract}
""";
    }

    private async Task<AgentTaskResult?> TryReturnAfterStructuredSynthesisAsync(
        AgentSpecialistKind kind,
        string model,
        AgentTaskRequest request,
        List<AgentConversationMessage> conversation,
        List<AgentToolExecutionRecord> executedTools,
        int iteration,
        int effectiveIterationLimit,
        CancellationToken cancellationToken)
    {
        var insufficientDatasetEvidenceForSynthesis =
            SpecialistDatasetEvidencePolicy.GetMissingRequiredEvidenceKinds(request, executedTools).Count > 0;

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
            if (iteration + 1 >= effectiveIterationLimit)
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
            return null;
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
        if (maxOut <= 0)
        {
            _logger.LogWarning(
                "Specialist {Specialist} structured synthesis skipped: insufficient assistant completion budget (max_out=0).",
                kind);
            return (
                RetryToolLoop: false,
                SpecialistStructuredOutputParser.FromToolFallback(
                    kind,
                    toolExecutions,
                    digestMaxChars: Math.Clamp(_options.MemoryEvidenceDigestMaxChars, 96, 1200)));
        }

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
                EnableTools = false,
                ResponseFormat = _options.EnableStructuredJsonOutputs
                    ? new AgentJsonSchemaResponseFormat
                    {
                        Type = "json_schema",
                        Name = AgentStructuredOutputJsonSchemas.SpecialistStructuredSynthesisSchemaName,
                        Strict = _options.UseStrictJsonSchemaInResponseFormat,
                        SchemaJson = AgentStructuredOutputJsonSchemas.SpecialistStructuredSynthesis.Trim()
                    }
                    : null
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

    internal static void AppendAssistantTurnFromModel(List<AgentConversationMessage> conversation, AgentModelResponse response)
    {
        var stripped = AgentVisibleResponseNormalizer.StripInternalAssistantSurface(response.Content);
        if (string.IsNullOrWhiteSpace(stripped) && string.IsNullOrWhiteSpace(response.Content))
        {
            return;
        }

        var content = string.IsNullOrWhiteSpace(stripped) ? (response.Content ?? string.Empty).Trim() : stripped;
        conversation.Add(new AgentConversationMessage
        {
            Role = AgentConversationRole.Assistant,
            Content = content
        });
    }

    private static bool IsCompactDoneNoMoreToolsSignal(AgentModelResponse response)
    {
        var stripped = AgentVisibleResponseNormalizer.StripInternalAssistantSurface(response.Content)?.Trim();
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
        if (effective <= 0)
        {
            return 0;
        }

        var cap = AgentContextBudgetEstimator.ComputeToolTurnDynamicOutputCap(_options, lastUsage);
        var merged = Math.Min(effective, cap);
        if (request.ToolTurnMaxOutputTokensCap is { } hardCap)
        {
            merged = Math.Min(merged, Math.Max(96, hardCap));
        }

        return merged;
    }

    private bool ShouldOmitPlannerDueToSmallCatalogAndBudget(
        AgentTaskRequest request,
        List<AgentConversationMessage> conversation,
        IReadOnlyList<AgentToolDefinition> scopedTools,
        int safeFloor,
        AgentTokenUsage? lastToolTurnUsage,
        bool hasExecutedAnyTool)
    {
        if (hasExecutedAnyTool && request.ExpectsDatasetQueryEvidence)
        {
            return false;
        }

        var maxCatalog = Math.Max(0, _options.MultiAgent.SpecialistToolPlannerSkipWhenCatalogSizeAtMost);
        if (scopedTools.Count == 0 || scopedTools.Count > maxCatalog)
        {
            return false;
        }

        var estimated = AgentContextBudgetEstimator.EstimatePromptTokens(
            _options,
            request.SpecialistSystemPrompt,
            conversation,
            scopedTools);
        var toolTurnMaxOut = ComputeToolCallMaxOutputTokens(estimated, request, lastToolTurnUsage);
        return toolTurnMaxOut >= safeFloor;
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
- For group_and_aggregate, only treat a Count as a conditional/subset tally when the tool request (see aggregationRequestSummary in tool envelopes) shows a per-aggregation filter; unfiltered Count is a group row tally even if the alias suggests events.
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

    private static IReadOnlyList<ChatToolCallLogSummary>? SummarizeToolCallsForLog(IReadOnlyList<AgentToolCall> toolCalls)
    {
        if (toolCalls.Count == 0)
        {
            return null;
        }

        return toolCalls
            .Select(call => new ChatToolCallLogSummary(call.Id, call.Name, (call.ArgumentsJson ?? "{}").Length))
            .Where(summary => !string.IsNullOrWhiteSpace(summary.Name))
            .ToArray();
    }
}
