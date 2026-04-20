using MachineHealthExplorer.Agent.Abstractions;
using MachineHealthExplorer.Agent.Models;
using MachineHealthExplorer.Agent.Services;
using Microsoft.Extensions.Logging;

namespace MachineHealthExplorer.Agent.MultiAgent;

internal sealed class MultiAgentSessionEngine
{
    private readonly AgentOptions _options;
    private readonly IAgentChatClient _chatClient;
    private readonly IAgentToolRuntime _toolRuntime;
    private readonly AgentEphemeralWorkerRunner _workerRunner;
    private readonly AgentAssistantAnswerPipeline _answerPipeline;
    private readonly CoordinatorAgent _coordinator;
    private readonly SpecialistToolAgentWorker _specialistWorker;
    private readonly IFinalResponseComposer _finalComposer;
    private readonly ILogger _logger;
    private string? _resolvedModel;

    public MultiAgentSessionEngine(
        AgentOptions options,
        IAgentChatClient chatClient,
        IAgentToolRuntime toolRuntime,
        ILogger logger)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _chatClient = chatClient ?? throw new ArgumentNullException(nameof(chatClient));
        _toolRuntime = toolRuntime ?? throw new ArgumentNullException(nameof(toolRuntime));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _workerRunner = new AgentEphemeralWorkerRunner(_options, _chatClient);
        _answerPipeline = new AgentAssistantAnswerPipeline(_options, _chatClient, _workerRunner, logger);
        _coordinator = new CoordinatorAgent(_options, _chatClient, logger);
        _specialistWorker = new SpecialistToolAgentWorker(_options, _toolRuntime, _chatClient, logger);
        _finalComposer = new FinalComposerAgent(_options, _chatClient, logger);
    }

    public Task<IReadOnlyList<ToolRegistrationDescriptor>> DescribeToolsAsync(CancellationToken cancellationToken = default)
        => Task.FromResult<IReadOnlyList<ToolRegistrationDescriptor>>(
            _toolRuntime.GetTools()
                .Select(tool => new ToolRegistrationDescriptor
                {
                    Name = tool.Name,
                    Description = tool.Description
                })
                .ToArray());

    public async Task<AgentExecutionResult> RunAsync(AgentExecutionContext context, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(context);

        var availableTools = await DescribeToolsAsync(cancellationToken).ConfigureAwait(false);
        var model = await ResolveModelAsync(cancellationToken).ConfigureAwait(false);

        var memory = context.ConversationMemory ?? new AgentConversationMemory();
        memory = memory with
        {
            CurrentUserIntent = string.IsNullOrWhiteSpace(context.UserInput) ? memory.CurrentUserIntent : context.UserInput.Trim(),
            LanguagePreference = LanguageHeuristics.DetectLanguage(context.UserInput, memory.LanguagePreference),
            LastUpdatedUtc = DateTimeOffset.UtcNow
        };

        var conversation = context.ConversationHistory.ToList();
        conversation.Add(new AgentConversationMessage
        {
            Role = AgentConversationRole.User,
            Content = context.UserInput
        });

        memory = await MaybeCompactConversationAsync(conversation, memory, model, cancellationToken).ConfigureAwait(false);

        var coordinatorSystemForBudget = BuildCoordinatorMemorySystemPrompt(memory);
        memory = await EnforceMainConversationBudgetAsync(
                conversation,
                memory,
                model,
                coordinatorSystemForBudget,
                cancellationToken)
            .ConfigureAwait(false);

        _answerPipeline.ResetTelemetry();

        var planning = await _coordinator.TryPlanAsync(model, context.UserInput.Trim(), memory, conversation, cancellationToken).ConfigureAwait(false);
        if (!planning.Success)
        {
            var failureText = planning.UserVisibleFailureMessage
                ?? "O coordenador multi-agente não conseguiu produzir um plano válido.";
            conversation.Add(new AgentConversationMessage
            {
                Role = AgentConversationRole.Assistant,
                Content = failureText
            });

            return new AgentExecutionResult
            {
                IsImplemented = true,
                Message = failureText,
                Model = model,
                AvailableTools = availableTools,
                UpdatedConversation = conversation.ToArray(),
                ToolExecutions = Array.Empty<AgentToolExecutionRecord>(),
                UpdatedConversationMemory = memory,
                ContinuationExhausted = false,
                MultiAgentTrace = new AgentMultiAgentExecutionTrace(planning.Plan, Array.Empty<AgentSpecialistRunTrace>())
            };
        }

        var plan = planning.Plan;

        var specialistRuns = new List<AgentSpecialistRunTrace>();
        var specialistResults = new List<AgentTaskResult>();
        var executedTools = new List<AgentToolExecutionRecord>();

        var minimalTail = BuildMinimalTranscriptTail(context.ConversationHistory);

        foreach (var parallelGroup in plan.Steps.GroupBy(step => step.ParallelGroup).OrderBy(group => group.Key))
        {
            var orderedSteps = parallelGroup.ToArray();
            var tasks = orderedSteps.Select(step => ExecuteSpecialistSafeAsync(model, context.UserInput.Trim(), memory, minimalTail, step, cancellationToken));
            var results = await Task.WhenAll(tasks).ConfigureAwait(false);

            for (var index = 0; index < results.Length; index++)
            {
                var step = orderedSteps[index];
                var result = results[index];
                specialistResults.Add(result);
                executedTools.AddRange(result.ToolExecutions);
                specialistRuns.Add(new AgentSpecialistRunTrace(
                    result.SpecialistKind,
                    step.Reason,
                    AllowedTools: SpecialistToolAllowlists.ForSpecialist(result.SpecialistKind).ToArray(),
                    ToolsUsed: result.ToolExecutions.Select(tool => tool.ToolName).Distinct(StringComparer.OrdinalIgnoreCase).ToArray(),
                    result.Success,
                    result.FailureMessage));

                memory = memory.WithToolEvidence(
                    result.ToolExecutions.Select(execution => new AgentToolEvidenceDigest
                    {
                        ToolName = $"{result.SpecialistKind}:{execution.ToolName}",
                        Digest = BuildDigest(execution.ResultJson)
                    }),
                    _options.MemoryEvidenceDigestMaxChars,
                    _options.MemoryToolEvidenceMaxDigests);
            }
        }

        // Consolidate memory from specialist tool evidence (structured worker).
        memory = await _workerRunner.RefreshMemoryAsync(
                model,
                memory,
                context.UserInput,
                executedTools,
                conversation.ToArray(),
                cancellationToken)
            .ConfigureAwait(false);

        var schemaColumnHints = SchemaColumnNamesFromToolExecutions.Extract(executedTools);
        var composerInput = new FinalComposerInput(
            OriginalUserQuestion: context.UserInput.Trim(),
            DetectedLanguage: memory.LanguagePreference,
            ConversationRollingSummary: memory.RollingSummary,
            SpecialistResults: specialistResults.ToArray(),
            RecentUserAssistantTail: context.ConversationHistory.TakeLast(8).ToArray(),
            SchemaColumnNamesFromTools: schemaColumnHints);

        var composerTurn = await _finalComposer.ComposeFirstResponseAsync(composerInput, model, memory, cancellationToken).ConfigureAwait(false);
        _answerPipeline.IngestUsage(composerTurn.FirstResponse.Usage);

        var scratch = composerTurn.PrefixMessages.ToList();
        var visibleFirst = await _answerPipeline.EnsureVisibleModelResponseAsync(
                model,
                memory,
                scratch,
                composerTurn.FirstResponse,
                cancellationToken)
            .ConfigureAwait(false);

        var (assistantMessage, continuationExhausted) = await _answerPipeline.CompleteAssistantAnswerAsync(
                model,
                memory,
                scratch,
                visibleFirst,
                composerTurn.SystemPrompt,
                cancellationToken)
            .ConfigureAwait(false);

        conversation.Add(new AgentConversationMessage
        {
            Role = AgentConversationRole.Assistant,
            Content = assistantMessage
        });

        if (_options.EnableMemoryWorkerAfterFinalAnswer)
        {
            memory = await _workerRunner.RefreshMemoryAsync(
                    model,
                    memory,
                    context.UserInput,
                    Array.Empty<AgentToolExecutionRecord>(),
                    conversation.ToArray(),
                    cancellationToken)
                .ConfigureAwait(false);
        }

        return new AgentExecutionResult
        {
            IsImplemented = true,
            Message = assistantMessage,
            Model = model,
            AvailableTools = availableTools,
            UpdatedConversation = conversation.ToArray(),
            ToolExecutions = executedTools.ToArray(),
            UpdatedConversationMemory = memory,
            ContinuationExhausted = continuationExhausted,
            MultiAgentTrace = new AgentMultiAgentExecutionTrace(plan, specialistRuns)
        };
    }

    private async Task<AgentTaskResult> ExecuteSpecialistSafeAsync(
        string model,
        string userQuestion,
        AgentConversationMemory memory,
        IReadOnlyList<AgentConversationMessage> minimalTail,
        AgentDispatchStep step,
        CancellationToken cancellationToken)
    {
        try
        {
            var allowed = SpecialistToolAllowlists.ForSpecialist(step.SpecialistKind);
            var filtered = new FilteredAgentToolRuntime(_toolRuntime, allowed, step.SpecialistKind.ToString());
            var specialistSystem = MultiAgentPromptBuilder.BuildSpecialistSystemPrompt(step.SpecialistKind, _options);

            var request = new AgentTaskRequest(
                step.SpecialistKind,
                userQuestion,
                step.Reason,
                memory,
                minimalTail,
                filtered.GetTools(),
                model,
                specialistSystem,
                ExpectsDatasetQueryEvidence: step.ExpectsDatasetQueryEvidence);

            return await _specialistWorker.ExecuteAsync(request, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            _logger.LogError(exception, "Specialist {Specialist} crashed; continuing with empty evidence.", step.SpecialistKind);
            return new AgentTaskResult(
                step.SpecialistKind,
                Success: false,
                FailureMessage: exception.Message,
                ToolExecutions: Array.Empty<AgentToolExecutionRecord>(),
                StructuredOutput: SpecialistStructuredOutputParser.FromToolFallback(
                    step.SpecialistKind,
                    Array.Empty<AgentToolExecutionRecord>(),
                    digestMaxChars: Math.Clamp(_options.MemoryEvidenceDigestMaxChars, 96, 1200)),
                SpecialistScratchTranscript: Array.Empty<AgentConversationMessage>());
        }
    }

    private static string BuildDigest(string? json)
    {
        var text = (json ?? string.Empty).Trim();
        return text.Length <= 220 ? text : string.Concat(text.AsSpan(0, 220), "…");
    }

    private static IReadOnlyList<AgentConversationMessage> BuildMinimalTranscriptTail(IReadOnlyList<AgentConversationMessage> history)
        => history
            .Where(message => message.Role is AgentConversationRole.User or AgentConversationRole.Assistant)
            .TakeLast(6)
            .ToArray();

    private static string BuildCoordinatorMemorySystemPrompt(AgentConversationMemory memory)
    {
        var memoryBlock = AgentEphemeralWorkerRunner.FormatMemoryBlock(memory);
        if (string.IsNullOrWhiteSpace(memoryBlock))
        {
            return "You are the Machine Health Explorer multi-agent coordinator (memory-only block).";
        }

        return $"""
You are the Machine Health Explorer multi-agent coordinator (memory-only block).

[Session memory — authoritative for continuity across context resets]
{memoryBlock}
""";
    }

    private async Task<AgentConversationMemory> EnforceMainConversationBudgetAsync(
        List<AgentConversationMessage> conversation,
        AgentConversationMemory memory,
        string model,
        string coordinatorSystem,
        CancellationToken cancellationToken)
    {
        var mem = memory;
        var guard = 0;

        while (guard++ < 32)
        {
            var estimated = AgentContextBudgetEstimator.EstimatePromptTokens(_options, coordinatorSystem, conversation, Array.Empty<AgentToolDefinition>());
            var maxOut = AgentContextBudgetEstimator.ComputeEffectiveMaxOutputTokens(_options, estimated, reasoningPressureSteps: 0, lastUsage: null);
            var softSlot = Math.Max(512, AgentContextBudgetEstimator.GetEffectiveHostContextTokens(_options) - _options.ContextSafetyMarginTokens);

            var messagePressure = _options.EnableContextCompaction && conversation.Count > _options.MaxConversationMessages;
            var tokenPressure = _options.EnableTokenBudgetCompaction && estimated + maxOut > softSlot;

            if (!messagePressure && !tokenPressure)
            {
                return mem;
            }

            var reason = tokenPressure ? "token_budget" : "message_count";
            _logger.LogWarning(
                "Main conversation compaction ({Reason}): est_prompt~{Est} max_out={Max} soft_slot={Slot} msgs={Msgs}",
                reason,
                estimated,
                maxOut,
                softSlot,
                conversation.Count);

            var before = conversation.Count;
            mem = await CompactOldestMessagesOnceAsync(conversation, mem, model, cancellationToken).ConfigureAwait(false);
            var after = conversation.Count;

            if (after >= before)
            {
                _logger.LogError(
                    "Unable to compact further (est_prompt~{Est}, max_out={Max}, msgs={Msgs}). Proceeding with best-effort.",
                    estimated,
                    maxOut,
                    conversation.Count);
                return mem;
            }
        }

        return mem;
    }

    private async Task<AgentConversationMemory> MaybeCompactConversationAsync(
        List<AgentConversationMessage> conversation,
        AgentConversationMemory memory,
        string model,
        CancellationToken cancellationToken)
    {
        var mem = memory;

        while (_options.EnableContextCompaction && conversation.Count > _options.MaxConversationMessages)
        {
            mem = await CompactOldestMessagesOnceAsync(conversation, mem, model, cancellationToken).ConfigureAwait(false);
        }

        return mem;
    }

    private async Task<AgentConversationMemory> CompactOldestMessagesOnceAsync(
        List<AgentConversationMessage> conversation,
        AgentConversationMemory memory,
        string model,
        CancellationToken cancellationToken)
    {
        var keep = Math.Max(4, Math.Min(_options.CompactionKeepRecentMessages, Math.Max(1, _options.MaxConversationMessages - 1)));
        if (keep >= conversation.Count)
        {
            return memory;
        }

        var headCount = conversation.Count - keep;
        var head = conversation.Take(headCount).ToArray();
        conversation.RemoveRange(0, headCount);

        _logger.LogWarning(
            "Compacted conversation head: removed_messages={Removed} kept_messages={Kept}",
            head.Length,
            conversation.Count);

        var summary = await _workerRunner.SummarizeConversationHeadAsync(model, head, memory, cancellationToken).ConfigureAwait(false);
        var mergedSummary = string.IsNullOrWhiteSpace(memory.RollingSummary)
            ? summary
            : $"{memory.RollingSummary}\n{summary}";

        return memory.WithRollingSummary(mergedSummary.Trim(), _options.MemorySummaryMaxLength);
    }

    private async Task<string> ResolveModelAsync(CancellationToken cancellationToken)
    {
        if (!string.IsNullOrWhiteSpace(_resolvedModel))
        {
            return _resolvedModel;
        }

        if (!string.IsNullOrWhiteSpace(_options.Model))
        {
            _resolvedModel = _options.Model.Trim();
            return _resolvedModel;
        }

        var models = await _chatClient.GetAvailableModelsAsync(cancellationToken).ConfigureAwait(false);
        _resolvedModel = models.FirstOrDefault();
        if (string.IsNullOrWhiteSpace(_resolvedModel))
        {
            throw new InvalidOperationException(
                $"No LM Studio model is available at '{_options.BaseUrl}'. Load a model in LM Studio or set Agent:Model in appsettings.json.");
        }

        return _resolvedModel;
    }
}
