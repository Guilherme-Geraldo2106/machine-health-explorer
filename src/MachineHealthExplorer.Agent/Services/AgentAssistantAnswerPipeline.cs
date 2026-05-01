using MachineHealthExplorer.Agent.Abstractions;
using MachineHealthExplorer.Agent.Models;
using MachineHealthExplorer.Logging.Abstractions;
using MachineHealthExplorer.Logging.Models;
using MachineHealthExplorer.Logging.Services;
using Microsoft.Extensions.Logging;

namespace MachineHealthExplorer.Agent.Services;

internal sealed class AgentAssistantAnswerPipeline
{
    private readonly AgentOptions _options;
    private readonly IAgentChatClient _chatClient;
    private readonly AgentEphemeralWorkerRunner _workerRunner;
    private readonly ILogger _logger;
    private readonly IChatSessionLogger _chatSessionLogger;
    private AgentTokenUsage? _lastUsage;
    private int _reasoningPressure;

    public AgentAssistantAnswerPipeline(
        AgentOptions options,
        IAgentChatClient chatClient,
        AgentEphemeralWorkerRunner workerRunner,
        ILogger logger,
        IChatSessionLogger? chatSessionLogger = null)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _chatClient = chatClient ?? throw new ArgumentNullException(nameof(chatClient));
        _workerRunner = workerRunner ?? throw new ArgumentNullException(nameof(workerRunner));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _chatSessionLogger = chatSessionLogger ?? NullChatSessionLogger.Instance;
    }

    public void ResetTelemetry()
    {
        _lastUsage = null;
        _reasoningPressure = 0;
    }

    public void IngestUsage(AgentTokenUsage? usage) => UpdateUsageTelemetry(usage);

    public async Task<(string message, bool continuationExhausted)> CompleteAssistantAnswerAsync(
        string model,
        AgentConversationMemory memory,
        List<AgentConversationMessage> conversationPrefix,
        AgentModelResponse firstResponse,
        string systemPrompt,
        CancellationToken cancellationToken)
    {
        var scratch = new List<AgentConversationMessage>(conversationPrefix);
        var current = firstResponse;
        var accumulated = string.Empty;
        var additionalCallsMade = 0;
        var continuationRounds = 0;
        var mem = memory;

        while (true)
        {
            current = await EnsureVisibleModelResponseAsync(model, mem, scratch, current, cancellationToken).ConfigureAwait(false);

            var piece = AgentVisibleResponseNormalizer.StripInternalAssistantSurface(current.Content ?? string.Empty) ?? string.Empty;
            accumulated = AgentResponseMerger.MergeAssistantFragments(accumulated, piece);

            if (!AgentFinishReason.IsTruncated(current.FinishReason))
            {
                _logger.LogInformation("Assistant answer completed without continuation (rounds={Rounds}).", continuationRounds);
                return (TrimAssistantMessage(accumulated), continuationExhausted: false);
            }

            if (additionalCallsMade >= _options.MaxContinuationRounds)
            {
                break;
            }

            scratch.Add(new AgentConversationMessage
            {
                Role = AgentConversationRole.Assistant,
                Content = string.IsNullOrWhiteSpace(piece) ? null : piece
            });

            scratch.Add(new AgentConversationMessage
            {
                Role = AgentConversationRole.User,
                Content = BuildContinuationUserPrompt(mem.LanguagePreference)
            });

            mem = await EnforceContextBudgetAsync(scratch, mem, model, systemPrompt, cancellationToken).ConfigureAwait(false);

            var floor = AgentContextBudgetEstimator.GetAssistantCompletionFloorTokens(_options);
            var maxOut = 0;
            for (var budgetLoop = 0; budgetLoop < 24; budgetLoop++)
            {
                var estimatedPrompt = AgentContextBudgetEstimator.EstimatePromptTokens(_options, systemPrompt, scratch, Array.Empty<AgentToolDefinition>());
                maxOut = AgentContextBudgetEstimator.ComputeEffectiveMaxOutputTokens(
                    _options,
                    estimatedPrompt,
                    _reasoningPressure,
                    _lastUsage,
                    continuationAssistantPass: true);
                if (maxOut >= floor)
                {
                    break;
                }

                if (scratch.Count <= 2)
                {
                    break;
                }

                mem = await CompactOldestMessagesOnceAsync(scratch, mem, model, cancellationToken).ConfigureAwait(false);
            }

            _logger.LogInformation(
                "Assistant continuation {Round}: max_out={MaxOut} floor={Floor}",
                additionalCallsMade + 1,
                maxOut,
                floor);

            if (maxOut == 0 || maxOut < floor)
            {
                _chatSessionLogger.Append(new ChatSessionLogEvent(
                    default,
                    string.Empty,
                    "agent.continuation_cancelled",
                    "internal",
                    model,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    ContinuationBudgetDetail: $"max_out={maxOut};floor={floor};reason=budget"));
                accumulated = AgentResponseMerger.MergeAssistantFragments(
                    accumulated,
                    "\n\n[Encerramento técnico: orçamento de contexto insuficiente para continuar sem violar o piso de max_tokens.]");
                return (TrimAssistantMessage(accumulated), continuationExhausted: true);
            }

            try
            {
                current = await _chatClient.CompleteAsync(new AgentModelRequest
                {
                    Model = model,
                    SystemPrompt = systemPrompt,
                    Messages = scratch,
                    Tools = Array.Empty<AgentToolDefinition>(),
                    Temperature = _options.Temperature,
                    MaxOutputTokens = maxOut,
                    EnableTools = false
                }, cancellationToken).ConfigureAwait(false);
            }
            catch (AgentModelBackendException ex) when (ex.IsContextLengthExceeded && scratch.Count > 4)
            {
                _logger.LogWarning(ex, "Continuation exceeded host context; compacting once and retrying.");
                mem = await CompactOldestMessagesOnceAsync(scratch, mem, model, cancellationToken).ConfigureAwait(false);
                var coordinatorSystem = systemPrompt;
                var estimatedRetry = AgentContextBudgetEstimator.EstimatePromptTokens(_options, coordinatorSystem, scratch, Array.Empty<AgentToolDefinition>());
                var maxOutRetry = AgentContextBudgetEstimator.ComputeEffectiveMaxOutputTokens(
                    _options,
                    estimatedRetry,
                    _reasoningPressure,
                    _lastUsage,
                    continuationAssistantPass: true);
                if (maxOutRetry == 0 || maxOutRetry < floor)
                {
                    _chatSessionLogger.Append(new ChatSessionLogEvent(
                        default,
                        string.Empty,
                        "agent.continuation_cancelled",
                        "internal",
                        model,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        ContinuationBudgetDetail: $"max_out={maxOutRetry};floor={floor};reason=context_error"));
                    accumulated = AgentResponseMerger.MergeAssistantFragments(
                        accumulated,
                        "\n\n[Encerramento técnico: continuação cancelada após erro de contexto no backend.]");
                    return (TrimAssistantMessage(accumulated), continuationExhausted: true);
                }

                current = await _chatClient.CompleteAsync(new AgentModelRequest
                {
                    Model = model,
                    SystemPrompt = coordinatorSystem,
                    Messages = scratch,
                    Tools = Array.Empty<AgentToolDefinition>(),
                    Temperature = _options.Temperature,
                    MaxOutputTokens = maxOutRetry,
                    EnableTools = false
                }, cancellationToken).ConfigureAwait(false);
            }

            model = string.IsNullOrWhiteSpace(current.Model) ? model : current.Model;
            UpdateUsageTelemetry(current.Usage);
            continuationRounds++;
            additionalCallsMade++;
        }

        var continuationExhausted = AgentFinishReason.IsTruncated(current.FinishReason)
            && additionalCallsMade >= _options.MaxContinuationRounds;

        _logger.LogWarning(
            "Continuation budget exhausted (rounds={Rounds}). Running epilogue. finish={Finish}",
            continuationRounds,
            current.FinishReason);

        accumulated = await _workerRunner.RunEpilogueAsync(
            model,
            mem.LanguagePreference,
            accumulated,
            cancellationToken).ConfigureAwait(false);

        return (TrimAssistantMessage(accumulated), continuationExhausted);
    }

    public async Task<AgentModelResponse> EnsureVisibleModelResponseAsync(
        string model,
        AgentConversationMemory memory,
        IReadOnlyList<AgentConversationMessage> transcript,
        AgentModelResponse response,
        CancellationToken cancellationToken)
    {
        if (!AgentVisibleResponseNormalizer.NeedsVisibleRecovery(response))
        {
            return response with { ReasoningContent = null };
        }

        _logger.LogWarning(
            "Empty user-visible assistant content. finish_reason={Finish} reasoning_len={ReasoningLen} truncated_like={Trunc}",
            response.FinishReason,
            response.ReasoningContent?.Length ?? 0,
            AgentFinishReason.IsTruncated(response.FinishReason));

        var recovered = await _workerRunner.RequestUserVisibleAnswerAsync(model, memory, transcript, cancellationToken).ConfigureAwait(false);
        UpdateUsageTelemetry(recovered.Usage);

        _logger.LogInformation(
            "Visible recovery pass: finish={Finish} content_len={Len} usage_prompt={Up} usage_compl={Uc}",
            recovered.FinishReason,
            recovered.Content?.Length ?? 0,
            recovered.Usage?.PromptTokens,
            recovered.Usage?.CompletionTokens);

        if (AgentVisibleResponseNormalizer.NeedsVisibleRecovery(recovered))
        {
            _logger.LogError("Visible recovery did not produce user-visible text. Returning a safe terminal response object.");
            return recovered with
            {
                Content = "Não foi possível gerar uma resposta visível após um ciclo interno do modelo. Tente novamente com uma pergunta mais curta ou reduza o histórico.",
                FinishReason = "stop",
                ReasoningContent = null
            };
        }

        return recovered with { ReasoningContent = null };
    }

    private async Task<AgentConversationMemory> EnforceContextBudgetAsync(
        List<AgentConversationMessage> conversation,
        AgentConversationMemory memory,
        string model,
        string systemPrompt,
        CancellationToken cancellationToken)
    {
        var mem = memory;
        var guard = 0;

        while (guard++ < 32)
        {
            var estimated = AgentContextBudgetEstimator.EstimatePromptTokens(_options, systemPrompt, conversation, Array.Empty<AgentToolDefinition>());
            var maxOut = AgentContextBudgetEstimator.ComputeEffectiveMaxOutputTokens(
                _options,
                estimated,
                _reasoningPressure,
                _lastUsage,
                continuationAssistantPass: false);
            var softSlot = Math.Max(512, AgentContextBudgetEstimator.GetEffectiveHostContextTokens(_options) - _options.ContextSafetyMarginTokens);

            var messagePressure = _options.EnableContextCompaction && conversation.Count > _options.MaxConversationMessages;
            var tokenPressure = _options.EnableTokenBudgetCompaction && (maxOut == 0 || estimated + maxOut > softSlot);

            if (!messagePressure && !tokenPressure)
            {
                return mem;
            }

            var reason = tokenPressure ? "token_budget" : "message_count";
            _logger.LogWarning(
                "Context compaction ({Reason}): est_prompt~{Est} max_out={Max} soft_slot={Slot} msgs={Msgs}",
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

    private void UpdateUsageTelemetry(AgentTokenUsage? usage)
    {
        if (usage is null)
        {
            return;
        }

        _lastUsage = usage;

        if (usage.ReasoningTokens is int reasoning && reasoning > 0)
        {
            _reasoningPressure = Math.Min(_reasoningPressure + 1, 24);
        }

        _logger.LogInformation(
            "Model usage: prompt_tokens={Prompt} completion_tokens={Completion} total_tokens={Total} reasoning_tokens={Reasoning}",
            usage.PromptTokens,
            usage.CompletionTokens,
            usage.TotalTokens,
            usage.ReasoningTokens);
    }

    private static string TrimAssistantMessage(string accumulated)
    {
        var cleaned = AgentVisibleResponseNormalizer.StripInternalAssistantSurface(accumulated) ?? string.Empty;
        if (string.IsNullOrWhiteSpace(cleaned))
        {
            return "The agent did not produce a final answer.";
        }

        return cleaned.Trim();
    }

    private static string BuildContinuationUserPrompt(string languagePreference)
    {
        var language = string.IsNullOrWhiteSpace(languagePreference) ? "the same language as your previous assistant content" : languagePreference;
        return $"""
Continue the assistant answer exactly where it stopped.
Rules:
- Write ONLY the continuation text (no preface).
- Do not repeat earlier paragraphs or restate what is already written.
- Keep {language} consistently.
- No chain-of-thought. Output only user-visible text.
- If tools are required, say so briefly, but prefer finishing the explanation if the facts are already present in the transcript.
""";
    }
}
