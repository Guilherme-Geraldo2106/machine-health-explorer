using MachineHealthExplorer.Agent.Abstractions;
using MachineHealthExplorer.Agent.Models;
using Microsoft.Extensions.Logging;

namespace MachineHealthExplorer.Agent.Services;

internal sealed class AgentSessionEngine
{
    private readonly AgentOptions _options;
    private readonly IAgentChatClient _chatClient;
    private readonly IAgentToolRuntime _toolRuntime;
    private readonly AgentEphemeralWorkerRunner _workerRunner;
    private readonly AgentToolScopePlanner _toolPlanner;
    private readonly ILogger _logger;
    private string? _resolvedModel;
    private AgentTokenUsage? _lastUsage;
    private int _reasoningPressure;

    public AgentSessionEngine(
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
        _toolPlanner = new AgentToolScopePlanner(_options, _chatClient);
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
        var allTools = _toolRuntime.GetTools();
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

        var executedTools = new List<AgentToolExecutionRecord>();
        var continuationExhausted = false;

        for (var iteration = 0; iteration < Math.Max(1, _options.MaxToolIterations); iteration++)
        {
            var coordinatorSystem = BuildCoordinatorSystemPrompt(BuildSystemPrompt(), memory);
            var scopedTools = await _toolPlanner.SelectToolsAsync(model, conversation, memory, allTools, cancellationToken).ConfigureAwait(false);

            memory = await EnforceContextBudgetAsync(
                conversation,
                memory,
                model,
                coordinatorSystem,
                scopedTools,
                cancellationToken).ConfigureAwait(false);

            var estimatedPrompt = AgentContextBudgetEstimator.EstimatePromptTokens(_options, coordinatorSystem, conversation, scopedTools);
            var maxOut = AgentContextBudgetEstimator.ComputeEffectiveMaxOutputTokens(_options, estimatedPrompt, _reasoningPressure, _lastUsage);

            _logger.LogInformation(
                "Agent turn {Iteration}: prompt_est~{PromptEst} max_out={MaxOut} tools_exposed={ToolCount} msgs={MsgCount}",
                iteration,
                estimatedPrompt,
                maxOut,
                scopedTools.Count,
                conversation.Count);

            AgentModelResponse response;
            try
            {
                response = await _chatClient.CompleteAsync(new AgentModelRequest
                {
                    Model = model,
                    SystemPrompt = coordinatorSystem,
                    Messages = conversation,
                    Tools = scopedTools,
                    Temperature = _options.Temperature,
                    MaxOutputTokens = maxOut,
                    EnableTools = scopedTools.Count > 0,
                    UseMinimalToolSchemas = scopedTools.Count > 0 && _options.EnableDynamicToolScoping
                }, cancellationToken).ConfigureAwait(false);
            }
            catch (AgentModelBackendException ex) when (ex.IsContextLengthExceeded)
            {
                var keep = Math.Max(4, Math.Min(_options.CompactionKeepRecentMessages, Math.Max(1, _options.MaxConversationMessages - 1)));
                if (conversation.Count <= keep + 1)
                {
                    throw;
                }

                _logger.LogWarning(ex, "Primary completion exceeded host context; compacting once and retrying.");
                memory = await CompactOldestMessagesOnceAsync(conversation, memory, model, cancellationToken).ConfigureAwait(false);
                coordinatorSystem = BuildCoordinatorSystemPrompt(BuildSystemPrompt(), memory);
                var estimatedRetry = AgentContextBudgetEstimator.EstimatePromptTokens(_options, coordinatorSystem, conversation, scopedTools);
                var maxOutRetry = AgentContextBudgetEstimator.ComputeEffectiveMaxOutputTokens(_options, estimatedRetry, _reasoningPressure, _lastUsage);
                response = await _chatClient.CompleteAsync(new AgentModelRequest
                {
                    Model = model,
                    SystemPrompt = coordinatorSystem,
                    Messages = conversation,
                    Tools = scopedTools,
                    Temperature = _options.Temperature,
                    MaxOutputTokens = maxOutRetry,
                    EnableTools = scopedTools.Count > 0,
                    UseMinimalToolSchemas = scopedTools.Count > 0 && _options.EnableDynamicToolScoping
                }, cancellationToken).ConfigureAwait(false);
            }

            model = string.IsNullOrWhiteSpace(response.Model) ? model : response.Model;
            UpdateUsageTelemetry(response.Usage);

            response = await EnsureVisibleModelResponseAsync(model, memory, conversation, response, cancellationToken).ConfigureAwait(false);
            response = FilterToolCallsToKnownCatalog(response, allTools);

            if (response.ToolCalls.Count > 0)
            {
                conversation.Add(new AgentConversationMessage
                {
                    Role = AgentConversationRole.Assistant,
                    Content = AgentVisibleResponseNormalizer.StripInternalAssistantSurface(response.Content),
                    ToolCalls = response.ToolCalls
                });

                foreach (var toolCall in response.ToolCalls)
                {
                    var execution = await _toolRuntime.ExecuteAsync(toolCall.Name, toolCall.ArgumentsJson, cancellationToken).ConfigureAwait(false);
                    executedTools.Add(execution);
                    var toolMessageContent = AgentToolEvidenceCompressor.BuildToolMessageContent(
                        toolCall.Name,
                        execution.ResultJson,
                        _options.MaxToolEvidenceContentChars);

                    conversation.Add(new AgentConversationMessage
                    {
                        Role = AgentConversationRole.Tool,
                        Name = toolCall.Name,
                        ToolCallId = toolCall.Id,
                        Content = toolMessageContent
                    });
                }

                memory = await _workerRunner.RefreshMemoryAsync(
                    model,
                    memory,
                    context.UserInput,
                    executedTools,
                    conversation.ToArray(),
                    cancellationToken).ConfigureAwait(false);

                continue;
            }

            var (assistantMessage, exhausted, updatedMemory) = await CompleteAssistantAnswerAsync(
                model,
                memory,
                conversation,
                response,
                coordinatorSystem,
                cancellationToken).ConfigureAwait(false);

            continuationExhausted = exhausted;
            memory = updatedMemory;

            conversation.Add(new AgentConversationMessage
            {
                Role = AgentConversationRole.Assistant,
                Content = assistantMessage
            });

            memory = await _workerRunner.RefreshMemoryAsync(
                model,
                memory,
                context.UserInput,
                Array.Empty<AgentToolExecutionRecord>(),
                conversation.ToArray(),
                cancellationToken).ConfigureAwait(false);

            return new AgentExecutionResult
            {
                IsImplemented = true,
                Message = assistantMessage,
                Model = model,
                AvailableTools = availableTools,
                UpdatedConversation = conversation.ToArray(),
                ToolExecutions = executedTools.ToArray(),
                UpdatedConversationMemory = memory,
                ContinuationExhausted = continuationExhausted
            };
        }

        const string limitMessage = "The agent reached the tool-call limit before producing a final answer.";
        conversation.Add(new AgentConversationMessage
        {
            Role = AgentConversationRole.Assistant,
            Content = limitMessage
        });

        return new AgentExecutionResult
        {
            IsImplemented = true,
            Message = limitMessage,
            Model = model,
            AvailableTools = availableTools,
            UpdatedConversation = conversation.ToArray(),
            ToolExecutions = executedTools.ToArray(),
            UpdatedConversationMemory = memory,
            ContinuationExhausted = continuationExhausted
        };
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

    private async Task<AgentModelResponse> EnsureVisibleModelResponseAsync(
        string model,
        AgentConversationMemory memory,
        IReadOnlyList<AgentConversationMessage> transcript,
        AgentModelResponse response,
        CancellationToken cancellationToken)
    {
        if (!AgentVisibleResponseNormalizer.NeedsVisibleRecovery(response))
        {
            return response;
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

        return recovered;
    }

    private async Task<(string message, bool continuationExhausted, AgentConversationMemory memory)> CompleteAssistantAnswerAsync(
        string model,
        AgentConversationMemory memory,
        List<AgentConversationMessage> conversation,
        AgentModelResponse firstResponse,
        string coordinatorSystem,
        CancellationToken cancellationToken)
    {
        var scratch = new List<AgentConversationMessage>(conversation);
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
                return (TrimAssistantMessage(accumulated), continuationExhausted: false, mem);
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

            mem = await EnforceContextBudgetAsync(
                scratch,
                mem,
                model,
                coordinatorSystem,
                Array.Empty<AgentToolDefinition>(),
                cancellationToken).ConfigureAwait(false);

            var estimatedPrompt = AgentContextBudgetEstimator.EstimatePromptTokens(_options, coordinatorSystem, scratch, Array.Empty<AgentToolDefinition>());
            var maxOut = AgentContextBudgetEstimator.ComputeEffectiveMaxOutputTokens(_options, estimatedPrompt, _reasoningPressure, _lastUsage);

            _logger.LogInformation(
                "Assistant continuation {Round}: prompt_est~{PromptEst} max_out={MaxOut}",
                additionalCallsMade + 1,
                estimatedPrompt,
                maxOut);

            try
            {
                current = await _chatClient.CompleteAsync(new AgentModelRequest
                {
                    Model = model,
                    SystemPrompt = coordinatorSystem,
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
                coordinatorSystem = BuildCoordinatorSystemPrompt(BuildSystemPrompt(), mem);
                var estimatedRetry = AgentContextBudgetEstimator.EstimatePromptTokens(_options, coordinatorSystem, scratch, Array.Empty<AgentToolDefinition>());
                var maxOutRetry = AgentContextBudgetEstimator.ComputeEffectiveMaxOutputTokens(_options, estimatedRetry, _reasoningPressure, _lastUsage);
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

        return (TrimAssistantMessage(accumulated), continuationExhausted, mem);
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

    private async Task<AgentConversationMemory> EnforceContextBudgetAsync(
        List<AgentConversationMessage> conversation,
        AgentConversationMemory memory,
        string model,
        string coordinatorSystem,
        IReadOnlyList<AgentToolDefinition> scopedTools,
        CancellationToken cancellationToken)
    {
        var mem = memory;
        var guard = 0;

        while (guard++ < 32)
        {
            var estimated = AgentContextBudgetEstimator.EstimatePromptTokens(_options, coordinatorSystem, conversation, scopedTools);
            var maxOut = AgentContextBudgetEstimator.ComputeEffectiveMaxOutputTokens(_options, estimated, _reasoningPressure, _lastUsage);
            var softSlot = Math.Max(512, AgentContextBudgetEstimator.GetEffectiveHostContextTokens(_options) - _options.ContextSafetyMarginTokens);

            var messagePressure = _options.EnableContextCompaction && conversation.Count > _options.MaxConversationMessages;
            var tokenPressure = _options.EnableTokenBudgetCompaction && estimated + maxOut > softSlot;

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

    private static string TrimAssistantMessage(string accumulated)
    {
        var cleaned = AgentVisibleResponseNormalizer.StripInternalAssistantSurface(accumulated) ?? string.Empty;
        if (string.IsNullOrWhiteSpace(cleaned))
        {
            return "The agent did not produce a final answer.";
        }

        return cleaned.Trim();
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

    private string BuildCoordinatorSystemPrompt(string basePrompt, AgentConversationMemory memory)
    {
        var memoryBlock = AgentEphemeralWorkerRunner.FormatMemoryBlock(memory);
        if (string.IsNullOrWhiteSpace(memoryBlock))
        {
            return basePrompt;
        }

        return $"""
{basePrompt}

[Session memory — authoritative for continuity across context resets]
{memoryBlock}
""";
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

    private string BuildSystemPrompt()
    {
        if (!string.IsNullOrWhiteSpace(_options.SystemPrompt))
        {
            return _options.SystemPrompt;
        }

        return """
You are the Machine Health Explorer analysis agent.
Work only with the loaded dataset and the available tools.
Use tools whenever the user asks for schema, counts, comparisons, filters, reports, or any factual claim about the dataset.
Do not invent dataset values. Base every concrete statement on tool output.
When tool output is enough, answer clearly and concisely in the same language as the user.
If a tool returns an error, adjust the arguments and retry when it is reasonable.
Prefer shorter answers when the prompt budget is tight, without omitting critical caveats.
When several numeric columns could match an underspecified metric (for example both air and process temperature), either report both briefly or state your default assumption instead of only asking the user to choose, unless the user explicitly asked to pick one.
""";
    }
}
