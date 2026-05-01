using MachineHealthExplorer.Agent.Abstractions;
using MachineHealthExplorer.Agent.Models;
using MachineHealthExplorer.Agent.Serialization;
using MachineHealthExplorer.Agent.Services;
using Microsoft.Extensions.Logging;

namespace MachineHealthExplorer.Agent.MultiAgent;

internal sealed class FinalComposerAgent : IFinalResponseComposer
{
    private readonly AgentOptions _options;
    private readonly IAgentChatClient _chatClient;
    private readonly ILogger _logger;

    public FinalComposerAgent(AgentOptions options, IAgentChatClient chatClient, ILogger logger)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _chatClient = chatClient ?? throw new ArgumentNullException(nameof(chatClient));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public async Task<FinalComposerLlmTurn> ComposeFirstResponseAsync(
        FinalComposerInput input,
        string model,
        AgentConversationMemory memory,
        CancellationToken cancellationToken = default)
    {
        var systemPrompt = MultiAgentPromptBuilder.BuildFinalComposerSystemPrompt(_options);
        var schemaColumns = input.SchemaColumnNamesFromTools ?? Array.Empty<string>();
        var payloadJson = AgentJsonSerializer.Serialize(new
        {
            originalUserQuestion = input.OriginalUserQuestion,
            detectedLanguage = input.DetectedLanguage,
            conversationRollingSummary = input.ConversationRollingSummary,
            schemaColumnNamesFromTools = schemaColumns,
            specialistResults = input.SpecialistResults.Select(result => new
            {
                specialist = result.SpecialistKind.ToString(),
                success = result.Success,
                failure = result.FailureMessage,
                structured = result.StructuredOutput
            })
        });

        var memoryPlain = AgentEphemeralWorkerRunner.FormatMemoryBlock(memory);
        var floor = AgentContextBudgetEstimator.GetAssistantCompletionFloorTokens(_options);
        var compactLimit = 24000;

        for (var attempt = 0; attempt < 24; attempt++)
        {
            var messages = new List<AgentConversationMessage>();
            foreach (var message in input.RecentUserAssistantTail.TakeLast(6))
            {
                if (message.Role is AgentConversationRole.User or AgentConversationRole.Assistant)
                {
                    messages.Add(message);
                }
            }

            var memoryBlock = SpecialistArtifactsCarryToolEvidence(input)
                ? AgentPromptBudgetGuard.CompactPlain(memoryPlain, 720)
                : memoryPlain;

            messages.Add(new AgentConversationMessage
            {
                Role = AgentConversationRole.User,
                Content = $"""
Compose the final user-visible answer.

Specialist artifacts JSON:
{AgentPromptBudgetGuard.CompactPlain(payloadJson, compactLimit)}

Memory block (may overlap; JSON is primary — keep this block very short when JSON already carries tool-backed evidence):
{memoryBlock}
"""
            });

            var estimatedPrompt = AgentContextBudgetEstimator.EstimatePromptTokens(_options, systemPrompt, messages, Array.Empty<AgentToolDefinition>());
            var maxOut = AgentContextBudgetEstimator.ComputeEffectiveMaxOutputTokens(
                _options,
                estimatedPrompt,
                reasoningPressureSteps: 0,
                lastUsage: null,
                continuationAssistantPass: false,
                visibleCompletionFloorOverride: floor);

            _logger.LogInformation(
                "FinalComposer: model={Model} attempt={Attempt} prompt_est~{PromptEst} max_out={MaxOut} compact_limit={Compact} floor={Floor}",
                model,
                attempt + 1,
                estimatedPrompt,
                maxOut,
                compactLimit,
                floor);

            if (maxOut >= floor)
            {
                var response = await _chatClient.CompleteAsync(new AgentModelRequest
                {
                    Model = model,
                    SystemPrompt = systemPrompt,
                    Messages = messages,
                    Tools = Array.Empty<AgentToolDefinition>(),
                    Temperature = _options.Temperature,
                    MaxOutputTokens = maxOut,
                    EnableTools = false,
                    UseMinimalToolSchemas = false
                }, cancellationToken).ConfigureAwait(false);

                if (response.ToolCalls.Count > 0)
                {
                    _logger.LogError(
                        "FinalComposer received tool calls from the model ({Count}). This should be impossible by construction; stripping tool calls.",
                        response.ToolCalls.Count);
                }

                return new FinalComposerLlmTurn(
                    systemPrompt,
                    messages.ToArray(),
                    response with
                    {
                        ToolCalls = Array.Empty<AgentToolCall>(),
                        ReasoningContent = null
                    });
            }

            if (compactLimit <= 1400)
            {
                _logger.LogError("FinalComposer: budget still insufficient after shrinking payload; returning technical fallback without LLM.");
                return TechnicalFallbackTurn(
                    systemPrompt,
                    input,
                    SpecialistArtifactsCarryToolEvidence(input)
                        ? AgentPromptBudgetGuard.CompactPlain(memoryPlain, 720)
                        : memoryPlain,
                    compactLimit,
                    payloadJson,
                    model);
            }

            compactLimit = Math.Max(1400, compactLimit - 1800);
        }

        _logger.LogError("FinalComposer: exhausted payload shrink attempts without a valid max_out budget.");
        return TechnicalFallbackTurn(
            systemPrompt,
            input,
            SpecialistArtifactsCarryToolEvidence(input)
                ? AgentPromptBudgetGuard.CompactPlain(memoryPlain, 720)
                : memoryPlain,
            compactLimit,
            payloadJson,
            model);
    }

    private FinalComposerLlmTurn TechnicalFallbackTurn(
        string systemPrompt,
        FinalComposerInput input,
        string memoryBlock,
        int compactLimit,
        string payloadJson,
        string model)
    {
        var messages = new List<AgentConversationMessage>();
        foreach (var message in input.RecentUserAssistantTail.TakeLast(6))
        {
            if (message.Role is AgentConversationRole.User or AgentConversationRole.Assistant)
            {
                messages.Add(message);
            }
        }

        messages.Add(new AgentConversationMessage
        {
            Role = AgentConversationRole.User,
            Content = $"""
Compose the final user-visible answer.

Specialist artifacts JSON:
{AgentPromptBudgetGuard.CompactPlain(payloadJson, compactLimit)}

Memory block (may overlap; JSON is primary — keep this block very short when JSON already carries tool-backed evidence):
{memoryBlock}
"""
        });

        var response = new AgentModelResponse
        {
            Model = model,
            Content =
                "Não foi possível compor a resposta final: orçamento de contexto insuficiente para respeitar o piso de max_tokens mesmo após compactar o payload de evidências.",
            FinishReason = "stop",
            ToolCalls = Array.Empty<AgentToolCall>(),
            ReasoningContent = null
        };

        return new FinalComposerLlmTurn(systemPrompt, messages.ToArray(), response);
    }

    private static bool SpecialistArtifactsCarryToolEvidence(FinalComposerInput input)
    {
        foreach (var result in input.SpecialistResults)
        {
            if (result.ToolExecutions.Any(execution => !execution.IsError))
            {
                return true;
            }

            foreach (var evidence in result.StructuredOutput.Evidences)
            {
                if (!string.IsNullOrWhiteSpace(evidence.SupportingJsonFragment))
                {
                    return true;
                }
            }

            if (result.StructuredOutput.KeyMetrics.Count > 0)
            {
                return true;
            }
        }

        return false;
    }
}
