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
{AgentPromptBudgetGuard.CompactPlain(payloadJson, 24000)}

Memory block (may overlap; treat JSON as primary):
{AgentEphemeralWorkerRunner.FormatMemoryBlock(memory)}
"""
        });

        var estimatedPrompt = AgentContextBudgetEstimator.EstimatePromptTokens(_options, systemPrompt, messages, Array.Empty<AgentToolDefinition>());
        var maxOut = AgentContextBudgetEstimator.ComputeEffectiveMaxOutputTokens(_options, estimatedPrompt, reasoningPressureSteps: 0, lastUsage: null);

        _logger.LogInformation(
            "FinalComposer: model={Model} prompt_est~{PromptEst} max_out={MaxOut} tools_exposed=0 enable_tools=false",
            model,
            estimatedPrompt,
            maxOut);

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

        return new FinalComposerLlmTurn(systemPrompt, messages.ToArray(), response with { ToolCalls = Array.Empty<AgentToolCall>() });
    }
}
