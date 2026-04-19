namespace MachineHealthExplorer.Agent.Models;

/// <summary>
/// Thrown when the chat backend rejects a request (for example context length exceeded).
/// </summary>
public sealed class AgentModelBackendException : InvalidOperationException
{
    public AgentModelBackendException(string message, int? httpStatusCode, string? responseBody, bool isContextLengthExceeded)
        : base(message)
    {
        HttpStatusCode = httpStatusCode;
        ResponseBody = responseBody ?? string.Empty;
        IsContextLengthExceeded = isContextLengthExceeded;
    }

    public int? HttpStatusCode { get; }
    public string ResponseBody { get; }
    public bool IsContextLengthExceeded { get; }

    public static bool LooksLikeContextLengthExceeded(string? responseBody)
    {
        if (string.IsNullOrWhiteSpace(responseBody))
        {
            return false;
        }

        var lower = responseBody.ToLowerInvariant();
        if (lower.Contains("n_ctx", StringComparison.Ordinal) || lower.Contains("n_keep", StringComparison.Ordinal))
        {
            return true;
        }

        return lower.Contains("context length", StringComparison.Ordinal)
               && (lower.Contains("greater than", StringComparison.Ordinal) || lower.Contains("exceed", StringComparison.Ordinal));
    }
}
