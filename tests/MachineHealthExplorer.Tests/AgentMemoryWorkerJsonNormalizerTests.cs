using System.Text.Json;
using MachineHealthExplorer.Agent.Services;

namespace MachineHealthExplorer.Tests;

public sealed class AgentMemoryWorkerJsonNormalizerTests
{
    [Fact]
    public void PrepareMemoryWorkerJson_PlainObject_PreservesPayload()
    {
        const string raw = """{"currentUserIntent":"x","pendingQuestions":[],"confirmedFacts":[],"toolHighlights":[],"language":"pt","rollingSummary":""}""";
        var prepared = AgentMemoryWorkerJsonNormalizer.PrepareMemoryWorkerJson(raw);
        using var doc = JsonDocument.Parse(prepared);
        Assert.Equal("x", doc.RootElement.GetProperty("currentUserIntent").GetString());
    }

    [Fact]
    public void PrepareMemoryWorkerJson_MarkdownJsonFence_StripsAndParses()
    {
        var raw = """
```json
{"currentUserIntent":"intent","pendingQuestions":[],"confirmedFacts":[],"toolHighlights":[],"language":"","rollingSummary":"s"}
```
""";
        var prepared = AgentMemoryWorkerJsonNormalizer.PrepareMemoryWorkerJson(raw);
        using var doc = JsonDocument.Parse(prepared);
        Assert.Equal("intent", doc.RootElement.GetProperty("currentUserIntent").GetString());
        Assert.Equal("s", doc.RootElement.GetProperty("rollingSummary").GetString());
    }

    [Fact]
    public void PrepareMemoryWorkerJson_GenericFence_StripsAndParses()
    {
        var raw = """
```
{"pendingQuestions":["q1"],"confirmedFacts":[],"toolHighlights":[],"language":"","rollingSummary":"","currentUserIntent":""}
```
""";
        var prepared = AgentMemoryWorkerJsonNormalizer.PrepareMemoryWorkerJson(raw);
        using var doc = JsonDocument.Parse(prepared);
        Assert.Equal("q1", doc.RootElement.GetProperty("pendingQuestions")[0].GetString());
    }

    [Fact]
    public void PrepareMemoryWorkerJson_PreambleBeforeBrace_ExtractsObject()
    {
        const string raw = """Here is JSON: {"language":"en","currentUserIntent":"","pendingQuestions":[],"confirmedFacts":[],"toolHighlights":[],"rollingSummary":""} thanks""";
        var prepared = AgentMemoryWorkerJsonNormalizer.PrepareMemoryWorkerJson(raw);
        using var doc = JsonDocument.Parse(prepared);
        Assert.Equal("en", doc.RootElement.GetProperty("language").GetString());
    }
}
