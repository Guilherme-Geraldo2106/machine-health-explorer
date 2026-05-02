using System.Linq;
using System.Text.Json;
using MachineHealthExplorer.Agent.Models;
using MachineHealthExplorer.Agent.MultiAgent;

namespace MachineHealthExplorer.Tests;

public sealed class SpecialistStructuredOutputParserTests
{
    [Fact]
    public void FromToolFallback_PreservesAggregationRequestSummary_InEnvelope()
    {
        var args =
            """{"aggregations":[{"alias":"row_count","function":"Count"},{"alias":"sub_count","function":"Count","filter":{"columnName":"flag","operator":"Equals","value":true}}]}""";
        var resultJson = """{"columns":["g","row_count","sub_count"],"rows":[{"g":"A","row_count":10,"sub_count":2}]}""";
        var exec = new AgentToolExecutionRecord
        {
            ToolName = "group_and_aggregate",
            ArgumentsJson = args,
            ResultJson = resultJson,
            IsError = false
        };

        var structured = SpecialistStructuredOutputParser.FromToolFallback(
            AgentSpecialistKind.QueryAnalysis,
            [exec],
            toolEvidenceMaxChars: 8000);

        var frag = structured.Evidences[0].SupportingJsonFragment!;
        using var doc = JsonDocument.Parse(frag);
        var root = doc.RootElement;
        Assert.Equal("mhe_tool_evidence_v1", root.GetProperty("schema").GetString());
        var summary = root.GetProperty("aggregationRequestSummary");
        Assert.Equal(2, summary.GetArrayLength());
        Assert.False(summary[0].GetProperty("perAggregationFilterPresent").GetBoolean());
        Assert.True(summary[1].GetProperty("perAggregationFilterPresent").GetBoolean());
    }

    [Fact]
    public void FromToolFallback_PreservesDerivedMetricsSummary_InEnvelope()
    {
        var args =
            """{"aggregations":[{"alias":"row_count","function":"Count"}],"derivedMetrics":[{"alias":"r","expression":"row_count / 2"}]}""";
        var exec = new AgentToolExecutionRecord
        {
            ToolName = "group_and_aggregate",
            ArgumentsJson = args,
            ResultJson = """{"columns":["row_count","r"],"rows":[]}""",
            IsError = false
        };

        var structured = SpecialistStructuredOutputParser.FromToolFallback(
            AgentSpecialistKind.QueryAnalysis,
            [exec],
            toolEvidenceMaxChars: 6000);

        using var doc = JsonDocument.Parse(structured.Evidences[0].SupportingJsonFragment!);
        var root = doc.RootElement;
        Assert.True(root.TryGetProperty("derivedMetricsSummary", out var dm));
        Assert.Equal(1, dm.GetArrayLength());
        Assert.Equal("r", dm[0].GetProperty("alias").GetString());
    }

    [Fact]
    public void FromToolFallback_SmallTabularResult_NotRawPrefixTruncated()
    {
        var rows = Enumerable.Range(0, 8)
            .Select(i => $@"{{""k"":{i},""row_count"":{i + 1}}}")
            .ToArray();
        var rowsJson = string.Join(",", rows);
        var resultJson = $@"{{""columns"":[""k"",""row_count""],""rows"":[{rowsJson}],""totalGroups"":8,""page"":1,""pageSize"":50,""scopedRowCount"":8}}";
        var exec = new AgentToolExecutionRecord
        {
            ToolName = "group_and_aggregate",
            ArgumentsJson = """{"aggregations":[{"alias":"row_count","function":"Count"}]}""",
            ResultJson = resultJson,
            IsError = false
        };

        var structured = SpecialistStructuredOutputParser.FromToolFallback(
            AgentSpecialistKind.QueryAnalysis,
            [exec],
            toolEvidenceMaxChars: 12_000);

        var frag = structured.Evidences[0].SupportingJsonFragment!;
        using var doc = JsonDocument.Parse(frag);
        var preview = doc.RootElement.GetProperty("preview").GetString()!;
        using var inner = JsonDocument.Parse(preview);
        var rowArray = inner.RootElement.GetProperty("rows");
        Assert.Equal(8, rowArray.GetArrayLength());
        Assert.Equal(7, rowArray[7].GetProperty("k").GetInt32());
    }

    [Fact]
    public void FromToolFallback_StructuralCompact_KeepsPaginationAndValidJson()
    {
        var rowTemplates = Enumerable.Range(0, 500).Select(i => $@"{{""i"":{i}}}").ToArray();
        var rowsJson = string.Join(",", rowTemplates);
        var wide = $@"{{""columns"":[""i""],""rows"":[{rowsJson}],""totalGroups"":500,""scopedRowCount"":5000,""page"":2,""pageSize"":50}}";
        var exec = new AgentToolExecutionRecord
        {
            ToolName = "group_and_aggregate",
            ArgumentsJson = "{}",
            ResultJson = wide,
            IsError = false
        };

        var structured = SpecialistStructuredOutputParser.FromToolFallback(
            AgentSpecialistKind.QueryAnalysis,
            [exec],
            toolEvidenceMaxChars: 3500);

        var frag = structured.Evidences[0].SupportingJsonFragment!;
        using var doc = JsonDocument.Parse(frag);
        var preview = doc.RootElement.GetProperty("preview").GetString()!;
        using var inner = JsonDocument.Parse(preview);
        var root = inner.RootElement;
        Assert.True(root.TryGetProperty("rowsFirst", out _));
        Assert.True(root.TryGetProperty("rowsLast", out _));
        Assert.Equal(500, root.GetProperty("totalGroups").GetInt32());
        Assert.Equal(2, root.GetProperty("page").GetInt32());
        Assert.Equal(50, root.GetProperty("pageSize").GetInt32());
        Assert.Equal(5000, root.GetProperty("scopedRowCount").GetInt32());
    }

    [Fact]
    public void FromToolFallback_ToolError_PreservesPayloadFragment()
    {
        var err = """{"tool_error":true,"code":"bad_args","detail":"fix request"}""";
        var exec = new AgentToolExecutionRecord
        {
            ToolName = "group_and_aggregate",
            ArgumentsJson = "{}",
            ResultJson = err,
            IsError = true
        };

        var structured = SpecialistStructuredOutputParser.FromToolFallback(
            AgentSpecialistKind.QueryAnalysis,
            [exec],
            toolEvidenceMaxChars: 2000);

        Assert.Equal("tool_error", structured.Evidences[0].Summary);
        Assert.Contains("tool_error", structured.Evidences[0].SupportingJsonFragment!, StringComparison.Ordinal);
    }
}
