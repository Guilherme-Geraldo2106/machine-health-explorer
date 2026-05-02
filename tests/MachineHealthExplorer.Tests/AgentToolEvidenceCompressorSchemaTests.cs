using System.Text;
using System.Text.Json;
using MachineHealthExplorer.Agent.Models;
using MachineHealthExplorer.Agent.MultiAgent;
using MachineHealthExplorer.Agent.Services;

namespace MachineHealthExplorer.Tests;

public sealed class AgentToolEvidenceCompressorSchemaTests
{
    [Fact]
    public void BuildToolMessageContent_GetSchema_PreservesKeyColumnNamesWhenTightBudget()
    {
        var options = new AgentOptions();
        var wideJson =
            """{"datasetName":"AI4I 2020 Predictive Maintenance Dataset","rowCount":10000,"generatedAtUtc":"2020-01-01T00:00:00Z","columns":[{"name":"Air temperature [K]","dataType":"Decimal","isNullable":false,"nonNullCount":10000,"completenessRatio":1,"isNumeric":true,"isCategorical":false,"nullCount":0,"distinctCount":100,"distinctRatio":0.01,"cardinalityHint":"High","sampleValues":["295","298"],"numericSummary":null,"categoricalSummary":null},{"name":"Process temperature [K]","dataType":"Decimal","isNullable":false,"nonNullCount":10000,"completenessRatio":1,"isNumeric":true,"isCategorical":false,"nullCount":0,"distinctCount":200,"distinctRatio":0.02,"cardinalityHint":"High","sampleValues":["300","305"],"numericSummary":null,"categoricalSummary":null},{"name":"Machine failure","dataType":"Boolean","isNullable":false,"nonNullCount":10000,"completenessRatio":1,"isNumeric":false,"isCategorical":true,"nullCount":0,"distinctCount":2,"distinctRatio":0.0002,"cardinalityHint":"Low","sampleValues":["0","1"],"numericSummary":null,"categoricalSummary":null}]}""";

        var envelope = AgentToolEvidenceCompressor.BuildToolMessageContent("get_schema", wideJson, maxChars: 520);

        Assert.Contains("Air temperature [K]", envelope, StringComparison.Ordinal);
        Assert.Contains("Process temperature [K]", envelope, StringComparison.Ordinal);
        Assert.Contains("Machine failure", envelope, StringComparison.Ordinal);
    }

    [Fact]
    public void BuildToolMessageContent_GroupAndAggregate_IncludesAggregationRequestSummaryFromArguments()
    {
        var args =
            """{"aggregations":[{"alias":"events_alias","function":"Count"},{"alias":"rows_alias","function":"Count","filter":{"columnName":"flag","operator":"Equals","value":true}}],"pageSize":50}""";
        var envelopeJson = AgentToolEvidenceCompressor.BuildToolMessageContent(
            "group_and_aggregate",
            """{"rows":[]}""",
            maxChars: 2000,
            args);

        using var doc = JsonDocument.Parse(envelopeJson);
        var root = doc.RootElement;
        Assert.Equal("mhe_tool_evidence_v1", root.GetProperty("schema").GetString());
        var summary = root.GetProperty("aggregationRequestSummary");
        Assert.Equal(2, summary.GetArrayLength());
        Assert.False(summary[0].GetProperty("perAggregationFilterPresent").GetBoolean());
        Assert.True(summary[1].GetProperty("perAggregationFilterPresent").GetBoolean());
    }

    [Fact]
    public void BuildToolMessageContent_GroupAndAggregate_FullPageSmallResultPreservesAllRowsNotRowsFirstLast()
    {
        var rowsJson = new StringBuilder();
        rowsJson.Append('[');
        for (var i = 0; i < 50; i++)
        {
            if (i > 0)
            {
                rowsJson.Append(',');
            }

            rowsJson.Append("{\"values\":{\"Type\":\"M\",\"Air temperature [K]\":298.");
            rowsJson.Append(i % 9);
            rowsJson.Append(",\"Process temperature [K]\":308.6,\"row_count\":1,\"failure_count\":0}}");
        }

        rowsJson.Append(']');

        var wideJson =
            $$"""{"columns":["Type","Air temperature [K]","Process temperature [K]","row_count","failure_count"],"rows":{{rowsJson}},"scopedRowCount":10000,"totalGroups":3109,"page":1,"pageSize":50}""";

        var args =
            """{"aggregations":[{"alias":"row_count","function":"Count"},{"alias":"failure_count","function":"Count","filter":{"columnName":"Machine failure","operator":"equals","value":true}}],"pageSize":50}""";

        var envelopeJson = AgentToolEvidenceCompressor.BuildToolMessageContent(
            "group_and_aggregate",
            wideJson,
            maxChars: 2200,
            args);

        using var outer = JsonDocument.Parse(envelopeJson);
        var preview = outer.RootElement.GetProperty("preview").GetString() ?? string.Empty;
        Assert.DoesNotContain("rowsFirst", preview, StringComparison.Ordinal);
        Assert.DoesNotContain("omittedMiddleRowCount", preview, StringComparison.Ordinal);

        using var inner = JsonDocument.Parse(preview);
        Assert.Equal(50, inner.RootElement.GetProperty("rows").GetArrayLength());

        var summary = outer.RootElement.GetProperty("aggregationRequestSummary");
        Assert.False(summary[0].GetProperty("perAggregationFilterPresent").GetBoolean());
        Assert.True(summary[1].GetProperty("perAggregationFilterPresent").GetBoolean());
    }

    [Fact]
    public void FromToolFallback_IncludesTechnicalGapsInAnalystNotes()
    {
        var gaps = new[] { "missing derived rate evidence" };
        var structured = SpecialistStructuredOutputParser.FromToolFallback(
            AgentSpecialistKind.QueryAnalysis,
            Array.Empty<AgentToolExecutionRecord>(),
            1024,
            gaps);

        Assert.Contains("missing derived rate evidence", structured.AnalystNotes, StringComparison.Ordinal);
    }
}
