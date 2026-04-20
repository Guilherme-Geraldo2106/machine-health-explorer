using MachineHealthExplorer.Agent.Models;
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
}
