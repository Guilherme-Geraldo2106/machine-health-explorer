using MachineHealthExplorer.Agent.MultiAgent;
using MachineHealthExplorer.Agent.Services;
using MachineHealthExplorer.Domain.Abstractions;
using MachineHealthExplorer.Domain.Models;
using MachineHealthExplorer.Tools.Services;
using System.Text.Json;

namespace MachineHealthExplorer.Tests;

/// <summary>
/// Guards model-facing metadata for group_and_aggregate: Count semantics, per-aggregation filters, and no hardcoded dataset column names.
/// </summary>
public sealed class GroupAndAggregateToolMetadataTests
{
    private static readonly string[] ForbiddenDatasetColumnSnippets =
    [
        "Machine failure",
        "Air temperature",
        "Process temperature"
    ];

    [Fact]
    public void GroupAndAggregate_CatalogDescription_ExplainsUnfilteredVsFilteredCount()
    {
        var catalog = new DatasetToolCatalog();
        var tool = catalog.GetTools().Single(t => t.Name.Equals("group_and_aggregate", StringComparison.OrdinalIgnoreCase));

        Assert.Contains("Count with no per-aggregation filter", tool.Description, StringComparison.Ordinal);
        Assert.Contains("per-aggregation filter", tool.Description, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("row_count", tool.Description, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("event_count", tool.Description, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("groupByBins", tool.Description, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("groupByAutoBins", tool.Description, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("derivedMetrics", tool.Description, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("numeric", tool.Description, StringComparison.OrdinalIgnoreCase);

        foreach (var forbidden in ForbiddenDatasetColumnSnippets)
        {
            Assert.DoesNotContain(forbidden, tool.Description, StringComparison.OrdinalIgnoreCase);
        }
    }

    [Fact]
    public void GroupAndAggregate_RuntimeJsonSchema_FilterIsPerAggregation_AndRootExplainsCount()
    {
        var engine = new MinimalStubAnalyticsEngine();
        var toolService = new DatasetToolService(engine);
        var runtime = new DatasetAgentToolRuntime(new DatasetToolCatalog(), toolService);
        var tool = runtime.GetTools().Single(t => t.Name.Equals("group_and_aggregate", StringComparison.OrdinalIgnoreCase));

        using var doc = JsonDocument.Parse(tool.ParametersJsonSchema);
        var root = doc.RootElement;
        Assert.True(root.TryGetProperty("description", out var rootDesc));
        var rootText = rootDesc.GetString() ?? string.Empty;
        Assert.Contains("per-aggregation filter", rootText, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("Count", rootText, StringComparison.Ordinal);

        var agItems = root.GetProperty("properties").GetProperty("aggregations").GetProperty("items");
        var filterDesc = agItems.GetProperty("properties").GetProperty("filter").GetProperty("description").GetString() ?? string.Empty;
        Assert.Contains("this aggregation", filterDesc, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("other aggregations", filterDesc, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("unfiltered", filterDesc, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("filtered Count", filterDesc, StringComparison.OrdinalIgnoreCase);

        foreach (var forbidden in ForbiddenDatasetColumnSnippets)
        {
            Assert.DoesNotContain(forbidden, tool.ParametersJsonSchema, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain(forbidden, tool.Description, StringComparison.OrdinalIgnoreCase);
        }
    }

    [Fact]
    public void GroupAndAggregate_CompactPromptContracts_UseGenericPlaceholders_AndAvoidForbiddenColumns()
    {
        var minimal = MultiAgentPromptBuilder.BuildMinimalToolParametersContractHint();
        var compact = MultiAgentPromptBuilder.BuildGroupAndAggregateCompactContractHint();

        Assert.Contains("<numeric column>", minimal, StringComparison.Ordinal);
        Assert.Contains("<boolean/event column>", minimal, StringComparison.Ordinal);
        Assert.Contains("\"row_count\"", minimal, StringComparison.Ordinal);
        Assert.Contains("\"event_count\"", minimal, StringComparison.Ordinal);
        Assert.Contains("Count with no per-aggregation", compact, StringComparison.Ordinal);
        Assert.Contains("derivedMetrics", compact, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("groupByAutoBins", compact, StringComparison.OrdinalIgnoreCase);

        foreach (var forbidden in ForbiddenDatasetColumnSnippets)
        {
            Assert.DoesNotContain(forbidden, minimal, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain(forbidden, compact, StringComparison.OrdinalIgnoreCase);
        }
    }

    [Fact]
    public void GroupAndAggregate_AllCatalogToolDescriptions_AvoidForbiddenDatasetColumns()
    {
        var catalog = new DatasetToolCatalog();
        foreach (var tool in catalog.GetTools())
        {
            foreach (var forbidden in ForbiddenDatasetColumnSnippets)
            {
                Assert.DoesNotContain(forbidden, tool.Description, StringComparison.OrdinalIgnoreCase);
            }
        }
    }

    private sealed class MinimalStubAnalyticsEngine : IDatasetAnalyticsEngine
    {
        public Task<DatasetSchema> GetSchemaAsync(CancellationToken cancellationToken = default)
            => Task.FromResult(new DatasetSchema { DatasetName = "stub" });

        public Task<ColumnProfile> GetColumnProfileAsync(string columnName, CancellationToken cancellationToken = default)
            => Task.FromResult(new ColumnProfile { ColumnName = columnName });

        public Task<SearchColumnsResult> SearchColumnsAsync(string keyword, CancellationToken cancellationToken = default)
            => Task.FromResult(new SearchColumnsResult { Keyword = keyword });

        public Task<QueryResult> QueryRowsAsync(QueryRequest request, CancellationToken cancellationToken = default)
            => Task.FromResult(new QueryResult());

        public Task<ColumnExtremaResult> FindColumnExtremaRowsAsync(ColumnExtremaRequest request, CancellationToken cancellationToken = default)
            => Task.FromResult(new ColumnExtremaResult());

        public Task<DistinctValuesResult> GetDistinctValuesAsync(DistinctValuesRequest request, CancellationToken cancellationToken = default)
            => Task.FromResult(new DistinctValuesResult { ColumnName = request.ColumnName });

        public Task<GroupAggregationResult> GroupAndAggregateAsync(GroupAggregationRequest request, CancellationToken cancellationToken = default)
            => Task.FromResult(new GroupAggregationResult());

        public Task<NumericSummary> GetNumericSummaryAsync(string columnName, FilterExpression? filter = null, CancellationToken cancellationToken = default)
            => Task.FromResult(new NumericSummary { ColumnName = columnName });

        public Task<CategoricalSummary> GetCategoricalSummaryAsync(string columnName, FilterExpression? filter = null, int top = 10, CancellationToken cancellationToken = default)
            => Task.FromResult(new CategoricalSummary { ColumnName = columnName });

        public Task<SubsetComparisonResult> CompareSubsetsAsync(SubsetComparisonRequest request, CancellationToken cancellationToken = default)
            => Task.FromResult(new SubsetComparisonResult());

        public Task<DatasetDescription> DescribeDatasetAsync(CancellationToken cancellationToken = default)
            => Task.FromResult(new DatasetDescription { DatasetName = "stub" });

        public Task<DatasetReport> BuildReportAsync(ReportRequest request, CancellationToken cancellationToken = default)
            => Task.FromResult(new DatasetReport());

        public Task<ColumnProfilingResult> ProfileColumnsAsync(ColumnProfilingRequest request, CancellationToken cancellationToken = default)
            => Task.FromResult(new ColumnProfilingResult());
    }
}
