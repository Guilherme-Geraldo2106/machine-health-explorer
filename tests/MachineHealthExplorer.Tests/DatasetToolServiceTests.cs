using MachineHealthExplorer.Domain.Abstractions;
using MachineHealthExplorer.Domain.Models;
using MachineHealthExplorer.Tests.Infrastructure;
using MachineHealthExplorer.Tools.Services;

namespace MachineHealthExplorer.Tests;

public sealed class DatasetToolServiceTests
{
    [Fact]
    public async Task ToolLayer_ExposesGenericDatasetOperationsOnRealDataset()
    {
        var repository = TestDatasetFactory.CreateRepository();
        var toolService = TestDatasetFactory.CreateToolService(repository);

        var schema = await toolService.GetSchemaAsync();
        var distinct = await toolService.GetDistinctValuesAsync(new DistinctValuesRequest { ColumnName = "Type", Limit = 10 });
        var grouped = await toolService.GroupAndAggregateAsync(new GroupAggregationRequest
        {
            GroupByColumns = ["Type"],
            Aggregations =
            [
                DatasetAggregations.Count("row_count"),
                DatasetAggregations.Count("failure_count", DatasetFilters.Equal("Machine failure", true))
            ],
            Page = 1,
            PageSize = 10
        });

        Assert.False(string.IsNullOrWhiteSpace(schema.DatasetName));
        Assert.True(distinct.TotalDistinctCount > 0);
        Assert.True(grouped.TotalGroups > 0);
    }

    [Fact]
    public async Task MachineHealthAnalytics_RemainsAvailableOutsideAgentToolSurface()
    {
        var repository = TestDatasetFactory.CreateRepository();
        var mh = TestDatasetFactory.CreateMachineHealthAnalyticsService(repository);
        var engine = TestDatasetFactory.CreateAnalyticsEngine(repository);

        var failureAnalysis = await mh.GetFailureAnalysisAsync();
        var customReport = await engine.BuildReportAsync(new ReportRequest
        {
            Title = "Failure Slice",
            BaseFilter = DatasetFilters.Equal("Machine failure", true),
            FocusColumns = ["Type", "Torque [Nm]"],
            GroupByColumns = ["Type"],
            Aggregations =
            [
                DatasetAggregations.Count("row_count"),
                DatasetAggregations.Average("avg_torque", "Torque [Nm]")
            ]
        });

        Assert.Equal("Machine failure", failureAnalysis.FailureIndicatorColumn);
        Assert.True(failureAnalysis.FailureCount > 0);
        Assert.Equal("Failure Slice", customReport.Title);
        Assert.Contains(customReport.Sections, section => section.Heading == "Grouped view");
    }

    [Fact]
    public async Task ToolLayer_IsThinWrapperOverAnalyticsEngine()
    {
        var analyticsEngine = new StubDatasetAnalyticsEngine();
        var toolService = new DatasetToolService(analyticsEngine);

        var queryRequest = new QueryRequest { Page = 1, PageSize = 1 };
        var groupRequest = new GroupAggregationRequest { Page = 1, PageSize = 1 };
        var distinctRequest = new DistinctValuesRequest { ColumnName = "Type" };
        var profilingRequest = new ColumnProfilingRequest();

        Assert.Same(analyticsEngine.SchemaResult, await toolService.GetSchemaAsync());
        Assert.Same(analyticsEngine.DatasetDescriptionResult, await toolService.DescribeDatasetAsync());
        Assert.Same(analyticsEngine.ColumnProfileResult, await toolService.GetColumnProfileAsync("Type"));
        Assert.Same(analyticsEngine.ColumnProfilingResult, await toolService.ProfileColumnsAsync(profilingRequest));
        Assert.Same(analyticsEngine.QueryRowsResult, await toolService.QueryRowsAsync(queryRequest));
        Assert.Same(analyticsEngine.GroupResult, await toolService.GroupAndAggregateAsync(groupRequest));
        Assert.Same(analyticsEngine.DistinctValuesResult, await toolService.GetDistinctValuesAsync(distinctRequest));
        Assert.Same(analyticsEngine.SearchResult, await toolService.SearchColumnsAsync("torque"));
        Assert.Same(queryRequest, analyticsEngine.LastQueryRequest);
        Assert.Same(groupRequest, analyticsEngine.LastGroupRequest);
        Assert.Same(distinctRequest, analyticsEngine.LastDistinctRequest);
        Assert.Same(profilingRequest, analyticsEngine.LastProfilingRequest);
    }

    private sealed class StubDatasetAnalyticsEngine : IDatasetAnalyticsEngine
    {
        public DatasetSchema SchemaResult { get; } = new() { DatasetName = "stub" };
        public DatasetDescription DatasetDescriptionResult { get; } = new() { DatasetName = "stub" };
        public ColumnProfile ColumnProfileResult { get; } = new() { ColumnName = "Type" };
        public ColumnProfilingResult ColumnProfilingResult { get; } = new();
        public QueryResult QueryRowsResult { get; } = new() { Page = 1, PageSize = 1 };
        public GroupAggregationResult GroupResult { get; } = new() { Page = 1, PageSize = 1 };
        public DistinctValuesResult DistinctValuesResult { get; } = new() { ColumnName = "Type" };
        public SearchColumnsResult SearchResult { get; } = new() { Keyword = "torque" };
        public NumericSummary NumericSummaryResult { get; } = new() { ColumnName = "Torque [Nm]" };
        public CategoricalSummary CategoricalSummaryResult { get; } = new() { ColumnName = "Type" };

        public QueryRequest? LastQueryRequest { get; private set; }
        public GroupAggregationRequest? LastGroupRequest { get; private set; }
        public DistinctValuesRequest? LastDistinctRequest { get; private set; }
        public ColumnProfilingRequest? LastProfilingRequest { get; private set; }

        public Task<DatasetSchema> GetSchemaAsync(CancellationToken cancellationToken = default)
            => Task.FromResult(SchemaResult);

        public Task<ColumnProfile> GetColumnProfileAsync(string columnName, CancellationToken cancellationToken = default)
            => Task.FromResult(ColumnProfileResult);

        public Task<SearchColumnsResult> SearchColumnsAsync(string keyword, CancellationToken cancellationToken = default)
            => Task.FromResult(SearchResult);

        public Task<QueryResult> QueryRowsAsync(QueryRequest request, CancellationToken cancellationToken = default)
        {
            LastQueryRequest = request;
            return Task.FromResult(QueryRowsResult);
        }

        public Task<ColumnExtremaResult> FindColumnExtremaRowsAsync(ColumnExtremaRequest request, CancellationToken cancellationToken = default)
            => Task.FromResult(new ColumnExtremaResult());

        public Task<DistinctValuesResult> GetDistinctValuesAsync(DistinctValuesRequest request, CancellationToken cancellationToken = default)
        {
            LastDistinctRequest = request;
            return Task.FromResult(DistinctValuesResult);
        }

        public Task<GroupAggregationResult> GroupAndAggregateAsync(GroupAggregationRequest request, CancellationToken cancellationToken = default)
        {
            LastGroupRequest = request;
            return Task.FromResult(GroupResult);
        }

        public Task<NumericSummary> GetNumericSummaryAsync(string columnName, FilterExpression? filter = null, CancellationToken cancellationToken = default)
            => Task.FromResult(NumericSummaryResult);

        public Task<CategoricalSummary> GetCategoricalSummaryAsync(string columnName, FilterExpression? filter = null, int top = 10, CancellationToken cancellationToken = default)
            => Task.FromResult(CategoricalSummaryResult);

        public Task<SubsetComparisonResult> CompareSubsetsAsync(SubsetComparisonRequest request, CancellationToken cancellationToken = default)
            => Task.FromResult(new SubsetComparisonResult());

        public Task<DatasetDescription> DescribeDatasetAsync(CancellationToken cancellationToken = default)
            => Task.FromResult(DatasetDescriptionResult);

        public Task<DatasetReport> BuildReportAsync(ReportRequest request, CancellationToken cancellationToken = default)
            => Task.FromResult(new DatasetReport());

        public Task<ColumnProfilingResult> ProfileColumnsAsync(ColumnProfilingRequest request, CancellationToken cancellationToken = default)
        {
            LastProfilingRequest = request;
            return Task.FromResult(ColumnProfilingResult);
        }
    }
}
