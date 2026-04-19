using MachineHealthExplorer.Domain.Abstractions;
using MachineHealthExplorer.Domain.Models;
using MachineHealthExplorer.Tests.Infrastructure;
using MachineHealthExplorer.Tools.Services;

namespace MachineHealthExplorer.Tests;

public sealed class DatasetToolServiceTests
{
    [Fact]
    public async Task ToolLayer_ReturnsFailureAnalysisAndReportsFromRealDataset()
    {
        var repository = TestDatasetFactory.CreateRepository();
        var toolService = TestDatasetFactory.CreateToolService(repository);

        var failureAnalysis = await toolService.GetFailureAnalysisAsync();
        var executiveReport = await toolService.BuildExecutiveReportAsync();
        var customReport = await toolService.BuildReportAsync(new ReportRequest
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
        Assert.Contains(failureAnalysis.FailureModes, mode => mode.Value == "HDF");
        Assert.Equal("Executive Dataset Overview", executiveReport.Title);
        Assert.NotEmpty(executiveReport.Sections);
        Assert.Equal("Failure Slice", customReport.Title);
        Assert.Contains(customReport.Sections, section => section.Heading == "Grouped view");
    }

    [Fact]
    public async Task ToolLayer_ExposesRunnableAnalysisExamples()
    {
        var repository = TestDatasetFactory.CreateRepository();
        var toolService = TestDatasetFactory.CreateToolService(repository);

        var examples = await toolService.GetAnalysisExamplesAsync();

        Assert.Contains(examples, example => example.Name == "high-stress-failures" && example.Kind == AnalysisExampleKind.RowQuery);
        Assert.Contains(examples, example => example.Name == "failure-rate-by-type" && example.Kind == AnalysisExampleKind.GroupAggregation);
        Assert.Contains(examples, example => example.Name == "high-wear-failed-vs-healthy" && example.Kind == AnalysisExampleKind.SubsetComparison);

        foreach (var example in examples)
        {
            switch (example.Kind)
            {
                case AnalysisExampleKind.RowQuery:
                    var queryResult = await toolService.QueryRowsAsync(example.RowQuery!);
                    Assert.True(queryResult.TotalCount > 0);
                    Assert.NotEmpty(queryResult.Rows);
                    break;
                case AnalysisExampleKind.GroupAggregation:
                    var groupResult = await toolService.GroupAndAggregateAsync(example.GroupAggregationQuery!);
                    Assert.True(groupResult.TotalGroups > 0);
                    Assert.All(groupResult.Rows, row => Assert.True(Convert.ToInt32(row.Values["failure_count"]) > 0));
                    break;
                case AnalysisExampleKind.SubsetComparison:
                    var comparisonResult = await toolService.CompareSubsetsAsync(example.ComparisonQuery!);
                    Assert.True(comparisonResult.Left.RowCount > 0);
                    Assert.True(comparisonResult.Right.RowCount > 0);
                    break;
                default:
                    throw new InvalidOperationException($"Unexpected example kind '{example.Kind}'.");
            }
        }
    }

    [Fact]
    public async Task ToolLayer_IsThinWrapperOverReusableServices()
    {
        var analyticsEngine = new StubDatasetAnalyticsEngine();
        var machineHealthAnalytics = new StubMachineHealthAnalyticsService();
        var toolService = new DatasetToolService(analyticsEngine, machineHealthAnalytics);

        var queryRequest = new QueryRequest { Page = 1, PageSize = 1 };
        var extremaRequest = new ColumnExtremaRequest { ColumnName = "Air temperature [K]" };
        var groupRequest = new GroupAggregationRequest { Page = 1, PageSize = 1 };
        var distinctRequest = new DistinctValuesRequest { ColumnName = "Type" };
        var comparisonRequest = new SubsetComparisonRequest();
        var profilingRequest = new ColumnProfilingRequest();
        var reportRequest = new ReportRequest();

        Assert.Same(analyticsEngine.SchemaResult, await toolService.GetSchemaAsync());
        Assert.Same(analyticsEngine.DatasetDescriptionResult, await toolService.DescribeDatasetAsync());
        Assert.Same(analyticsEngine.ColumnProfileResult, await toolService.GetColumnProfileAsync("Type"));
        Assert.Same(analyticsEngine.ColumnProfilingResult, await toolService.ProfileColumnsAsync(profilingRequest));
        Assert.Same(analyticsEngine.QueryRowsResult, await toolService.QueryRowsAsync(queryRequest));
        Assert.Same(analyticsEngine.ColumnExtremaResult, await toolService.FindColumnExtremaRowsAsync(extremaRequest));
        Assert.Same(analyticsEngine.GroupResult, await toolService.GroupAndAggregateAsync(groupRequest));
        Assert.Same(analyticsEngine.DistinctValuesResult, await toolService.GetDistinctValuesAsync(distinctRequest));
        Assert.Same(analyticsEngine.ComparisonResult, await toolService.CompareSubsetsAsync(comparisonRequest));
        Assert.Same(analyticsEngine.SearchResult, await toolService.SearchColumnsAsync("torque"));
        Assert.Same(analyticsEngine.ReportResult, await toolService.BuildReportAsync(reportRequest));
        Assert.Same(machineHealthAnalytics.FailureAnalysisResult, await toolService.GetFailureAnalysisAsync());
        Assert.Same(machineHealthAnalytics.FailureComparisonResult, await toolService.CompareFailureCohortsAsync());
        Assert.Same(machineHealthAnalytics.FailureModesResult, await toolService.GetFailureModesAsync());
        Assert.Same(machineHealthAnalytics.OperatingConditionSummaryResult, await toolService.GetOperatingConditionSummaryAsync());
        Assert.Same(machineHealthAnalytics.ExecutiveReportResult, await toolService.BuildExecutiveReportAsync());
        Assert.Same(machineHealthAnalytics.AnalysisExamplesResult, await toolService.GetAnalysisExamplesAsync());
        Assert.Same(queryRequest, analyticsEngine.LastQueryRequest);
        Assert.Same(extremaRequest, analyticsEngine.LastExtremaRequest);
        Assert.Same(groupRequest, analyticsEngine.LastGroupRequest);
        Assert.Same(distinctRequest, analyticsEngine.LastDistinctRequest);
        Assert.Same(comparisonRequest, analyticsEngine.LastComparisonRequest);
        Assert.Same(profilingRequest, analyticsEngine.LastProfilingRequest);
        Assert.Same(reportRequest, analyticsEngine.LastReportRequest);
    }

    private sealed class StubDatasetAnalyticsEngine : IDatasetAnalyticsEngine
    {
        public DatasetSchema SchemaResult { get; } = new() { DatasetName = "stub" };
        public DatasetDescription DatasetDescriptionResult { get; } = new() { DatasetName = "stub" };
        public ColumnProfile ColumnProfileResult { get; } = new() { ColumnName = "Type" };
        public ColumnProfilingResult ColumnProfilingResult { get; } = new();
        public QueryResult QueryRowsResult { get; } = new() { Page = 1, PageSize = 1 };
        public ColumnExtremaResult ColumnExtremaResult { get; } = new() { ColumnName = "stub", Mode = "max" };
        public GroupAggregationResult GroupResult { get; } = new() { Page = 1, PageSize = 1 };
        public DistinctValuesResult DistinctValuesResult { get; } = new() { ColumnName = "Type" };
        public SubsetComparisonResult ComparisonResult { get; } = new();
        public SearchColumnsResult SearchResult { get; } = new() { Keyword = "torque" };
        public DatasetReport ReportResult { get; } = new() { Title = "stub" };
        public NumericSummary NumericSummaryResult { get; } = new() { ColumnName = "Torque [Nm]" };
        public CategoricalSummary CategoricalSummaryResult { get; } = new() { ColumnName = "Type" };

        public QueryRequest? LastQueryRequest { get; private set; }
        public ColumnExtremaRequest? LastExtremaRequest { get; private set; }
        public GroupAggregationRequest? LastGroupRequest { get; private set; }
        public DistinctValuesRequest? LastDistinctRequest { get; private set; }
        public SubsetComparisonRequest? LastComparisonRequest { get; private set; }
        public ColumnProfilingRequest? LastProfilingRequest { get; private set; }
        public ReportRequest? LastReportRequest { get; private set; }

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
        {
            LastExtremaRequest = request;
            return Task.FromResult(ColumnExtremaResult);
        }

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
        {
            LastComparisonRequest = request;
            return Task.FromResult(ComparisonResult);
        }

        public Task<DatasetDescription> DescribeDatasetAsync(CancellationToken cancellationToken = default)
            => Task.FromResult(DatasetDescriptionResult);

        public Task<DatasetReport> BuildReportAsync(ReportRequest request, CancellationToken cancellationToken = default)
        {
            LastReportRequest = request;
            return Task.FromResult(ReportResult);
        }

        public Task<ColumnProfilingResult> ProfileColumnsAsync(ColumnProfilingRequest request, CancellationToken cancellationToken = default)
        {
            LastProfilingRequest = request;
            return Task.FromResult(ColumnProfilingResult);
        }
    }

    private sealed class StubMachineHealthAnalyticsService : IMachineHealthAnalyticsService
    {
        public FailureAnalysisSummary FailureAnalysisResult { get; } = new() { FailureIndicatorColumn = "Machine failure" };
        public SubsetComparisonResult FailureComparisonResult { get; } = new();
        public IReadOnlyList<ValueFrequency> FailureModesResult { get; } = [new ValueFrequency { Value = "HDF", Count = 1 }];
        public OperatingConditionSummary OperatingConditionSummaryResult { get; } = new();
        public DatasetReport ExecutiveReportResult { get; } = new() { Title = "Executive Dataset Overview" };
        public IReadOnlyList<AnalysisExample> AnalysisExamplesResult { get; } = [new AnalysisExample { Name = "example" }];

        public Task<FailureAnalysisSummary> GetFailureAnalysisAsync(CancellationToken cancellationToken = default)
            => Task.FromResult(FailureAnalysisResult);

        public Task<SubsetComparisonResult> CompareFailureCohortsAsync(CancellationToken cancellationToken = default)
            => Task.FromResult(FailureComparisonResult);

        public Task<IReadOnlyList<ValueFrequency>> GetFailureModesAsync(CancellationToken cancellationToken = default)
            => Task.FromResult(FailureModesResult);

        public Task<OperatingConditionSummary> GetOperatingConditionSummaryAsync(FilterExpression? filter = null, CancellationToken cancellationToken = default)
            => Task.FromResult(OperatingConditionSummaryResult);

        public Task<DatasetReport> BuildExecutiveReportAsync(CancellationToken cancellationToken = default)
            => Task.FromResult(ExecutiveReportResult);

        public Task<IReadOnlyList<AnalysisExample>> GetAnalysisExamplesAsync(CancellationToken cancellationToken = default)
            => Task.FromResult(AnalysisExamplesResult);
    }
}
