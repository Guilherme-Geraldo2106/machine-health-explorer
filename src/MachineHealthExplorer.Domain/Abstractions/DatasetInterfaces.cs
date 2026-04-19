using MachineHealthExplorer.Domain.Models;

namespace MachineHealthExplorer.Domain.Abstractions;

public interface IDatasetRepository
{
    Task<DatasetSnapshot> GetDatasetAsync(CancellationToken cancellationToken = default);
}

public interface IDatasetSchemaProvider
{
    Task<DatasetSchema> GetSchemaAsync(CancellationToken cancellationToken = default);
    Task<ColumnProfile> GetColumnProfileAsync(string columnName, CancellationToken cancellationToken = default);
    Task<SearchColumnsResult> SearchColumnsAsync(string keyword, CancellationToken cancellationToken = default);
}

public interface IDatasetQueryEngine
{
    Task<QueryResult> QueryRowsAsync(QueryRequest request, CancellationToken cancellationToken = default);
    Task<DistinctValuesResult> GetDistinctValuesAsync(DistinctValuesRequest request, CancellationToken cancellationToken = default);
    Task<GroupAggregationResult> GroupAndAggregateAsync(GroupAggregationRequest request, CancellationToken cancellationToken = default);
    Task<ColumnExtremaResult> FindColumnExtremaRowsAsync(ColumnExtremaRequest request, CancellationToken cancellationToken = default);
}

public interface IDatasetAnalyticsService
{
    Task<NumericSummary> GetNumericSummaryAsync(string columnName, FilterExpression? filter = null, CancellationToken cancellationToken = default);
    Task<CategoricalSummary> GetCategoricalSummaryAsync(string columnName, FilterExpression? filter = null, int top = 10, CancellationToken cancellationToken = default);
    Task<SubsetComparisonResult> CompareSubsetsAsync(SubsetComparisonRequest request, CancellationToken cancellationToken = default);
    Task<DatasetDescription> DescribeDatasetAsync(CancellationToken cancellationToken = default);
    Task<DatasetReport> BuildReportAsync(ReportRequest request, CancellationToken cancellationToken = default);
}

public interface IDatasetAnalyticsEngine : IDatasetSchemaProvider, IDatasetQueryEngine, IDatasetAnalyticsService
{
    Task<ColumnProfilingResult> ProfileColumnsAsync(ColumnProfilingRequest request, CancellationToken cancellationToken = default);
}

public interface IMachineHealthAnalyticsService
{
    Task<FailureAnalysisSummary> GetFailureAnalysisAsync(CancellationToken cancellationToken = default);
    Task<SubsetComparisonResult> CompareFailureCohortsAsync(CancellationToken cancellationToken = default);
    Task<IReadOnlyList<ValueFrequency>> GetFailureModesAsync(CancellationToken cancellationToken = default);
    Task<OperatingConditionSummary> GetOperatingConditionSummaryAsync(FilterExpression? filter = null, CancellationToken cancellationToken = default);
    Task<DatasetReport> BuildExecutiveReportAsync(CancellationToken cancellationToken = default);
    Task<IReadOnlyList<AnalysisExample>> GetAnalysisExamplesAsync(CancellationToken cancellationToken = default);
}
