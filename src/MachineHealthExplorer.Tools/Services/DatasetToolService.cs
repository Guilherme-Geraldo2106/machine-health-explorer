using MachineHealthExplorer.Domain.Abstractions;
using MachineHealthExplorer.Domain.Models;
using MachineHealthExplorer.Tools.Abstractions;

namespace MachineHealthExplorer.Tools.Services;

public sealed class DatasetToolService : IDatasetToolService
{
    private readonly IDatasetAnalyticsEngine _analyticsEngine;
    private readonly IMachineHealthAnalyticsService _machineHealthAnalyticsService;

    public DatasetToolService(
        IDatasetAnalyticsEngine analyticsEngine,
        IMachineHealthAnalyticsService machineHealthAnalyticsService)
    {
        _analyticsEngine = analyticsEngine ?? throw new ArgumentNullException(nameof(analyticsEngine));
        _machineHealthAnalyticsService = machineHealthAnalyticsService ?? throw new ArgumentNullException(nameof(machineHealthAnalyticsService));
    }

    public Task<DatasetSchema> GetSchemaAsync(CancellationToken cancellationToken = default)
        => _analyticsEngine.GetSchemaAsync(cancellationToken);

    public Task<DatasetDescription> DescribeDatasetAsync(CancellationToken cancellationToken = default)
        => _analyticsEngine.DescribeDatasetAsync(cancellationToken);

    public Task<ColumnProfile> GetColumnProfileAsync(string columnName, CancellationToken cancellationToken = default)
        => _analyticsEngine.GetColumnProfileAsync(columnName, cancellationToken);

    public Task<ColumnProfilingResult> ProfileColumnsAsync(ColumnProfilingRequest request, CancellationToken cancellationToken = default)
        => _analyticsEngine.ProfileColumnsAsync(request, cancellationToken);

    public Task<QueryResult> QueryRowsAsync(QueryRequest request, CancellationToken cancellationToken = default)
        => _analyticsEngine.QueryRowsAsync(request, cancellationToken);

    public Task<ColumnExtremaResult> FindColumnExtremaRowsAsync(ColumnExtremaRequest request, CancellationToken cancellationToken = default)
        => _analyticsEngine.FindColumnExtremaRowsAsync(request, cancellationToken);

    public Task<GroupAggregationResult> GroupAndAggregateAsync(GroupAggregationRequest request, CancellationToken cancellationToken = default)
        => _analyticsEngine.GroupAndAggregateAsync(request, cancellationToken);

    public Task<DistinctValuesResult> GetDistinctValuesAsync(DistinctValuesRequest request, CancellationToken cancellationToken = default)
        => _analyticsEngine.GetDistinctValuesAsync(request, cancellationToken);

    public Task<SubsetComparisonResult> CompareSubsetsAsync(SubsetComparisonRequest request, CancellationToken cancellationToken = default)
        => _analyticsEngine.CompareSubsetsAsync(request, cancellationToken);

    public Task<SearchColumnsResult> SearchColumnsAsync(string keyword, CancellationToken cancellationToken = default)
        => _analyticsEngine.SearchColumnsAsync(keyword, cancellationToken);

    public Task<DatasetReport> BuildReportAsync(ReportRequest request, CancellationToken cancellationToken = default)
        => _analyticsEngine.BuildReportAsync(request, cancellationToken);

    public Task<FailureAnalysisSummary> GetFailureAnalysisAsync(CancellationToken cancellationToken = default)
        => _machineHealthAnalyticsService.GetFailureAnalysisAsync(cancellationToken);

    public Task<SubsetComparisonResult> CompareFailureCohortsAsync(CancellationToken cancellationToken = default)
        => _machineHealthAnalyticsService.CompareFailureCohortsAsync(cancellationToken);

    public Task<IReadOnlyList<ValueFrequency>> GetFailureModesAsync(CancellationToken cancellationToken = default)
        => _machineHealthAnalyticsService.GetFailureModesAsync(cancellationToken);

    public Task<OperatingConditionSummary> GetOperatingConditionSummaryAsync(FilterExpression? filter = null, CancellationToken cancellationToken = default)
        => _machineHealthAnalyticsService.GetOperatingConditionSummaryAsync(filter, cancellationToken);

    public Task<DatasetReport> BuildExecutiveReportAsync(CancellationToken cancellationToken = default)
        => _machineHealthAnalyticsService.BuildExecutiveReportAsync(cancellationToken);

    public Task<IReadOnlyList<AnalysisExample>> GetAnalysisExamplesAsync(CancellationToken cancellationToken = default)
        => _machineHealthAnalyticsService.GetAnalysisExamplesAsync(cancellationToken);
}
