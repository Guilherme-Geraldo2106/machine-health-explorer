using MachineHealthExplorer.Domain.Abstractions;
using MachineHealthExplorer.Domain.Models;
using MachineHealthExplorer.Tools.Abstractions;

namespace MachineHealthExplorer.Tools.Services;

public sealed class DatasetToolService : IDatasetToolService
{
    private readonly IDatasetAnalyticsEngine _analyticsEngine;

    public DatasetToolService(IDatasetAnalyticsEngine analyticsEngine)
    {
        _analyticsEngine = analyticsEngine ?? throw new ArgumentNullException(nameof(analyticsEngine));
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

    public Task<GroupAggregationResult> GroupAndAggregateAsync(GroupAggregationRequest request, CancellationToken cancellationToken = default)
        => _analyticsEngine.GroupAndAggregateAsync(request, cancellationToken);

    public Task<DistinctValuesResult> GetDistinctValuesAsync(DistinctValuesRequest request, CancellationToken cancellationToken = default)
        => _analyticsEngine.GetDistinctValuesAsync(request, cancellationToken);

    public Task<SearchColumnsResult> SearchColumnsAsync(string keyword, CancellationToken cancellationToken = default)
        => _analyticsEngine.SearchColumnsAsync(keyword, cancellationToken);
}
