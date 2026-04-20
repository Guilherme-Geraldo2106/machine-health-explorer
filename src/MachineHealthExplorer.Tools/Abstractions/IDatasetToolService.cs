using MachineHealthExplorer.Domain.Models;

namespace MachineHealthExplorer.Tools.Abstractions;

public interface IDatasetToolService
{
    Task<DatasetSchema> GetSchemaAsync(CancellationToken cancellationToken = default);
    Task<DatasetDescription> DescribeDatasetAsync(CancellationToken cancellationToken = default);
    Task<ColumnProfile> GetColumnProfileAsync(string columnName, CancellationToken cancellationToken = default);
    Task<ColumnProfilingResult> ProfileColumnsAsync(ColumnProfilingRequest request, CancellationToken cancellationToken = default);
    Task<QueryResult> QueryRowsAsync(QueryRequest request, CancellationToken cancellationToken = default);
    Task<GroupAggregationResult> GroupAndAggregateAsync(GroupAggregationRequest request, CancellationToken cancellationToken = default);
    Task<DistinctValuesResult> GetDistinctValuesAsync(DistinctValuesRequest request, CancellationToken cancellationToken = default);
    Task<SearchColumnsResult> SearchColumnsAsync(string keyword, CancellationToken cancellationToken = default);
}
