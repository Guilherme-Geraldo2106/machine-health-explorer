using MachineHealthExplorer.Data.Services;
using MachineHealthExplorer.Domain.Abstractions;
using MachineHealthExplorer.Domain.Models;

namespace MachineHealthExplorer.Data.Querying;

public sealed class DatasetQueryEngine : IDatasetQueryEngine
{
    private readonly IDatasetAnalyticsEngine _engine;

    public DatasetQueryEngine(IDatasetRepository repository)
        : this(new DatasetAnalyticsEngine(repository))
    {
    }

    public DatasetQueryEngine(IDatasetAnalyticsEngine engine)
    {
        _engine = engine ?? throw new ArgumentNullException(nameof(engine));
    }

    public Task<QueryResult> QueryRowsAsync(QueryRequest request, CancellationToken cancellationToken = default)
        => _engine.QueryRowsAsync(request, cancellationToken);

    public Task<DistinctValuesResult> GetDistinctValuesAsync(DistinctValuesRequest request, CancellationToken cancellationToken = default)
        => _engine.GetDistinctValuesAsync(request, cancellationToken);

    public Task<GroupAggregationResult> GroupAndAggregateAsync(GroupAggregationRequest request, CancellationToken cancellationToken = default)
        => _engine.GroupAndAggregateAsync(request, cancellationToken);

    public Task<ColumnExtremaResult> FindColumnExtremaRowsAsync(ColumnExtremaRequest request, CancellationToken cancellationToken = default)
        => _engine.FindColumnExtremaRowsAsync(request, cancellationToken);
}
