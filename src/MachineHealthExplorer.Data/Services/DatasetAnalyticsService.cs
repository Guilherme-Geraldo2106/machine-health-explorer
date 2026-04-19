using MachineHealthExplorer.Domain.Abstractions;
using MachineHealthExplorer.Domain.Models;

namespace MachineHealthExplorer.Data.Services;

public sealed class DatasetAnalyticsService : IDatasetAnalyticsService
{
    private readonly IDatasetAnalyticsEngine _engine;

    public DatasetAnalyticsService(IDatasetRepository repository)
        : this(new DatasetAnalyticsEngine(repository))
    {
    }

    public DatasetAnalyticsService(IDatasetAnalyticsEngine engine)
    {
        _engine = engine ?? throw new ArgumentNullException(nameof(engine));
    }

    public Task<NumericSummary> GetNumericSummaryAsync(string columnName, FilterExpression? filter = null, CancellationToken cancellationToken = default)
        => _engine.GetNumericSummaryAsync(columnName, filter, cancellationToken);

    public Task<CategoricalSummary> GetCategoricalSummaryAsync(string columnName, FilterExpression? filter = null, int top = 10, CancellationToken cancellationToken = default)
        => _engine.GetCategoricalSummaryAsync(columnName, filter, top, cancellationToken);

    public Task<SubsetComparisonResult> CompareSubsetsAsync(SubsetComparisonRequest request, CancellationToken cancellationToken = default)
        => _engine.CompareSubsetsAsync(request, cancellationToken);

    public Task<DatasetDescription> DescribeDatasetAsync(CancellationToken cancellationToken = default)
        => _engine.DescribeDatasetAsync(cancellationToken);

    public Task<DatasetReport> BuildReportAsync(ReportRequest request, CancellationToken cancellationToken = default)
        => _engine.BuildReportAsync(request, cancellationToken);
}
