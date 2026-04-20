using MachineHealthExplorer.Agent.Services;
using MachineHealthExplorer.Domain.Models;
using MachineHealthExplorer.Tests.Infrastructure;
using MachineHealthExplorer.Tools.Services;

namespace MachineHealthExplorer.Tests;

public sealed class GroupByBinsDatasetTests
{
    [Fact]
    public async Task GroupByBins_Ai4i_AirAndProcessBins_MatchReferenceCounts()
    {
        var repository = TestDatasetFactory.CreateRepository();
        var engine = TestDatasetFactory.CreateAnalyticsEngine(repository);

        var air = await engine.GroupAndAggregateAsync(new GroupAggregationRequest
        {
            GroupByBins =
            [
                new NumericGroupBinSpec { ColumnName = "Air temperature [K]", Alias = "air_temp_bin", BinWidth = 1 }
            ],
            Aggregations =
            [
                DatasetAggregations.Count("row_count"),
                DatasetAggregations.Count("failure_count", DatasetFilters.Equal("Machine failure", true))
            ],
            SortRules = [DatasetSorts.Descending("failure_count")],
            Page = 1,
            PageSize = 500
        });

        var airRow = air.Rows.Single(r => NearlyEqual(r.Values["air_temp_bin"], 302d));
        Assert.Equal(89, Convert.ToInt32(airRow.Values["failure_count"]));
        Assert.Equal(1180, Convert.ToInt32(airRow.Values["row_count"]));

        var process = await engine.GroupAndAggregateAsync(new GroupAggregationRequest
        {
            GroupByBins =
            [
                new NumericGroupBinSpec { ColumnName = "Process temperature [K]", Alias = "proc_temp_bin", BinWidth = 1 }
            ],
            Aggregations =
            [
                DatasetAggregations.Count("row_count"),
                DatasetAggregations.Count("failure_count", DatasetFilters.Equal("Machine failure", true))
            ],
            SortRules = [DatasetSorts.Descending("failure_count")],
            Page = 1,
            PageSize = 500
        });

        var procRow = process.Rows.Single(r => NearlyEqual(r.Values["proc_temp_bin"], 310d));
        Assert.Equal(120, Convert.ToInt32(procRow.Values["failure_count"]));
        Assert.Equal(2456, Convert.ToInt32(procRow.Values["row_count"]));
    }

    private static bool NearlyEqual(object? raw, double expected)
    {
        if (raw is null)
        {
            return false;
        }

        var v = Convert.ToDouble(raw);
        return Math.Abs(v - expected) < 1e-6;
    }

    [Fact]
    public async Task DatasetAgentToolRuntime_GetSchema_OmitsNumericSummaryAndSampleValues()
    {
        var repository = TestDatasetFactory.CreateRepository();
        var toolService = TestDatasetFactory.CreateToolService(repository);
        var runtime = new DatasetAgentToolRuntime(new DatasetToolCatalog(), toolService);

        var record = await runtime.ExecuteAsync("get_schema", "{}", CancellationToken.None);

        Assert.False(record.IsError);
        Assert.DoesNotContain("numericSummary", record.ResultJson, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("sampleValues", record.ResultJson, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("categoricalSummary", record.ResultJson, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("Air temperature [K]", record.ResultJson, StringComparison.Ordinal);
        Assert.Contains("\"isNumeric\":", record.ResultJson, StringComparison.Ordinal);
    }

    [Fact]
    public async Task DatasetAgentToolRuntime_GroupAndAggregate_UnknownProperties_ReturnsError()
    {
        var repository = TestDatasetFactory.CreateRepository();
        var toolService = TestDatasetFactory.CreateToolService(repository);
        var runtime = new DatasetAgentToolRuntime(new DatasetToolCatalog(), toolService);

        var record = await runtime.ExecuteAsync(
            "group_and_aggregate",
            """{"column_to_group":"temperature","metrics":[{"aggregation":"count","column":"failure_id"}]}""",
            CancellationToken.None);

        Assert.True(record.IsError);
        Assert.Contains("column_to_group", record.ResultJson, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task DatasetAgentToolRuntime_GroupAndAggregate_EmptyAggregations_ReturnsError()
    {
        var repository = TestDatasetFactory.CreateRepository();
        var toolService = TestDatasetFactory.CreateToolService(repository);
        var runtime = new DatasetAgentToolRuntime(new DatasetToolCatalog(), toolService);

        var record = await runtime.ExecuteAsync("group_and_aggregate", "{}", CancellationToken.None);

        Assert.True(record.IsError);
        Assert.Contains("aggregations", record.ResultJson, StringComparison.OrdinalIgnoreCase);
    }
}
