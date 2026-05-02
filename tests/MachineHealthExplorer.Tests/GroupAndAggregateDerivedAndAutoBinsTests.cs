using System.Text.Json;
using MachineHealthExplorer.Agent.Services;
using MachineHealthExplorer.Data.Services;
using MachineHealthExplorer.Domain.Models;
using MachineHealthExplorer.Tools.Services;
using MachineHealthExplorer.Tests.Infrastructure;

namespace MachineHealthExplorer.Tests;

public sealed class GroupAndAggregateDerivedAndAutoBinsTests
{
    [Fact]
    public async Task DerivedMetrics_EventRate_SortAndHaving_Work()
    {
        var engine = TestDatasetFactory.CreateAnalyticsEngine(TestDatasetFactory.CreateRepository());

        var result = await engine.GroupAndAggregateAsync(new GroupAggregationRequest
        {
            GroupByColumns = ["Type"],
            Aggregations =
            [
                new AggregationDefinition { Alias = "row_count", Function = AggregateFunction.Count },
                new AggregationDefinition
                {
                    Alias = "event_count",
                    Function = AggregateFunction.Count,
                    Filter = DatasetFilters.Equal("Machine failure", true)
                },
                new AggregationDefinition
                {
                    Alias = "avg_metric",
                    ColumnName = "Torque [Nm]",
                    Function = AggregateFunction.Average
                }
            ],
            DerivedMetrics =
            [
                new DerivedMetricDefinition { Alias = "event_rate", Expression = "event_count / row_count" }
            ],
            SortRules = [DatasetSorts.Descending("event_rate")],
            Page = 1,
            PageSize = 20
        });

        Assert.Contains("event_rate", result.Columns, StringComparer.OrdinalIgnoreCase);
        Assert.True(result.Rows.Count > 0);
        var rates = result.Rows.Select(r => Convert.ToDouble(r.Values["event_rate"])).ToArray();
        Assert.True(rates.SequenceEqual(rates.OrderByDescending(x => x)));
    }

    [Fact]
    public async Task DerivedMetrics_HavingOnDerivedMetric_Filters()
    {
        var engine = TestDatasetFactory.CreateAnalyticsEngine(TestDatasetFactory.CreateRepository());

        var result = await engine.GroupAndAggregateAsync(new GroupAggregationRequest
        {
            GroupByColumns = ["Type"],
            Aggregations =
            [
                new AggregationDefinition { Alias = "row_count", Function = AggregateFunction.Count },
                new AggregationDefinition
                {
                    Alias = "event_count",
                    Function = AggregateFunction.Count,
                    Filter = DatasetFilters.Equal("Machine failure", true)
                }
            ],
            DerivedMetrics =
            [
                new DerivedMetricDefinition { Alias = "event_rate", Expression = "event_count / row_count" }
            ],
            Having = DatasetFilters.GreaterThan("event_rate", 0.001),
            Page = 1,
            PageSize = 50
        });

        Assert.All(result.Rows, row => Assert.True(Convert.ToDouble(row.Values["event_rate"]) > 0.001));
    }

    [Fact]
    public async Task DerivedMetrics_DivisionByZero_YieldsNull()
    {
        var engine = TestDatasetFactory.CreateAnalyticsEngine(TestDatasetFactory.CreateRepository());

        var result = await engine.GroupAndAggregateAsync(new GroupAggregationRequest
        {
            GroupByColumns = ["Type"],
            Aggregations =
            [
                new AggregationDefinition { Alias = "row_count", Function = AggregateFunction.Count }
            ],
            DerivedMetrics =
            [
                new DerivedMetricDefinition { Alias = "ratio", Expression = "1 / 0" }
            ],
            Page = 1,
            PageSize = 50
        });

        var row = result.Rows.First();
        Assert.True(row.Values.TryGetValue("ratio", out var ratio));
        Assert.Null(ratio);
    }

    [Fact]
    public async Task DerivedMetrics_UnknownIdentifier_Throws()
    {
        var engine = TestDatasetFactory.CreateAnalyticsEngine(TestDatasetFactory.CreateRepository());
        var ex = await Assert.ThrowsAsync<ArgumentException>(() => engine.GroupAndAggregateAsync(new GroupAggregationRequest
        {
            GroupByColumns = ["Type"],
            Aggregations =
            [
                new AggregationDefinition { Alias = "row_count", Function = AggregateFunction.Count }
            ],
            DerivedMetrics =
            [
                new DerivedMetricDefinition { Alias = "x", Expression = "missing / row_count" }
            ]
        }));
        Assert.Contains("unknown identifier", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task DerivedMetrics_InvalidToken_Throws()
    {
        var engine = TestDatasetFactory.CreateAnalyticsEngine(TestDatasetFactory.CreateRepository());
        var ex = await Assert.ThrowsAsync<ArgumentException>(() => engine.GroupAndAggregateAsync(new GroupAggregationRequest
        {
            GroupByColumns = ["Type"],
            Aggregations =
            [
                new AggregationDefinition { Alias = "row_count", Function = AggregateFunction.Count }
            ],
            DerivedMetrics =
            [
                new DerivedMetricDefinition { Alias = "x", Expression = "row_count @ 2" }
            ]
        }));
        Assert.Contains("Unexpected token", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task DerivedMetrics_DuplicateAlias_Throws()
    {
        var engine = TestDatasetFactory.CreateAnalyticsEngine(TestDatasetFactory.CreateRepository());
        await Assert.ThrowsAsync<ArgumentException>(() => engine.GroupAndAggregateAsync(new GroupAggregationRequest
        {
            GroupByColumns = ["Type"],
            Aggregations =
            [
                new AggregationDefinition { Alias = "row_count", Function = AggregateFunction.Count }
            ],
            DerivedMetrics =
            [
                new DerivedMetricDefinition { Alias = "dup", Expression = "1" },
                new DerivedMetricDefinition { Alias = "dup", Expression = "2" }
            ]
        }));
    }

    [Fact]
    public async Task DerivedMetrics_CollidesWithAggregation_Throws()
    {
        var engine = TestDatasetFactory.CreateAnalyticsEngine(TestDatasetFactory.CreateRepository());
        await Assert.ThrowsAsync<ArgumentException>(() => engine.GroupAndAggregateAsync(new GroupAggregationRequest
        {
            GroupByColumns = ["Type"],
            Aggregations =
            [
                new AggregationDefinition { Alias = "row_count", Function = AggregateFunction.Count }
            ],
            DerivedMetrics =
            [
                new DerivedMetricDefinition { Alias = "row_count", Expression = "1" }
            ]
        }));
    }

    [Fact]
    public async Task MisleadingEventAlias_CountWithoutFilter_IsTotalRows()
    {
        var engine = TestDatasetFactory.CreateAnalyticsEngine(TestDatasetFactory.CreateRepository());

        var result = await engine.GroupAndAggregateAsync(new GroupAggregationRequest
        {
            GroupByColumns = ["Type"],
            Aggregations =
            [
                new AggregationDefinition { Alias = "event_count", Function = AggregateFunction.Count }
            ],
            Page = 1,
            PageSize = 5
        });

        var row = result.Rows.First(r => Equals(r.Values["Type"], "L"));
        var n = Convert.ToInt32(row.Values["event_count"]);
        Assert.True(n > 100);
    }

    [Fact]
    public async Task AutoBins_EqualWidth_WithGroupByColumns()
    {
        var engine = TestDatasetFactory.CreateAnalyticsEngine(TestDatasetFactory.CreateRepository());

        var result = await engine.GroupAndAggregateAsync(new GroupAggregationRequest
        {
            GroupByColumns = ["Type"],
            GroupByAutoBins =
            [
                new NumericGroupAutoBinSpec
                {
                    ColumnName = "Torque [Nm]",
                    Alias = "torque_bin",
                    Method = GroupByAutoBinMethod.EqualWidth,
                    BinCount = 5
                }
            ],
            Aggregations =
            [
                new AggregationDefinition { Alias = "row_count", Function = AggregateFunction.Count }
            ],
            Page = 1,
            PageSize = 200
        });

        Assert.Contains("torque_bin", result.Columns, StringComparer.OrdinalIgnoreCase);
        Assert.True(result.TotalGroups > 0);
        Assert.Single(result.GroupByAutoBinsApplied);
        var applied = result.GroupByAutoBinsApplied[0];
        Assert.Equal("EqualWidth", applied.Method, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("Torque [Nm]", applied.ColumnName, StringComparer.Ordinal);
        Assert.NotEmpty(applied.ObservedBins);
        foreach (var band in applied.ObservedBins)
        {
            Assert.True(band.RowCount > 0);
            Assert.True(band.UpperBound is null || band.UpperBound >= band.LowerBound);
        }
    }

    [Fact]
    public async Task AutoBins_Quantile_NeutralLowerBounds()
    {
        var engine = TestDatasetFactory.CreateAnalyticsEngine(TestDatasetFactory.CreateRepository());

        var result = await engine.GroupAndAggregateAsync(new GroupAggregationRequest
        {
            GroupByAutoBins =
            [
                new NumericGroupAutoBinSpec
                {
                    ColumnName = "Air temperature [K]",
                    Alias = "air_q",
                    Method = GroupByAutoBinMethod.Quantile,
                    BinCount = 4
                }
            ],
            Aggregations =
            [
                new AggregationDefinition { Alias = "row_count", Function = AggregateFunction.Count }
            ],
            Page = 1,
            PageSize = 200
        });

        Assert.Contains("air_q", result.Columns, StringComparer.OrdinalIgnoreCase);
        var applied = result.GroupByAutoBinsApplied[0];
        Assert.Equal("Quantile", applied.Method, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("Air temperature [K]", applied.ColumnName, StringComparer.Ordinal);
        Assert.NotEmpty(applied.ObservedBins);
        foreach (var band in applied.ObservedBins)
        {
            Assert.True(band.RowCount > 0);
            Assert.True(band.UpperBound is null || band.UpperBound >= band.LowerBound);
        }
    }

    [Fact]
    public async Task AutoBins_NonNumericColumn_Throws()
    {
        var engine = TestDatasetFactory.CreateAnalyticsEngine(TestDatasetFactory.CreateRepository());
        await Assert.ThrowsAsync<ArgumentException>(() => engine.GroupAndAggregateAsync(new GroupAggregationRequest
        {
            GroupByAutoBins =
            [
                new NumericGroupAutoBinSpec
                {
                    ColumnName = "Type",
                    Alias = "bad",
                    Method = GroupByAutoBinMethod.EqualWidth,
                    BinCount = 5
                }
            ],
            Aggregations =
            [
                new AggregationDefinition { Alias = "row_count", Function = AggregateFunction.Count }
            ]
        }));
    }

    [Fact]
    public async Task AutoBins_BinCountOutOfRange_Throws()
    {
        var engine = TestDatasetFactory.CreateAnalyticsEngine(TestDatasetFactory.CreateRepository());
        await Assert.ThrowsAsync<ArgumentException>(() => engine.GroupAndAggregateAsync(new GroupAggregationRequest
        {
            GroupByAutoBins =
            [
                new NumericGroupAutoBinSpec
                {
                    ColumnName = "Torque [Nm]",
                    Alias = "b",
                    Method = GroupByAutoBinMethod.EqualWidth,
                    BinCount = 150
                }
            ],
            Aggregations =
            [
                new AggregationDefinition { Alias = "row_count", Function = AggregateFunction.Count }
            ]
        }));
    }

    [Fact]
    public async Task AutoBins_AliasCollidesWithAggregation_Throws()
    {
        var engine = TestDatasetFactory.CreateAnalyticsEngine(TestDatasetFactory.CreateRepository());
        await Assert.ThrowsAsync<ArgumentException>(() => engine.GroupAndAggregateAsync(new GroupAggregationRequest
        {
            GroupByAutoBins =
            [
                new NumericGroupAutoBinSpec
                {
                    ColumnName = "Torque [Nm]",
                    Alias = "row_count",
                    Method = GroupByAutoBinMethod.EqualWidth,
                    BinCount = 5
                }
            ],
            Aggregations =
            [
                new AggregationDefinition { Alias = "row_count", Function = AggregateFunction.Count }
            ]
        }));
    }

    [Fact]
    public void DatasetAgentToolRuntime_Schema_ContainsDerivedAndAutoBins()
    {
        var engine = TestDatasetFactory.CreateAnalyticsEngine(TestDatasetFactory.CreateRepository());
        var toolService = TestDatasetFactory.CreateToolService(TestDatasetFactory.CreateRepository());
        var runtime = new DatasetAgentToolRuntime(new DatasetToolCatalog(), toolService);
        var schema = runtime.GetTools().Single(t => t.Name.Equals("group_and_aggregate", StringComparison.OrdinalIgnoreCase))
            .ParametersJsonSchema;
        using var doc = JsonDocument.Parse(schema);
        var props = doc.RootElement.GetProperty("properties");
        Assert.True(props.TryGetProperty("derivedMetrics", out var dm));
        Assert.Equal("array", dm.GetProperty("type").GetString());
        Assert.True(props.TryGetProperty("groupByAutoBins", out var ab));
        Assert.Equal("array", ab.GetProperty("type").GetString());
    }

    [Fact]
    public async Task GroupByColumns_Bins_AndAutoBins_Coexist()
    {
        var engine = TestDatasetFactory.CreateAnalyticsEngine(TestDatasetFactory.CreateRepository());

        var result = await engine.GroupAndAggregateAsync(new GroupAggregationRequest
        {
            GroupByColumns = ["Type"],
            GroupByBins =
            [
                new NumericGroupBinSpec
                {
                    ColumnName = "Torque [Nm]",
                    Alias = "torque_bw",
                    BinWidth = 5
                }
            ],
            GroupByAutoBins =
            [
                new NumericGroupAutoBinSpec
                {
                    ColumnName = "Air temperature [K]",
                    Alias = "air_auto",
                    Method = GroupByAutoBinMethod.EqualWidth,
                    BinCount = 4
                }
            ],
            Aggregations =
            [
                new AggregationDefinition { Alias = "row_count", Function = AggregateFunction.Count }
            ],
            Page = 1,
            PageSize = 50
        });

        Assert.Contains("torque_bw", result.Columns, StringComparer.OrdinalIgnoreCase);
        Assert.Contains("air_auto", result.Columns, StringComparer.OrdinalIgnoreCase);
        Assert.Single(result.GroupByAutoBinsApplied);
        Assert.True(result.TotalGroups > 0);
    }

    [Fact]
    public async Task AutoBins_AliasCollidesWithGroupByColumn_Throws()
    {
        var engine = TestDatasetFactory.CreateAnalyticsEngine(TestDatasetFactory.CreateRepository());
        await Assert.ThrowsAsync<ArgumentException>(() => engine.GroupAndAggregateAsync(new GroupAggregationRequest
        {
            GroupByColumns = ["Type"],
            GroupByAutoBins =
            [
                new NumericGroupAutoBinSpec
                {
                    ColumnName = "Torque [Nm]",
                    Alias = "Type",
                    Method = GroupByAutoBinMethod.EqualWidth,
                    BinCount = 4
                }
            ],
            Aggregations =
            [
                new AggregationDefinition { Alias = "row_count", Function = AggregateFunction.Count }
            ]
        }));
    }

    [Fact]
    public void EvidenceCompressor_PreservesDerivedMetricsSummary()
    {
        var args =
            """{"aggregations":[{"alias":"row_count","function":"Count"},{"alias":"event_count","function":"Count","filter":{"columnName":"Machine failure","operator":"Equals","value":true}}],"derivedMetrics":[{"alias":"event_rate","expression":"event_count / row_count"}]}""";
        var envelope = AgentToolEvidenceCompressor.BuildToolMessageContent(
            "group_and_aggregate",
            """{"columns":["Type","row_count","event_count","event_rate"],"rows":[]}""",
            maxChars: 4000,
            args);
        using var doc = JsonDocument.Parse(envelope);
        var root = doc.RootElement;
        Assert.True(root.TryGetProperty("aggregationRequestSummary", out var agg));
        Assert.Equal(2, agg.GetArrayLength());
        Assert.True(root.TryGetProperty("derivedMetricsSummary", out var d));
        Assert.Equal(1, d.GetArrayLength());
        Assert.Equal("event_rate", d[0].GetProperty("alias").GetString());
    }

    [Fact]
    public async Task PortugueseCausationStyle_GenericRatesAndBins_ProducesRateColumn()
    {
        const string userQuestion =
            "qual a combinação de fator que voce considera maior causador de falhas e desgaste na maquina ?";

        var engine = TestDatasetFactory.CreateAnalyticsEngine(TestDatasetFactory.CreateRepository());

        var result = await engine.GroupAndAggregateAsync(new GroupAggregationRequest
        {
            GroupByColumns = ["Type"],
            GroupByAutoBins =
            [
                new NumericGroupAutoBinSpec
                {
                    ColumnName = "Tool wear [min]",
                    Alias = "wear_bin",
                    Method = GroupByAutoBinMethod.Quantile,
                    BinCount = 5
                }
            ],
            Aggregations =
            [
                new AggregationDefinition { Alias = "row_count", Function = AggregateFunction.Count },
                new AggregationDefinition
                {
                    Alias = "event_count",
                    Function = AggregateFunction.Count,
                    Filter = DatasetFilters.Equal("Machine failure", true)
                },
                new AggregationDefinition
                {
                    Alias = "avg_torque",
                    ColumnName = "Torque [Nm]",
                    Function = AggregateFunction.Median
                }
            ],
            DerivedMetrics =
            [
                new DerivedMetricDefinition { Alias = "event_rate", Expression = "event_count / row_count" }
            ],
            SortRules = [DatasetSorts.Descending("event_rate")],
            Page = 1,
            PageSize = 10
        });

        Assert.False(string.IsNullOrWhiteSpace(userQuestion));
        Assert.Contains("event_rate", result.Columns, StringComparer.OrdinalIgnoreCase);
        Assert.True(result.Rows.Count > 0);
        var top = result.Rows[0];
        Assert.True(top.Values.ContainsKey("event_rate"));
        Assert.True(top.Values.ContainsKey("row_count"));
        Assert.True(top.Values.ContainsKey("event_count"));
    }
}
