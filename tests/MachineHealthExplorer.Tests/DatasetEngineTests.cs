using MachineHealthExplorer.Domain.Abstractions;
using MachineHealthExplorer.Domain.Models;
using MachineHealthExplorer.Tests.Infrastructure;

namespace MachineHealthExplorer.Tests;

public sealed class DatasetEngineTests
{
    [Fact]
    public async Task CsvLoadSuccess_LoadsExpectedRowCountAndColumns()
    {
        var repository = TestDatasetFactory.CreateRepository();

        var snapshot = await repository.GetDatasetAsync();

        Assert.Equal(10_000, snapshot.Rows.Count);
        Assert.Contains(snapshot.Schema.Columns, column => column.Name == "Machine failure");
        Assert.Contains(snapshot.Schema.Columns, column => column.Name == "Type");
        Assert.Contains(snapshot.Schema.Columns, column => column.Name == "Torque [Nm]");
    }

    [Fact]
    public async Task SchemaInference_InfersExpectedColumnKinds()
    {
        var repository = TestDatasetFactory.CreateRepository();
        var schema = await repository.GetSchemaAsync();

        var typeColumn = schema.Columns.Single(column => column.Name == "Type");
        var airTemperatureColumn = schema.Columns.Single(column => column.Name == "Air temperature [K]");
        var failureColumn = schema.Columns.Single(column => column.Name == "Machine failure");
        var productIdColumn = schema.Columns.Single(column => column.Name == "Product ID");

        Assert.Equal(DataTypeKind.String, typeColumn.DataType);
        Assert.True(typeColumn.IsCategorical);
        Assert.Equal(DataTypeKind.Decimal, airTemperatureColumn.DataType);
        Assert.True(airTemperatureColumn.IsNumeric);
        Assert.Equal(DataTypeKind.Boolean, failureColumn.DataType);
        Assert.Equal(DataTypeKind.String, productIdColumn.DataType);
    }

    [Fact]
    public async Task Filtering_SelectsFailedRowsAndSortsByToolWear()
    {
        var repository = TestDatasetFactory.CreateRepository();
        var engine = TestDatasetFactory.CreateQueryEngine(repository);

        var result = await engine.QueryRowsAsync(new QueryRequest
        {
            Filter = DatasetFilters.Equal("Machine failure", true),
            SelectedColumns = ["Machine failure", "Tool wear [min]"],
            SortRules = [DatasetSorts.Descending("Tool wear [min]")],
            Page = 1,
            PageSize = 5
        });

        Assert.True(result.TotalCount > 0);
        Assert.Equal(5, result.Rows.Count);
        Assert.All(result.Rows, row => Assert.Equal(true, row["Machine failure"]));
        var orderedWear = result.Rows.Select(row => Convert.ToDouble(row["Tool wear [min]"])).ToArray();
        Assert.True(orderedWear.SequenceEqual(orderedWear.OrderByDescending(value => value)));
    }

    [Fact]
    public async Task GroupingAndAggregation_ReturnsCountsByType()
    {
        var repository = TestDatasetFactory.CreateRepository();
        var engine = TestDatasetFactory.CreateQueryEngine(repository);

        var result = await engine.GroupAndAggregateAsync(new GroupAggregationRequest
        {
            GroupByColumns = ["Type"],
            Aggregations =
            [
                new AggregationDefinition
                {
                    Alias = "row_count",
                    Function = AggregateFunction.Count
                },
                new AggregationDefinition
                {
                    Alias = "avg_torque",
                    ColumnName = "Torque [Nm]",
                    Function = AggregateFunction.Average
                }
            ],
            SortRules =
            [
                new SortRule
                {
                    ColumnName = "row_count",
                    Direction = SortDirection.Descending
                }
            ],
            Page = 1,
            PageSize = 10
        });

        Assert.True(result.TotalGroups >= 3);
        Assert.Contains(result.Rows, row => Equals(row.Values["Type"], "L"));
        Assert.Contains(result.Rows, row => Equals(row.Values["Type"], "M"));
        Assert.Contains(result.Rows, row => Equals(row.Values["Type"], "H"));
        Assert.All(result.Rows, row => Assert.True(Convert.ToInt32(row.Values["row_count"]) > 0));
    }

    [Fact]
    public async Task ComparisonLogic_ComparesFailedAndHealthyRows()
    {
        var repository = TestDatasetFactory.CreateRepository();
        IDatasetAnalyticsService analytics = TestDatasetFactory.CreateAnalyticsService(repository);

        var result = await analytics.CompareSubsetsAsync(new SubsetComparisonRequest
        {
            LeftLabel = "Failed",
            LeftFilter = DatasetFilters.Equal("Machine failure", true),
            RightLabel = "Healthy",
            RightFilter = DatasetFilters.Equal("Machine failure", false),
            NumericColumns = ["Air temperature [K]", "Torque [Nm]"],
            CategoricalColumns = ["Type"]
        });

        Assert.True(result.Left.RowCount > 0);
        Assert.True(result.Right.RowCount > result.Left.RowCount);
        Assert.Equal(2, result.NumericComparisons.Count);
        Assert.Single(result.CategoricalComparisons);
    }

    [Fact]
    public async Task DistinctValueRetrieval_ReturnsKnownProductTypes()
    {
        var repository = TestDatasetFactory.CreateRepository();
        var engine = TestDatasetFactory.CreateQueryEngine(repository);

        var result = await engine.GetDistinctValuesAsync(new DistinctValuesRequest
        {
            ColumnName = "Type",
            Limit = 10
        });

        var values = result.Values.Select(value => value?.ToString()).ToArray();
        Assert.Contains("L", values);
        Assert.Contains("M", values);
        Assert.Contains("H", values);
    }

    [Fact]
    public async Task Filtering_SupportsStartsWithAndNotInOperators()
    {
        var repository = TestDatasetFactory.CreateRepository();
        var engine = TestDatasetFactory.CreateQueryEngine(repository);

        var result = await engine.QueryRowsAsync(new QueryRequest
        {
            Filter = DatasetFilters.And(
                DatasetFilters.StartsWith("Product ID", "L"),
                DatasetFilters.NotIn("Type", "M", "H")),
            SelectedColumns = ["Product ID", "Type"],
            Page = 1,
            PageSize = 25
        });

        Assert.True(result.TotalCount > 0);
        Assert.All(result.Rows, row =>
        {
            Assert.StartsWith("L", row["Product ID"]?.ToString(), StringComparison.OrdinalIgnoreCase);
            Assert.Equal("L", row["Type"]);
        });
    }

    [Fact]
    public async Task AggregationEngine_SupportsGlobalAndFilteredAggregates()
    {
        var repository = TestDatasetFactory.CreateRepository();
        var engine = TestDatasetFactory.CreateAnalyticsEngine(repository);

        var totals = await engine.GroupAndAggregateAsync(new GroupAggregationRequest
        {
            Aggregations =
            [
                DatasetAggregations.Count("total_rows"),
                DatasetAggregations.Count("failure_rows", DatasetFilters.Equal("Machine failure", true))
            ],
            Page = 1,
            PageSize = 1
        });

        Assert.Single(totals.Rows);
        Assert.Equal(10_000, Convert.ToInt32(totals.Rows[0].Values["total_rows"]));
        Assert.True(Convert.ToInt32(totals.Rows[0].Values["failure_rows"]) > 0);

        var byType = await engine.GroupAndAggregateAsync(new GroupAggregationRequest
        {
            GroupByColumns = ["Type"],
            Aggregations =
            [
                DatasetAggregations.Count("row_count"),
                DatasetAggregations.Count("failure_count", DatasetFilters.Equal("Machine failure", true))
            ],
            Having = DatasetFilters.GreaterThan("failure_count", 0),
            SortRules = [DatasetSorts.Descending("failure_count")],
            Page = 1,
            PageSize = 10
        });

        Assert.True(byType.TotalGroups > 0);
        Assert.All(byType.Rows, row =>
        {
            var failureCount = Convert.ToInt32(row.Values["failure_count"]);
            var rowCount = Convert.ToInt32(row.Values["row_count"]);
            Assert.True(failureCount > 0);
            Assert.True(rowCount >= failureCount);
        });
    }

    [Fact]
    public async Task ColumnProfiling_CapturesScopedCompletenessAndSummaries()
    {
        var repository = TestDatasetFactory.CreateRepository();
        var engine = TestDatasetFactory.CreateAnalyticsEngine(repository);

        var profileResult = await engine.ProfileColumnsAsync(new ColumnProfilingRequest
        {
            Columns = ["Type", "Torque [Nm]"],
            Filter = DatasetFilters.Equal("Machine failure", true),
            TopCategoryCount = 3
        });

        Assert.True(profileResult.ScopedRowCount > 0);

        var typeProfile = profileResult.Profiles.Single(profile => profile.ColumnName == "Type");
        var torqueProfile = profileResult.Profiles.Single(profile => profile.ColumnName == "Torque [Nm]");

        Assert.Equal(profileResult.ScopedRowCount, typeProfile.RowCount);
        Assert.Equal(profileResult.ScopedRowCount, typeProfile.NonNullCount);
        Assert.Equal(1d, typeProfile.CompletenessRatio);
        Assert.NotNull(typeProfile.CategoricalSummary);
        Assert.NotNull(torqueProfile.NumericSummary);
        Assert.True(torqueProfile.NumericSummary!.Count > 0);
    }

    [Fact]
    public async Task MultiFilterQuery_MatchesExpectedRowsFromRealDataset()
    {
        var repository = TestDatasetFactory.CreateRepository();
        var engine = TestDatasetFactory.CreateAnalyticsEngine(repository);
        var snapshot = await repository.GetDatasetAsync();

        var request = new QueryRequest
        {
            Filter = DatasetFilters.And(
                DatasetFilters.Equal("Machine failure", true),
                DatasetFilters.In("Type", "L", "M"),
                DatasetFilters.GreaterThanOrEqual("Torque [Nm]", 50),
                DatasetFilters.GreaterThanOrEqual("Tool wear [min]", 150)),
            SelectedColumns = ["Type", "Torque [Nm]", "Tool wear [min]", "Machine failure"],
            SortRules =
            [
                DatasetSorts.Descending("Tool wear [min]"),
                DatasetSorts.Descending("Torque [Nm]")
            ],
            Page = 1,
            PageSize = 5
        };

        var result = await engine.QueryRowsAsync(request);

        var expectedRows = snapshot.Rows
            .Where(row =>
                Convert.ToBoolean(row["Machine failure"])
                && row["Type"]?.ToString() is "L" or "M"
                && Convert.ToDouble(row["Torque [Nm]"]) >= 50
                && Convert.ToDouble(row["Tool wear [min]"]) >= 150)
            .OrderByDescending(row => Convert.ToDouble(row["Tool wear [min]"]))
            .ThenByDescending(row => Convert.ToDouble(row["Torque [Nm]"]))
            .ToArray();

        Assert.Equal(expectedRows.Length, result.TotalCount);
        Assert.Equal(expectedRows.Take(5).Select(row => Convert.ToDouble(row["Tool wear [min]"])), result.Rows.Select(row => Convert.ToDouble(row["Tool wear [min]"])));
        Assert.All(result.Rows, row =>
        {
            Assert.True(Convert.ToBoolean(row["Machine failure"]));
            Assert.True(new[] { "L", "M" }.Contains(row["Type"]?.ToString(), StringComparer.OrdinalIgnoreCase));
            Assert.True(Convert.ToDouble(row["Torque [Nm]"]) >= 50);
            Assert.True(Convert.ToDouble(row["Tool wear [min]"]) >= 150);
        });
    }

    [Fact]
    public async Task GroupedFailureMetrics_MatchSnapshotCalculations()
    {
        var repository = TestDatasetFactory.CreateRepository();
        var engine = TestDatasetFactory.CreateAnalyticsEngine(repository);
        var snapshot = await repository.GetDatasetAsync();

        var result = await engine.GroupAndAggregateAsync(new GroupAggregationRequest
        {
            GroupByColumns = ["Type"],
            Aggregations =
            [
                DatasetAggregations.Count("row_count"),
                DatasetAggregations.Count("failure_count", DatasetFilters.Equal("Machine failure", true)),
                DatasetAggregations.Average("avg_torque", "Torque [Nm]")
            ],
            Having = DatasetFilters.GreaterThan("failure_count", 0),
            SortRules =
            [
                DatasetSorts.Descending("failure_count"),
                DatasetSorts.Descending("row_count")
            ],
            Page = 1,
            PageSize = 10
        });

        var expected = snapshot.Rows
            .GroupBy(row => row["Type"]?.ToString() ?? string.Empty, StringComparer.OrdinalIgnoreCase)
            .Select(group => new
            {
                Type = group.Key,
                RowCount = group.Count(),
                FailureCount = group.Count(row => Convert.ToBoolean(row["Machine failure"])),
                AverageTorque = group.Average(row => Convert.ToDouble(row["Torque [Nm]"]))
            })
            .Where(group => group.FailureCount > 0)
            .OrderByDescending(group => group.FailureCount)
            .ThenByDescending(group => group.RowCount)
            .ToArray();

        Assert.Equal(expected.Length, result.TotalGroups);

        foreach (var expectedGroup in expected)
        {
            var actual = result.Rows.Single(row => Equals(row.Values["Type"], expectedGroup.Type));
            Assert.Equal(expectedGroup.RowCount, Convert.ToInt32(actual.Values["row_count"]));
            Assert.Equal(expectedGroup.FailureCount, Convert.ToInt32(actual.Values["failure_count"]));
            Assert.Equal(expectedGroup.AverageTorque, Convert.ToDouble(actual.Values["avg_torque"]), 6);
        }
    }

    [Fact]
    public async Task ComparisonQuery_MatchesExpectedHighWearCohortCounts()
    {
        var repository = TestDatasetFactory.CreateRepository();
        var analytics = TestDatasetFactory.CreateAnalyticsService(repository);
        var snapshot = await repository.GetDatasetAsync();

        var request = new SubsetComparisonRequest
        {
            LeftLabel = "High-wear failed",
            LeftFilter = DatasetFilters.And(
                DatasetFilters.Equal("Machine failure", true),
                DatasetFilters.GreaterThanOrEqual("Tool wear [min]", 150)),
            RightLabel = "Low-wear healthy",
            RightFilter = DatasetFilters.And(
                DatasetFilters.Equal("Machine failure", false),
                DatasetFilters.LessThanOrEqual("Tool wear [min]", 50)),
            NumericColumns = ["Torque [Nm]", "Tool wear [min]"],
            CategoricalColumns = ["Type"]
        };

        var result = await analytics.CompareSubsetsAsync(request);

        var leftRows = snapshot.Rows
            .Where(row => Convert.ToBoolean(row["Machine failure"]) && Convert.ToDouble(row["Tool wear [min]"]) >= 150)
            .ToArray();
        var rightRows = snapshot.Rows
            .Where(row => !Convert.ToBoolean(row["Machine failure"]) && Convert.ToDouble(row["Tool wear [min]"]) <= 50)
            .ToArray();
        var expectedTorqueDelta = leftRows.Average(row => Convert.ToDouble(row["Torque [Nm]"]))
            - rightRows.Average(row => Convert.ToDouble(row["Torque [Nm]"]));

        Assert.Equal(leftRows.Length, result.Left.RowCount);
        Assert.Equal(rightRows.Length, result.Right.RowCount);
        var actualTorqueDelta = result.NumericComparisons.Single(metric => metric.ColumnName == "Torque [Nm]").AverageDelta;
        Assert.NotNull(actualTorqueDelta);
        Assert.Equal(expectedTorqueDelta, actualTorqueDelta.Value, 6);
        Assert.Single(result.CategoricalComparisons);
    }

    [Fact]
    public async Task ColumnExtrema_ReturnsRowsAtGlobalMaximum()
    {
        var repository = TestDatasetFactory.CreateRepository();
        var engine = TestDatasetFactory.CreateQueryEngine(repository);
        var snapshot = await repository.GetDatasetAsync();

        var column = "Process temperature [K]";
        var expectedMax = snapshot.Rows.Max(row => Convert.ToDouble(row[column]));

        var result = await engine.FindColumnExtremaRowsAsync(new ColumnExtremaRequest
        {
            ColumnName = column,
            Mode = "max",
            MaxTieRows = 5
        });

        Assert.Equal(expectedMax, Convert.ToDouble(result.ExtremumValue!), 9);
        Assert.True(result.TotalMatchingRows >= 1);
        Assert.NotEmpty(result.Rows);
        Assert.Contains("UDI", result.Columns, StringComparer.OrdinalIgnoreCase);
        Assert.All(result.Rows, row => Assert.Equal(expectedMax, Convert.ToDouble(row[column]), 9));
    }
}
