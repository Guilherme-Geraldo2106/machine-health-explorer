using MachineHealthExplorer.Domain.Abstractions;
using MachineHealthExplorer.Domain.Models;

namespace MachineHealthExplorer.Data.Services;

public sealed class MachineHealthAnalyticsService : IMachineHealthAnalyticsService
{
    private readonly IDatasetAnalyticsEngine _analyticsEngine;

    public MachineHealthAnalyticsService(IDatasetAnalyticsEngine analyticsEngine)
    {
        _analyticsEngine = analyticsEngine ?? throw new ArgumentNullException(nameof(analyticsEngine));
    }

    public async Task<FailureAnalysisSummary> GetFailureAnalysisAsync(CancellationToken cancellationToken = default)
    {
        var schema = await _analyticsEngine.GetSchemaAsync(cancellationToken).ConfigureAwait(false);
        var failureIndicator = MachineHealthDatasetConventions.ResolveFailureIndicator(schema);
        if (failureIndicator is null)
        {
            return new FailureAnalysisSummary();
        }

        var counts = await _analyticsEngine.GroupAndAggregateAsync(new GroupAggregationRequest
        {
            Aggregations =
            [
                DatasetAggregations.Count("failure_count", DatasetFilters.Equal(failureIndicator.Name, true)),
                DatasetAggregations.Count("healthy_count", DatasetFilters.Equal(failureIndicator.Name, false))
            ],
            Page = 1,
            PageSize = 1
        }, cancellationToken).ConfigureAwait(false);

        var totals = counts.Rows.FirstOrDefault();
        var failureCount = GetIntegerValue(totals?.Values, "failure_count");
        var healthyCount = GetIntegerValue(totals?.Values, "healthy_count");
        var failureModes = await GetFailureModesAsync(cancellationToken).ConfigureAwait(false);
        var comparison = await CompareFailureCohortsAsync(cancellationToken).ConfigureAwait(false);

        return new FailureAnalysisSummary
        {
            FailureIndicatorColumn = failureIndicator.Name,
            FailureCount = failureCount,
            HealthyCount = healthyCount,
            FailureRate = schema.RowCount == 0 ? 0 : failureCount / (double)schema.RowCount,
            FailureModes = failureModes,
            NumericComparisons = comparison.NumericComparisons.Take(5).ToArray()
        };
    }

    public async Task<SubsetComparisonResult> CompareFailureCohortsAsync(CancellationToken cancellationToken = default)
    {
        var schema = await _analyticsEngine.GetSchemaAsync(cancellationToken).ConfigureAwait(false);
        var failureIndicator = MachineHealthDatasetConventions.ResolveFailureIndicator(schema)
            ?? throw new InvalidOperationException("A failure indicator column could not be identified in the dataset.");

        return await _analyticsEngine.CompareSubsetsAsync(new SubsetComparisonRequest
        {
            LeftLabel = "Failed",
            LeftFilter = DatasetFilters.Equal(failureIndicator.Name, true),
            RightLabel = "Healthy",
            RightFilter = DatasetFilters.Equal(failureIndicator.Name, false),
            NumericColumns = MachineHealthDatasetConventions.GetOperationalNumericColumns(schema),
            CategoricalColumns = MachineHealthDatasetConventions.GetComparisonCategoricalColumns(schema)
                .Where(column => !column.Equals(failureIndicator.Name, StringComparison.OrdinalIgnoreCase))
                .Take(3)
                .ToArray(),
            TopCategoryCount = 5
        }, cancellationToken).ConfigureAwait(false);
    }

    public async Task<IReadOnlyList<ValueFrequency>> GetFailureModesAsync(CancellationToken cancellationToken = default)
    {
        var schema = await _analyticsEngine.GetSchemaAsync(cancellationToken).ConfigureAwait(false);
        var failureIndicator = MachineHealthDatasetConventions.ResolveFailureIndicator(schema);
        if (failureIndicator is null)
        {
            return Array.Empty<ValueFrequency>();
        }

        var failureModes = MachineHealthDatasetConventions.ResolveFailureModeColumns(schema, failureIndicator);
        if (failureModes.Length == 0)
        {
            return Array.Empty<ValueFrequency>();
        }

        var aggregations = new List<AggregationDefinition>
        {
            DatasetAggregations.Count("total_failures", DatasetFilters.Equal(failureIndicator.Name, true))
        };

        foreach (var column in failureModes)
        {
            aggregations.Add(DatasetAggregations.Count(
                $"mode_{MachineHealthDatasetConventions.SanitizeAlias(column.Name)}",
                DatasetFilters.And(
                    DatasetFilters.Equal(failureIndicator.Name, true),
                    DatasetFilters.Equal(column.Name, true))));
        }

        var aggregate = await _analyticsEngine.GroupAndAggregateAsync(new GroupAggregationRequest
        {
            Aggregations = aggregations,
            Page = 1,
            PageSize = 1
        }, cancellationToken).ConfigureAwait(false);

        var values = aggregate.Rows.FirstOrDefault()?.Values;
        var totalFailures = GetIntegerValue(values, "total_failures");

        return failureModes
            .Select(column =>
            {
                var alias = $"mode_{MachineHealthDatasetConventions.SanitizeAlias(column.Name)}";
                var count = GetIntegerValue(values, alias);
                return new ValueFrequency
                {
                    Value = column.Name,
                    Count = count,
                    Percentage = totalFailures == 0 ? 0 : count / (double)totalFailures
                };
            })
            .OrderByDescending(item => item.Count)
            .ThenBy(item => item.Value, StringComparer.OrdinalIgnoreCase)
            .ToArray();
    }

    public async Task<OperatingConditionSummary> GetOperatingConditionSummaryAsync(
        FilterExpression? filter = null,
        CancellationToken cancellationToken = default)
    {
        var schema = await _analyticsEngine.GetSchemaAsync(cancellationToken).ConfigureAwait(false);
        var numericColumns = MachineHealthDatasetConventions.GetOperationalNumericColumns(schema);
        var categoricalColumns = MachineHealthDatasetConventions.GetComparisonCategoricalColumns(schema)
            .Where(column =>
                !MachineHealthDatasetConventions.IsFailureColumn(
                    schema.Columns.First(schemaColumn => schemaColumn.Name.Equals(column, StringComparison.OrdinalIgnoreCase))))
            .Take(3)
            .ToArray();

        var profiles = await _analyticsEngine.ProfileColumnsAsync(new ColumnProfilingRequest
        {
            Columns = numericColumns.Concat(categoricalColumns).ToArray(),
            Filter = filter,
            TopCategoryCount = 5
        }, cancellationToken).ConfigureAwait(false);

        return new OperatingConditionSummary
        {
            NumericSummaries = profiles.Profiles
                .Where(profile => profile.NumericSummary is not null)
                .Select(profile => profile.NumericSummary!)
                .ToArray(),
            CategoricalSummaries = profiles.Profiles
                .Where(profile => profile.CategoricalSummary is not null)
                .Select(profile => profile.CategoricalSummary!)
                .ToArray()
        };
    }

    public async Task<DatasetReport> BuildExecutiveReportAsync(CancellationToken cancellationToken = default)
    {
        var schema = await _analyticsEngine.GetSchemaAsync(cancellationToken).ConfigureAwait(false);
        var failureIndicator = MachineHealthDatasetConventions.ResolveFailureIndicator(schema);
        var focusColumns = MachineHealthDatasetConventions.GetOperationalNumericColumns(schema).Take(3).ToList();
        if (failureIndicator is not null)
        {
            focusColumns.Insert(0, failureIndicator.Name);
        }

        var groupByColumns = MachineHealthDatasetConventions.GetComparisonCategoricalColumns(schema)
            .Where(column => !column.Equals(failureIndicator?.Name, StringComparison.OrdinalIgnoreCase))
            .Take(1)
            .ToArray();

        var aggregations = new List<AggregationDefinition>
        {
            DatasetAggregations.Count("row_count")
        };

        var firstMetric = MachineHealthDatasetConventions.GetOperationalNumericColumns(schema).FirstOrDefault();
        if (firstMetric is not null)
        {
            aggregations.Add(DatasetAggregations.Average(
                $"avg_{MachineHealthDatasetConventions.SanitizeAlias(firstMetric)}",
                firstMetric));
        }

        if (failureIndicator is not null)
        {
            aggregations.Add(DatasetAggregations.Count(
                "failure_count",
                DatasetFilters.Equal(failureIndicator.Name, true)));
        }

        return await _analyticsEngine.BuildReportAsync(new ReportRequest
        {
            Title = "Executive Dataset Overview",
            FocusColumns = focusColumns,
            GroupByColumns = groupByColumns,
            Aggregations = aggregations
        }, cancellationToken).ConfigureAwait(false);
    }

    public async Task<IReadOnlyList<AnalysisExample>> GetAnalysisExamplesAsync(CancellationToken cancellationToken = default)
    {
        var schema = await _analyticsEngine.GetSchemaAsync(cancellationToken).ConfigureAwait(false);
        var failureIndicator = MachineHealthDatasetConventions.ResolveFailureIndicator(schema);
        if (failureIndicator is null)
        {
            return Array.Empty<AnalysisExample>();
        }

        return
        [
            new AnalysisExample
            {
                Name = "high-stress-failures",
                Description = "A multi-filter row query focused on failed machines with elevated torque and heavy tool wear.",
                SuggestedPrompt = "Show failed Type L or M machines with high torque and high tool wear.",
                Kind = AnalysisExampleKind.RowQuery,
                RowQuery = BuildHighStressFailureQuery(schema, failureIndicator)
            },
            new AnalysisExample
            {
                Name = "failure-rate-by-type",
                Description = "A grouped comparison by product type with row counts, failure counts, and average operating metrics.",
                SuggestedPrompt = "Group the dataset by machine type and compare failures, torque, and tool wear.",
                Kind = AnalysisExampleKind.GroupAggregation,
                GroupAggregationQuery = BuildFailureRateByTypeQuery(schema, failureIndicator)
            },
            new AnalysisExample
            {
                Name = "high-wear-failed-vs-healthy",
                Description = "A subset comparison between high-wear failures and low-wear healthy machines.",
                SuggestedPrompt = "Compare high-wear failed machines against low-wear healthy machines.",
                Kind = AnalysisExampleKind.SubsetComparison,
                ComparisonQuery = BuildHighWearComparison(schema, failureIndicator)
            }
        ];
    }

    private static QueryRequest BuildHighStressFailureQuery(DatasetSchema schema, ColumnSchema failureIndicator)
    {
        var typeColumn = MachineHealthDatasetConventions.ResolvePreferredColumn(schema, "Type");
        var airTemperatureColumn = MachineHealthDatasetConventions.ResolvePreferredColumn(schema, "Air temperature [K]");
        var processTemperatureColumn = MachineHealthDatasetConventions.ResolvePreferredColumn(schema, "Process temperature [K]");
        var torqueColumn = MachineHealthDatasetConventions.ResolvePreferredColumn(schema, "Torque [Nm]");
        var wearColumn = MachineHealthDatasetConventions.ResolvePreferredColumn(schema, "Tool wear [min]");

        var filters = new List<FilterExpression?>
        {
            DatasetFilters.Equal(failureIndicator.Name, true)
        };

        if (typeColumn is not null)
        {
            filters.Add(DatasetFilters.In(typeColumn, "L", "M"));
        }

        if (torqueColumn is not null)
        {
            filters.Add(DatasetFilters.GreaterThanOrEqual(torqueColumn, 50));
        }

        if (wearColumn is not null)
        {
            filters.Add(DatasetFilters.GreaterThanOrEqual(wearColumn, 150));
        }

        var sortRules = new List<SortRule>();
        if (wearColumn is not null)
        {
            sortRules.Add(DatasetSorts.Descending(wearColumn));
        }

        if (torqueColumn is not null)
        {
            sortRules.Add(DatasetSorts.Descending(torqueColumn));
        }

        return new QueryRequest
        {
            Filter = DatasetFilters.And(filters.ToArray()),
            SelectedColumns = MachineHealthDatasetConventions.FilterAvailableColumns(
                schema,
                typeColumn,
                airTemperatureColumn,
                processTemperatureColumn,
                torqueColumn,
                wearColumn,
                failureIndicator.Name),
            SortRules = sortRules,
            Page = 1,
            PageSize = 5
        };
    }

    private static GroupAggregationRequest BuildFailureRateByTypeQuery(DatasetSchema schema, ColumnSchema failureIndicator)
    {
        var groupColumn = MachineHealthDatasetConventions.ResolvePreferredColumn(schema, "Type")
            ?? MachineHealthDatasetConventions.GetComparisonCategoricalColumns(schema)
                .FirstOrDefault(column => !column.Equals(failureIndicator.Name, StringComparison.OrdinalIgnoreCase));
        var torqueColumn = MachineHealthDatasetConventions.ResolvePreferredColumn(schema, "Torque [Nm]");
        var wearColumn = MachineHealthDatasetConventions.ResolvePreferredColumn(schema, "Tool wear [min]");

        var aggregations = new List<AggregationDefinition>
        {
            DatasetAggregations.Count("row_count"),
            DatasetAggregations.Count("failure_count", DatasetFilters.Equal(failureIndicator.Name, true))
        };

        if (torqueColumn is not null)
        {
            aggregations.Add(DatasetAggregations.Average("avg_torque", torqueColumn));
        }

        if (wearColumn is not null)
        {
            aggregations.Add(DatasetAggregations.Average("avg_tool_wear", wearColumn));
        }

        return new GroupAggregationRequest
        {
            GroupByColumns = groupColumn is null ? Array.Empty<string>() : [groupColumn],
            Aggregations = aggregations,
            Having = DatasetFilters.GreaterThan("failure_count", 0),
            SortRules =
            [
                DatasetSorts.Descending("failure_count"),
                DatasetSorts.Descending("row_count")
            ],
            Page = 1,
            PageSize = 10
        };
    }

    private static SubsetComparisonRequest BuildHighWearComparison(DatasetSchema schema, ColumnSchema failureIndicator)
    {
        var wearColumn = MachineHealthDatasetConventions.ResolvePreferredColumn(schema, "Tool wear [min]");
        var numericColumns = MachineHealthDatasetConventions.FilterAvailableColumns(
            schema,
            MachineHealthDatasetConventions.ResolvePreferredColumn(schema, "Torque [Nm]"),
            MachineHealthDatasetConventions.ResolvePreferredColumn(schema, "Rotational speed [rpm]"),
            wearColumn,
            MachineHealthDatasetConventions.ResolvePreferredColumn(schema, "Air temperature [K]"));

        if (numericColumns.Length == 0)
        {
            numericColumns = MachineHealthDatasetConventions.GetOperationalNumericColumns(schema).Take(3).ToArray();
        }

        var leftFilters = new List<FilterExpression?>
        {
            DatasetFilters.Equal(failureIndicator.Name, true)
        };
        var rightFilters = new List<FilterExpression?>
        {
            DatasetFilters.Equal(failureIndicator.Name, false)
        };

        if (wearColumn is not null)
        {
            leftFilters.Add(DatasetFilters.GreaterThanOrEqual(wearColumn, 150));
            rightFilters.Add(DatasetFilters.LessThanOrEqual(wearColumn, 50));
        }

        return new SubsetComparisonRequest
        {
            LeftLabel = "High-wear failed",
            LeftFilter = DatasetFilters.And(leftFilters.ToArray()),
            RightLabel = "Low-wear healthy",
            RightFilter = DatasetFilters.And(rightFilters.ToArray()),
            NumericColumns = numericColumns,
            CategoricalColumns = MachineHealthDatasetConventions.GetComparisonCategoricalColumns(schema)
                .Where(column => !column.Equals(failureIndicator.Name, StringComparison.OrdinalIgnoreCase))
                .Take(2)
                .ToArray(),
            TopCategoryCount = 3
        };
    }

    private static int GetIntegerValue(IReadOnlyDictionary<string, object?>? values, string key)
    {
        if (values is null || !values.TryGetValue(key, out var value) || value is null)
        {
            return 0;
        }

        return value switch
        {
            int intValue => intValue,
            long longValue => checked((int)longValue),
            short shortValue => shortValue,
            byte byteValue => byteValue,
            _ when int.TryParse(value.ToString(), out var parsed) => parsed,
            _ => 0
        };
    }
}
