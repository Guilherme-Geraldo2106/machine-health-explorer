using MachineHealthExplorer.Data.Infrastructure;
using MachineHealthExplorer.Data.Querying;
using MachineHealthExplorer.Domain.Abstractions;
using MachineHealthExplorer.Domain.Models;
using System.Collections.ObjectModel;

namespace MachineHealthExplorer.Data.Services;

public sealed class DatasetAnalyticsEngine : IDatasetAnalyticsEngine
{
    private readonly IDatasetRepository _repository;

    public DatasetAnalyticsEngine(IDatasetRepository repository)
    {
        _repository = repository ?? throw new ArgumentNullException(nameof(repository));
    }

    public async Task<DatasetSchema> GetSchemaAsync(CancellationToken cancellationToken = default)
        => (await _repository.GetDatasetAsync(cancellationToken).ConfigureAwait(false)).Schema;

    public async Task<ColumnProfile> GetColumnProfileAsync(string columnName, CancellationToken cancellationToken = default)
    {
        var result = await ProfileColumnsAsync(new ColumnProfilingRequest
        {
            Columns = [columnName]
        }, cancellationToken).ConfigureAwait(false);

        return result.Profiles.Single();
    }

    public async Task<SearchColumnsResult> SearchColumnsAsync(string keyword, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(keyword))
        {
            throw new ArgumentException("Search keyword must be provided.", nameof(keyword));
        }

        var schema = await GetSchemaAsync(cancellationToken).ConfigureAwait(false);
        var normalizedKeyword = keyword.Trim();

        return new SearchColumnsResult
        {
            Keyword = normalizedKeyword,
            Matches = ColumnSearchMatcher.Search(schema.Columns, normalizedKeyword)
        };
    }

    public async Task<ColumnProfilingResult> ProfileColumnsAsync(ColumnProfilingRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        var (snapshot, schemaLookup) = await GetContextAsync(cancellationToken).ConfigureAwait(false);
        ValidateFilter(request.Filter, schemaLookup);

        var columns = request.Columns.Count == 0
            ? snapshot.Schema.Columns.Select(column => column.Name).ToArray()
            : request.Columns.Distinct(StringComparer.OrdinalIgnoreCase).ToArray();

        ValidateColumns(columns, schemaLookup);

        var scopedRows = FilterRows(snapshot.Rows, request.Filter, schemaLookup);

        var profiles = columns
            .Select(columnName => AnalyticsComputation.BuildColumnProfile(
                schemaLookup[columnName],
                SelectColumnValues(scopedRows, columnName),
                scopedRows.Length,
                request.TopCategoryCount))
            .ToArray();

        return new ColumnProfilingResult
        {
            ScopedRowCount = scopedRows.Length,
            Profiles = profiles
        };
    }

    public async Task<QueryResult> QueryRowsAsync(QueryRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        ValidatePaging(request.Page, request.PageSize);
        var (snapshot, schemaLookup) = await GetContextAsync(cancellationToken).ConfigureAwait(false);

        ValidateColumns(request.SelectedColumns, schemaLookup);
        ValidateColumns(request.SortRules.Select(rule => rule.ColumnName), schemaLookup);
        ValidateFilter(request.Filter, schemaLookup);

        var selectedColumns = request.SelectedColumns.Count == 0
            ? snapshot.Schema.Columns.Select(column => column.Name).ToArray()
            : request.SelectedColumns.Distinct(StringComparer.OrdinalIgnoreCase).ToArray();

        var filteredRows = FilterRows(snapshot.Rows, request.Filter, schemaLookup);

        var orderedRows = TabularOrdering.ApplySorting(filteredRows, request.SortRules, GetValue);
        var pageRows = orderedRows
            .Skip((request.Page - 1) * request.PageSize)
            .Take(request.PageSize)
            .Select(row => row.SelectColumns(selectedColumns))
            .ToArray();

        return new QueryResult
        {
            Columns = selectedColumns,
            Rows = pageRows,
            TotalCount = filteredRows.Length,
            Page = request.Page,
            PageSize = request.PageSize
        };
    }

    public async Task<ColumnExtremaResult> FindColumnExtremaRowsAsync(ColumnExtremaRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        if (string.IsNullOrWhiteSpace(request.ColumnName))
        {
            throw new ArgumentException("Column name must be provided.", nameof(request));
        }

        var mode = (request.Mode ?? "max").Trim();
        var isMin = mode.Equals("min", StringComparison.OrdinalIgnoreCase);
        var maxTieRows = Math.Clamp(request.MaxTieRows, 1, 50);

        var (snapshot, schemaLookup) = await GetContextAsync(cancellationToken).ConfigureAwait(false);
        var column = GetRequiredColumn(request.ColumnName, schemaLookup);
        if (!column.IsNumeric)
        {
            throw new ArgumentException($"Column '{request.ColumnName}' is not numeric.", nameof(request));
        }

        ValidateFilter(request.Filter, schemaLookup);
        ValidateColumns(request.ExtraSelectedColumns, schemaLookup);

        var filteredRows = FilterRows(snapshot.Rows, request.Filter, schemaLookup);
        if (filteredRows.Length == 0)
        {
            return new ColumnExtremaResult
            {
                ColumnName = column.Name,
                Mode = isMin ? "min" : "max",
                ScopedRowCount = 0,
                TotalMatchingRows = 0
            };
        }

        var nonNullValues = filteredRows
            .Select(row => GetValue(row, column.Name))
            .Where(value => value is not null)
            .ToArray();

        if (nonNullValues.Length == 0)
        {
            return new ColumnExtremaResult
            {
                ColumnName = column.Name,
                Mode = isMin ? "min" : "max",
                ScopedRowCount = filteredRows.Length,
                TotalMatchingRows = 0
            };
        }

        var extremum = isMin
            ? nonNullValues.OrderBy(value => value, DatasetValueComparer.Instance).First()
            : nonNullValues.OrderByDescending(value => value, DatasetValueComparer.Instance).First();

        var ties = filteredRows
            .Where(row =>
            {
                var v = GetValue(row, column.Name);
                return v is not null && DatasetValueComparer.Instance.Equals(v, extremum);
            })
            .ToArray();

        var selected = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { column.Name };
        foreach (var extra in request.ExtraSelectedColumns)
        {
            if (!string.IsNullOrWhiteSpace(extra))
            {
                selected.Add(extra.Trim());
            }
        }

        if (schemaLookup.ContainsKey("UDI"))
        {
            selected.Add("UDI");
        }

        var columns = selected.ToArray();
        var pageRows = ties
            .Take(maxTieRows)
            .Select(row => row.SelectColumns(columns))
            .ToArray();

        return new ColumnExtremaResult
        {
            ColumnName = column.Name,
            Mode = isMin ? "min" : "max",
            ExtremumValue = extremum,
            ScopedRowCount = filteredRows.Length,
            TotalMatchingRows = ties.Length,
            Columns = columns,
            Rows = pageRows
        };
    }

    public async Task<DistinctValuesResult> GetDistinctValuesAsync(DistinctValuesRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        var (snapshot, schemaLookup) = await GetContextAsync(cancellationToken).ConfigureAwait(false);
        ValidateColumns([request.ColumnName], schemaLookup);
        ValidateFilter(request.Filter, schemaLookup);

        var values = FilterRows(snapshot.Rows, request.Filter, schemaLookup)
            .Select(row => GetValue(row, request.ColumnName))
            .Where(value => value is not null)
            .GroupBy(value => value, DatasetValueComparer.Instance)
            .OrderByDescending(group => group.Count())
            .ThenBy(group => group.Key, DatasetValueComparer.Instance)
            .ToArray();

        var requestedLimit = Math.Max(1, request.Limit);
        var totalObservedCount = values.Sum(group => group.Count());
        var topGroups = values.Take(requestedLimit).ToArray();

        return new DistinctValuesResult
        {
            ColumnName = request.ColumnName,
            TotalDistinctCount = values.Length,
            Values = topGroups.Select(group => group.Key).ToArray(),
            Frequencies = topGroups
                .Select(group => new ValueFrequency
                {
                    Value = group.Key?.ToString() ?? string.Empty,
                    Count = group.Count(),
                    Percentage = totalObservedCount == 0 ? 0 : group.Count() / (double)totalObservedCount
                })
                .ToArray()
        };
    }

    public async Task<GroupAggregationResult> GroupAndAggregateAsync(GroupAggregationRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        ValidatePaging(request.Page, request.PageSize);
        var (snapshot, schemaLookup) = await GetContextAsync(cancellationToken).ConfigureAwait(false);

        var binSpecs = request.GroupByBins?.Count > 0
            ? request.GroupByBins.ToArray()
            : Array.Empty<NumericGroupBinSpec>();

        var autoBinSpecs = request.GroupByAutoBins?.Count > 0
            ? request.GroupByAutoBins.ToArray()
            : Array.Empty<NumericGroupAutoBinSpec>();

        var groupByColumns = request.GroupByColumns.Count == 0
            ? Array.Empty<string>()
            : request.GroupByColumns.Distinct(StringComparer.OrdinalIgnoreCase).ToArray();

        ValidateGroupByBins(binSpecs, groupByColumns, schemaLookup, autoBinSpecs);
        ValidateGroupByAutoBins(autoBinSpecs, groupByColumns, binSpecs, schemaLookup);

        ValidateColumns(groupByColumns, schemaLookup);
        ValidateFilter(request.Filter, schemaLookup);

        var aggregations = request.Aggregations.Count == 0
            ? [DatasetAggregations.Count("row_count")]
            : request.Aggregations.ToArray();

        var derivedMetrics = request.DerivedMetrics?.Count > 0
            ? request.DerivedMetrics.ToArray()
            : Array.Empty<DerivedMetricDefinition>();

        var groupDimensionNames = groupByColumns
            .Concat(binSpecs.Select(spec => spec.Alias))
            .Concat(autoBinSpecs.Select(spec => spec.Alias))
            .ToArray();

        ValidateAggregations(aggregations, groupDimensionNames, schemaLookup);
        ValidateDerivedMetricDefinitions(derivedMetrics, groupByColumns, binSpecs, autoBinSpecs, aggregations, schemaLookup);

        var filteredRows = FilterRows(snapshot.Rows, request.Filter, schemaLookup);

        var autoBinMaps = BuildAutoBinRowMaps(filteredRows, autoBinSpecs);
        var autoBinSummaries = BuildAutoBinAppliedSummaries(autoBinSpecs, autoBinMaps);

        var groupedRows = binSpecs.Length == 0 && autoBinSpecs.Length == 0
            ? BuildGroups(filteredRows, groupByColumns)
                .Select(group => BuildGroupAggregationRowWithDerived(
                    group.KeyValues,
                    group.Rows,
                    groupDimensionNames,
                    aggregations,
                    derivedMetrics,
                    schemaLookup))
                .ToArray()
            : BuildGroupsWithBinDimensions(filteredRows, groupByColumns, binSpecs, autoBinSpecs, autoBinMaps)
                .Select(group => BuildGroupAggregationRowWithDerived(
                    group.KeyValues,
                    group.Rows,
                    groupDimensionNames,
                    aggregations,
                    derivedMetrics,
                    schemaLookup))
                .ToArray();

        var aggregatedColumns = groupDimensionNames
            .Concat(aggregations.Select(a => a.Alias))
            .Concat(derivedMetrics.Select(d => d.Alias))
            .ToArray();

        var aggregateSchema = BuildAggregateSchema(groupByColumns, binSpecs, autoBinSpecs, aggregations, derivedMetrics, schemaLookup);

        ValidateFilter(request.Having, aggregateSchema);
        ValidateColumns(request.SortRules.Select(rule => rule.ColumnName), aggregateSchema);

        var scopedGroups = groupedRows
            .Where(row => FilterEvaluator.Matches(row, request.Having, aggregateSchema))
            .ToArray();

        var orderedGroups = TabularOrdering.ApplySorting(scopedGroups, request.SortRules, GetValue);
        var pageRows = orderedGroups
            .Skip((request.Page - 1) * request.PageSize)
            .Take(request.PageSize)
            .ToArray();

        return new GroupAggregationResult
        {
            Columns = aggregatedColumns,
            Rows = pageRows,
            ScopedRowCount = filteredRows.Length,
            TotalGroups = scopedGroups.Length,
            Page = request.Page,
            PageSize = request.PageSize,
            GroupByAutoBinsApplied = autoBinSummaries
        };
    }

    public async Task<NumericSummary> GetNumericSummaryAsync(string columnName, FilterExpression? filter = null, CancellationToken cancellationToken = default)
    {
        var (snapshot, schemaLookup) = await GetContextAsync(cancellationToken).ConfigureAwait(false);
        var column = GetRequiredColumn(columnName, schemaLookup);
        if (!column.IsNumeric)
        {
            throw new ArgumentException($"Column '{columnName}' is not numeric and cannot produce a numeric summary.", nameof(columnName));
        }

        ValidateFilter(filter, schemaLookup);
        var values = SelectColumnValues(FilterRows(snapshot.Rows, filter, schemaLookup), column.Name);

        return AnalyticsComputation.BuildNumericSummary(column.Name, values);
    }

    public async Task<CategoricalSummary> GetCategoricalSummaryAsync(string columnName, FilterExpression? filter = null, int top = 10, CancellationToken cancellationToken = default)
    {
        var (snapshot, schemaLookup) = await GetContextAsync(cancellationToken).ConfigureAwait(false);
        _ = GetRequiredColumn(columnName, schemaLookup);
        ValidateFilter(filter, schemaLookup);

        var values = SelectColumnValues(FilterRows(snapshot.Rows, filter, schemaLookup), columnName);

        return AnalyticsComputation.BuildCategoricalSummary(columnName, values, top);
    }

    public async Task<SubsetComparisonResult> CompareSubsetsAsync(SubsetComparisonRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        var (snapshot, schemaLookup) = await GetContextAsync(cancellationToken).ConfigureAwait(false);
        ValidateFilter(request.LeftFilter, schemaLookup);
        ValidateFilter(request.RightFilter, schemaLookup);

        var leftRows = FilterRows(snapshot.Rows, request.LeftFilter, schemaLookup);
        var rightRows = FilterRows(snapshot.Rows, request.RightFilter, schemaLookup);

        var numericColumns = request.NumericColumns.Count == 0
            ? snapshot.Schema.Columns.Where(column => column.IsNumeric).Select(column => column.Name).ToArray()
            : request.NumericColumns.Distinct(StringComparer.OrdinalIgnoreCase).ToArray();
        var categoricalColumns = request.CategoricalColumns.Count == 0
            ? snapshot.Schema.Columns
                .Where(column => column.CategoricalSummary is not null)
                .Select(column => column.Name)
                .Take(5)
                .ToArray()
            : request.CategoricalColumns.Distinct(StringComparer.OrdinalIgnoreCase).ToArray();

        ValidateColumns(numericColumns.Concat(categoricalColumns), schemaLookup);

        var numericComparisons = numericColumns
            .Select(column => new NumericComparisonMetric
            {
                ColumnName = column,
                Left = AnalyticsComputation.BuildNumericSummary(column, SelectColumnValues(leftRows, column)),
                Right = AnalyticsComputation.BuildNumericSummary(column, SelectColumnValues(rightRows, column))
            })
            .Select(metric => metric with
            {
                AverageDelta = metric.Left.Average.HasValue && metric.Right.Average.HasValue
                    ? metric.Left.Average.Value - metric.Right.Average.Value
                    : null,
                MedianDelta = metric.Left.Median.HasValue && metric.Right.Median.HasValue
                    ? metric.Left.Median.Value - metric.Right.Median.Value
                    : null
            })
            .ToArray();

        var categoricalComparisons = categoricalColumns
            .Select(column => new CategoricalComparisonMetric
            {
                ColumnName = column,
                LeftTopValues = AnalyticsComputation.BuildCategoricalSummary(column, SelectColumnValues(leftRows, column), request.TopCategoryCount).TopValues,
                RightTopValues = AnalyticsComputation.BuildCategoricalSummary(column, SelectColumnValues(rightRows, column), request.TopCategoryCount).TopValues
            })
            .ToArray();

        return new SubsetComparisonResult
        {
            Left = new SubsetSnapshot
            {
                Label = request.LeftLabel,
                RowCount = leftRows.Length
            },
            Right = new SubsetSnapshot
            {
                Label = request.RightLabel,
                RowCount = rightRows.Length
            },
            NumericComparisons = numericComparisons,
            CategoricalComparisons = categoricalComparisons
        };
    }

    public async Task<DatasetDescription> DescribeDatasetAsync(CancellationToken cancellationToken = default)
    {
        var snapshot = await _repository.GetDatasetAsync(cancellationToken).ConfigureAwait(false);
        var numericColumns = snapshot.Schema.Columns.Where(column => column.IsNumeric).Select(column => column.Name).ToArray();
        var categoricalColumns = snapshot.Schema.Columns.Where(column => column.CategoricalSummary is not null).Select(column => column.Name).ToArray();
        var failureColumns = snapshot.Schema.Columns.Where(AnalyticsComputation.IsFailureLikeColumn).Select(column => column.Name).ToArray();
        var completeColumns = snapshot.Schema.Columns.Count(column => column.NullCount == 0);

        var highlights = new List<string>
        {
            $"Rows: {snapshot.Schema.RowCount}",
            $"Numeric columns: {numericColumns.Length}",
            $"Categorical or low-cardinality columns: {categoricalColumns.Length}",
            $"Fully populated columns: {completeColumns}"
        };

        if (failureColumns.Length > 0)
        {
            highlights.Add($"Boolean-like or binary label columns (heuristic name match): {string.Join(", ", failureColumns)}");
        }

        return new DatasetDescription
        {
            DatasetName = snapshot.DatasetName,
            SourcePath = snapshot.SourcePath,
            RowCount = snapshot.Schema.RowCount,
            ColumnCount = snapshot.Schema.Columns.Count,
            NumericColumns = numericColumns,
            CategoricalColumns = categoricalColumns,
            PotentialFailureColumns = failureColumns,
            Highlights = highlights
        };
    }

    public async Task<DatasetReport> BuildReportAsync(ReportRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        var (snapshot, schemaLookup) = await GetContextAsync(cancellationToken).ConfigureAwait(false);
        ValidateFilter(request.BaseFilter, schemaLookup);

        var focusColumns = request.FocusColumns.Count == 0
            ? snapshot.Schema.Columns.Take(5).Select(column => column.Name).ToArray()
            : request.FocusColumns.Distinct(StringComparer.OrdinalIgnoreCase).ToArray();

        ValidateColumns(focusColumns, schemaLookup);

        var profiles = await ProfileColumnsAsync(new ColumnProfilingRequest
        {
            Columns = focusColumns,
            Filter = request.BaseFilter,
            TopCategoryCount = request.TopCategoryCount
        }, cancellationToken).ConfigureAwait(false);

        var sections = new List<ReportSection>
        {
            new()
            {
                Heading = "Scope",
                Content = $"The report covers {profiles.ScopedRowCount} rows from '{snapshot.DatasetName}' across {snapshot.Schema.Columns.Count} columns."
            }
        };

        foreach (var profile in profiles.Profiles)
        {
            if (profile.NumericSummary is not null)
            {
                sections.Add(new ReportSection
                {
                    Heading = profile.ColumnName,
                    Content = $"count={profile.NumericSummary.Count}, avg={profile.NumericSummary.Average?.ToString("F3") ?? "n/a"}, min={profile.NumericSummary.Min?.ToString("F3") ?? "n/a"}, max={profile.NumericSummary.Max?.ToString("F3") ?? "n/a"}"
                });
                continue;
            }

            var topValues = profile.CategoricalSummary?.TopValues.Count > 0
                ? string.Join(", ", profile.CategoricalSummary.TopValues.Select(item => $"{item.Value} ({item.Count})"))
                : "n/a";

            sections.Add(new ReportSection
            {
                Heading = profile.ColumnName,
                Content = $"distinct={profile.DistinctCount}, completeness={profile.CompletenessRatio:P2}, top values={topValues}"
            });
        }

        if (request.GroupByColumns.Count > 0 && request.Aggregations.Count > 0)
        {
            var grouped = await GroupAndAggregateAsync(new GroupAggregationRequest
            {
                GroupByColumns = request.GroupByColumns,
                Aggregations = request.Aggregations,
                Filter = request.BaseFilter,
                SortRules = request.Aggregations.Select(aggregation => new SortRule
                {
                    ColumnName = aggregation.Alias,
                    Direction = SortDirection.Descending
                }).ToArray(),
                Page = 1,
                PageSize = 5
            }, cancellationToken).ConfigureAwait(false);

            var lines = grouped.Rows
                .Select(row => string.Join(", ", grouped.Columns.Select(column => $"{column}={row.Values[column]}")));
            sections.Add(new ReportSection
            {
                Heading = "Grouped view",
                Content = string.Join(Environment.NewLine, lines)
            });
        }

        return new DatasetReport
        {
            Title = request.Title,
            Summary = $"Generated {sections.Count} sections over {profiles.ScopedRowCount} scoped rows.",
            GeneratedAtUtc = DateTimeOffset.UtcNow,
            Sections = sections
        };
    }

    private static void ValidateGroupByBins(
        IReadOnlyList<NumericGroupBinSpec> bins,
        IReadOnlyList<string> groupByColumns,
        IReadOnlyDictionary<string, ColumnSchema> schemaLookup,
        IReadOnlyList<NumericGroupAutoBinSpec> autoBinSpecs)
    {
        var autoAliases = new HashSet<string>(
            autoBinSpecs.Select(spec => spec.Alias.Trim()).Where(a => a.Length > 0),
            StringComparer.OrdinalIgnoreCase);
        var groupNameSet = new HashSet<string>(groupByColumns, StringComparer.OrdinalIgnoreCase);
        foreach (var bin in bins)
        {
            if (string.IsNullOrWhiteSpace(bin.ColumnName))
            {
                throw new ArgumentException("Each groupByBins item must include a non-empty columnName.");
            }

            if (string.IsNullOrWhiteSpace(bin.Alias))
            {
                throw new ArgumentException("Each groupByBins item must include a non-empty alias.");
            }

            if (autoAliases.Contains(bin.Alias.Trim()))
            {
                throw new ArgumentException($"groupByBins alias '{bin.Alias}' collides with a groupByAutoBins alias.");
            }

            if (groupNameSet.Contains(bin.Alias.Trim()))
            {
                throw new ArgumentException($"groupByBins alias '{bin.Alias}' collides with a groupByColumns name.");
            }

            if (bin.BinWidth <= 0
                || double.IsNaN(bin.BinWidth)
                || double.IsInfinity(bin.BinWidth)
                || bin.BinWidth > 1_000_000d)
            {
                throw new ArgumentException($"BinWidth for '{bin.Alias}' must be a finite positive number within a reasonable range.");
            }

            var column = GetRequiredColumn(bin.ColumnName.Trim(), schemaLookup);
            if (!column.IsNumeric)
            {
                throw new ArgumentException($"groupByBins column '{column.Name}' must be numeric.");
            }
        }

        var aliases = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var bin in bins)
        {
            if (!aliases.Add(bin.Alias.Trim()))
            {
                throw new ArgumentException($"Duplicate groupByBins alias '{bin.Alias}'.");
            }
        }
    }

    private static void ValidateGroupByAutoBins(
        IReadOnlyList<NumericGroupAutoBinSpec> autoBins,
        IReadOnlyList<string> groupByColumns,
        IReadOnlyList<NumericGroupBinSpec> manualBins,
        IReadOnlyDictionary<string, ColumnSchema> schemaLookup)
    {
        var reserved = new HashSet<string>(groupByColumns, StringComparer.OrdinalIgnoreCase);
        foreach (var manual in manualBins)
        {
            reserved.Add(manual.Alias.Trim());
        }

        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var spec in autoBins)
        {
            if (string.IsNullOrWhiteSpace(spec.ColumnName))
            {
                throw new ArgumentException("Each groupByAutoBins item must include a non-empty columnName.");
            }

            if (string.IsNullOrWhiteSpace(spec.Alias))
            {
                throw new ArgumentException("Each groupByAutoBins item must include a non-empty alias.");
            }

            var alias = spec.Alias.Trim();
            if (reserved.Contains(alias))
            {
                throw new ArgumentException($"groupByAutoBins alias '{spec.Alias}' collides with a groupByColumns name or groupByBins alias.");
            }

            if (!seen.Add(alias))
            {
                throw new ArgumentException($"Duplicate groupByAutoBins alias '{spec.Alias}'.");
            }

            var binCount = GroupByAutoBinAssignments.ResolveBinCount(spec.BinCount);
            GroupByAutoBinAssignments.ValidateBinCount(binCount);

            var column = GetRequiredColumn(spec.ColumnName.Trim(), schemaLookup);
            if (!column.IsNumeric)
            {
                throw new ArgumentException($"groupByAutoBins column '{column.Name}' must be numeric.");
            }
        }
    }

    private static void ValidateDerivedMetricDefinitions(
        IReadOnlyList<DerivedMetricDefinition> derivedMetrics,
        IReadOnlyList<string> groupByColumns,
        IReadOnlyList<NumericGroupBinSpec> manualBins,
        IReadOnlyList<NumericGroupAutoBinSpec> autoBins,
        IReadOnlyList<AggregationDefinition> aggregations,
        IReadOnlyDictionary<string, ColumnSchema> schemaLookup)
    {
        var reservedNames = new HashSet<string>(
            groupByColumns
                .Concat(manualBins.Select(b => b.Alias))
                .Concat(autoBins.Select(b => b.Alias)),
            StringComparer.OrdinalIgnoreCase);

        foreach (var agg in aggregations)
        {
            reservedNames.Add(agg.Alias);
        }

        var allowedForExpression = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var columnName in groupByColumns)
        {
            if (schemaLookup.TryGetValue(columnName, out var column) && column.IsNumeric)
            {
                allowedForExpression.Add(columnName);
            }
        }

        foreach (var bin in manualBins)
        {
            allowedForExpression.Add(bin.Alias);
        }

        foreach (var bin in autoBins)
        {
            allowedForExpression.Add(bin.Alias);
        }

        foreach (var agg in aggregations)
        {
            allowedForExpression.Add(agg.Alias);
        }

        var derivedSeen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var derived in derivedMetrics)
        {
            if (string.IsNullOrWhiteSpace(derived.Alias))
            {
                throw new ArgumentException("Each derivedMetrics item must include a non-empty alias.");
            }

            var alias = derived.Alias.Trim();
            if (reservedNames.Contains(alias))
            {
                throw new ArgumentException(
                    $"Derived metric alias '{derived.Alias}' collides with a grouping dimension, bin alias, or aggregation alias.");
            }

            if (!derivedSeen.Add(alias))
            {
                throw new ArgumentException($"Duplicate derived metric alias '{derived.Alias}'.");
            }

            if (string.IsNullOrWhiteSpace(derived.Expression))
            {
                throw new ArgumentException($"Derived metric '{derived.Alias}' must include a non-empty expression.");
            }

            DerivedMetricExpressionEvaluator.Validate(derived.Expression, allowedForExpression);
            allowedForExpression.Add(alias);
        }
    }

    private static IReadOnlyList<Dictionary<DatasetRow, double>> BuildAutoBinRowMaps(
        IReadOnlyList<DatasetRow> filteredRows,
        IReadOnlyList<NumericGroupAutoBinSpec> autoBinSpecs)
    {
        if (autoBinSpecs.Count == 0)
        {
            return Array.Empty<Dictionary<DatasetRow, double>>();
        }

        var maps = new List<Dictionary<DatasetRow, double>>();
        foreach (var spec in autoBinSpecs)
        {
            var binCount = GroupByAutoBinAssignments.ResolveBinCount(spec.BinCount);
            GroupByAutoBinAssignments.ValidateBinCount(binCount);
            var column = spec.ColumnName.Trim();
            var map = spec.Method switch
            {
                GroupByAutoBinMethod.EqualWidth => GroupByAutoBinAssignments.BuildEqualWidthRowToLowerBound(
                    filteredRows,
                    column,
                    binCount),
                GroupByAutoBinMethod.Quantile => GroupByAutoBinAssignments.BuildQuantileRowToLowerBound(
                    filteredRows,
                    column,
                    binCount),
                _ => throw new ArgumentException($"Unsupported groupByAutoBins method '{spec.Method}'.")
            };
            maps.Add(map);
        }

        return maps;
    }

    private static IReadOnlyList<GroupByAutoBinAppliedSummary> BuildAutoBinAppliedSummaries(
        IReadOnlyList<NumericGroupAutoBinSpec> specs,
        IReadOnlyList<Dictionary<DatasetRow, double>> maps)
    {
        if (specs.Count == 0)
        {
            return Array.Empty<GroupByAutoBinAppliedSummary>();
        }

        var list = new List<GroupByAutoBinAppliedSummary>();
        for (var i = 0; i < specs.Count; i++)
        {
            var spec = specs[i];
            var map = maps[i];
            var values = map.Values.ToArray();
            var distinct = values.Distinct().Count();
            var binCount = GroupByAutoBinAssignments.ResolveBinCount(spec.BinCount);
            list.Add(new GroupByAutoBinAppliedSummary
            {
                Alias = spec.Alias.Trim(),
                Method = spec.Method.ToString(),
                BinCount = binCount,
                ScopedMin = values.Length == 0 ? null : values.Min(),
                ScopedMax = values.Length == 0 ? null : values.Max(),
                DistinctBinKeysObserved = distinct
            });
        }

        return list;
    }

    private static IReadOnlyList<(IReadOnlyList<object?> KeyValues, DatasetRow[] Rows)> BuildGroupsWithBinDimensions(
        IReadOnlyList<DatasetRow> rows,
        IReadOnlyList<string> groupByColumns,
        IReadOnlyList<NumericGroupBinSpec> manualBinSpecs,
        IReadOnlyList<NumericGroupAutoBinSpec> autoBinSpecs,
        IReadOnlyList<Dictionary<DatasetRow, double>> autoBinMaps)
    {
        var projected = new List<(GroupKey Key, DatasetRow Row)>();
        foreach (var row in rows)
        {
            if (!TryBuildFullGroupKey(row, groupByColumns, manualBinSpecs, autoBinMaps, out var keyValues))
            {
                continue;
            }

            projected.Add((new GroupKey(keyValues), row));
        }

        if (projected.Count == 0)
        {
            return Array.Empty<(IReadOnlyList<object?>, DatasetRow[])>();
        }

        return projected
            .GroupBy(tuple => tuple.Key, GroupKeyComparer.Instance)
            .Select(group => (KeyValues: (IReadOnlyList<object?>)group.Key.Values, Rows: group.Select(t => t.Row).ToArray()))
            .ToArray();
    }

    private static bool TryBuildFullGroupKey(
        DatasetRow row,
        IReadOnlyList<string> groupByColumns,
        IReadOnlyList<NumericGroupBinSpec> manualBinSpecs,
        IReadOnlyList<Dictionary<DatasetRow, double>> autoBinMaps,
        out object?[] keyValues)
    {
        keyValues = new object?[groupByColumns.Count + manualBinSpecs.Count + autoBinMaps.Count];
        for (var i = 0; i < groupByColumns.Count; i++)
        {
            keyValues[i] = GetValue(row, groupByColumns[i]);
        }

        for (var b = 0; b < manualBinSpecs.Count; b++)
        {
            var spec = manualBinSpecs[b];
            if (!TryGetNumericMeasurement(GetValue(row, spec.ColumnName), out var measurement))
            {
                return false;
            }

            var width = spec.BinWidth;
            var lower = Math.Floor(measurement / width) * width;
            keyValues[groupByColumns.Count + b] = lower;
        }

        for (var a = 0; a < autoBinMaps.Count; a++)
        {
            if (!autoBinMaps[a].TryGetValue(row, out var bound))
            {
                return false;
            }

            keyValues[groupByColumns.Count + manualBinSpecs.Count + a] = bound;
        }

        return true;
    }

    private static bool TryGetNumericMeasurement(object? raw, out double value)
    {
        value = default;
        if (raw is null)
        {
            return false;
        }

        switch (raw)
        {
            case double d:
                value = d;
                return true;
            case float f:
                value = f;
                return true;
            case int i:
                value = i;
                return true;
            case long l:
                value = l;
                return true;
            case decimal m:
                value = (double)m;
                return true;
            case string text when double.TryParse(text, System.Globalization.NumberStyles.Any, System.Globalization.CultureInfo.InvariantCulture, out var parsed):
                value = parsed;
                return true;
            default:
                try
                {
                    value = Convert.ToDouble(raw, System.Globalization.CultureInfo.InvariantCulture);
                    return !double.IsNaN(value) && !double.IsInfinity(value);
                }
                catch
                {
                    return false;
                }
        }
    }

    private async Task<(DatasetSnapshot Snapshot, IReadOnlyDictionary<string, ColumnSchema> SchemaLookup)> GetContextAsync(CancellationToken cancellationToken)
    {
        var snapshot = await _repository.GetDatasetAsync(cancellationToken).ConfigureAwait(false);
        var schemaLookup = snapshot.Schema.Columns.ToDictionary(column => column.Name, StringComparer.OrdinalIgnoreCase);
        return (snapshot, schemaLookup);
    }

    private static DatasetRow[] FilterRows(
        IEnumerable<DatasetRow> rows,
        FilterExpression? filter,
        IReadOnlyDictionary<string, ColumnSchema> schemaLookup)
        => rows.Where(row => FilterEvaluator.Matches(row, filter, schemaLookup)).ToArray();

    private static IEnumerable<object?> SelectColumnValues(IEnumerable<DatasetRow> rows, string columnName)
        => rows.Select(row => GetValue(row, columnName));

    private static object? GetValue(ITabularRow row, string columnName)
        => row.TryGetValue(columnName, out var value) ? value : null;

    private static ColumnSchema GetRequiredColumn(string columnName, IReadOnlyDictionary<string, ColumnSchema> schemaLookup)
    {
        if (string.IsNullOrWhiteSpace(columnName))
        {
            throw new ArgumentException("Column name must be provided.", nameof(columnName));
        }

        return schemaLookup.TryGetValue(columnName, out var column)
            ? column
            : throw new ArgumentException($"Column '{columnName}' was not found.", nameof(columnName));
    }

    private static void ValidatePaging(int page, int pageSize)
    {
        if (page <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(page), "Page must be greater than zero.");
        }

        if (pageSize <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(pageSize), "Page size must be greater than zero.");
        }
    }

    private static void ValidateColumns(IEnumerable<string> columns, IReadOnlyDictionary<string, ColumnSchema> schemaLookup)
    {
        foreach (var column in columns.Where(column => !string.IsNullOrWhiteSpace(column)))
        {
            if (!schemaLookup.ContainsKey(column))
            {
                throw new ArgumentException($"Column '{column}' was not found in the dataset schema.");
            }
        }
    }

    private static void ValidateFilter(FilterExpression? filter, IReadOnlyDictionary<string, ColumnSchema> schemaLookup)
    {
        foreach (var column in FilterEvaluator.CollectReferencedColumns(filter))
        {
            GetRequiredColumn(column, schemaLookup);
        }
    }

    private static void ValidateAggregations(
        IReadOnlyList<AggregationDefinition> aggregations,
        IReadOnlyList<string> groupByColumns,
        IReadOnlyDictionary<string, ColumnSchema> schemaLookup)
    {
        var aliases = new HashSet<string>(groupByColumns, StringComparer.OrdinalIgnoreCase);
        foreach (var aggregation in aggregations)
        {
            if (string.IsNullOrWhiteSpace(aggregation.Alias))
            {
                throw new ArgumentException("Aggregation alias must be provided.");
            }

            if (!aliases.Add(aggregation.Alias))
            {
                throw new ArgumentException($"Aggregation alias '{aggregation.Alias}' is duplicated or collides with a grouping dimension.");
            }

            ValidateFilter(aggregation.Filter, schemaLookup);

            if (aggregation.Function == AggregateFunction.Count)
            {
                continue;
            }

            if (string.IsNullOrWhiteSpace(aggregation.ColumnName) || !schemaLookup.TryGetValue(aggregation.ColumnName, out var column))
            {
                throw new ArgumentException($"Aggregation column '{aggregation.ColumnName}' was not found.");
            }

            if (aggregation.Function is AggregateFunction.Sum or AggregateFunction.Average or AggregateFunction.Median or AggregateFunction.StandardDeviation
                && !column.IsNumeric)
            {
                throw new ArgumentException($"Aggregation '{aggregation.Function}' requires a numeric column. '{aggregation.ColumnName}' is {column.DataType}.");
            }
        }
    }

    private static IReadOnlyList<(IReadOnlyList<object?> KeyValues, DatasetRow[] Rows)> BuildGroups(
        IReadOnlyList<DatasetRow> rows,
        IReadOnlyList<string> groupByColumns)
    {
        if (groupByColumns.Count == 0)
        {
            return [(KeyValues: Array.Empty<object?>(), Rows: rows.ToArray())];
        }

        return rows
            .GroupBy(
                row => new GroupKey(groupByColumns.Select(column => GetValue(row, column)).ToArray()),
                GroupKeyComparer.Instance)
            .Select(group => (KeyValues: (IReadOnlyList<object?>)group.Key.Values, Rows: group.ToArray()))
            .ToArray();
    }

    private static GroupAggregationRow BuildGroupAggregationRowWithDerived(
        IReadOnlyList<object?> keyValues,
        IReadOnlyList<DatasetRow> rows,
        IReadOnlyList<string> groupByColumns,
        IReadOnlyList<AggregationDefinition> aggregations,
        IReadOnlyList<DerivedMetricDefinition> derivedMetrics,
        IReadOnlyDictionary<string, ColumnSchema> sourceSchema)
    {
        var values = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        for (var index = 0; index < groupByColumns.Count; index++)
        {
            values[groupByColumns[index]] = keyValues[index];
        }

        foreach (var aggregation in aggregations)
        {
            values[aggregation.Alias] = EvaluateAggregation(rows, aggregation, sourceSchema);
        }

        foreach (var derived in derivedMetrics)
        {
            var evaluated = DerivedMetricExpressionEvaluator.Evaluate(derived.Expression, values);
            values[derived.Alias.Trim()] = evaluated.HasValue ? evaluated.Value : null;
        }

        return new GroupAggregationRow
        {
            Values = new ReadOnlyDictionary<string, object?>(values)
        };
    }

    private static IReadOnlyDictionary<string, ColumnSchema> BuildAggregateSchema(
        IReadOnlyList<string> groupByColumns,
        IReadOnlyList<NumericGroupBinSpec> manualBinSpecs,
        IReadOnlyList<NumericGroupAutoBinSpec> autoBinSpecs,
        IReadOnlyList<AggregationDefinition> aggregations,
        IReadOnlyList<DerivedMetricDefinition> derivedMetrics,
        IReadOnlyDictionary<string, ColumnSchema> sourceSchema)
    {
        var result = new Dictionary<string, ColumnSchema>(StringComparer.OrdinalIgnoreCase);
        foreach (var groupByColumn in groupByColumns)
        {
            result[groupByColumn] = sourceSchema[groupByColumn];
        }

        foreach (var bin in manualBinSpecs)
        {
            var source = sourceSchema[bin.ColumnName];
            result[bin.Alias] = source with { Name = bin.Alias };
        }

        foreach (var auto in autoBinSpecs)
        {
            var source = sourceSchema[auto.ColumnName.Trim()];
            result[auto.Alias.Trim()] = source with { Name = auto.Alias.Trim() };
        }

        foreach (var aggregation in aggregations)
        {
            result[aggregation.Alias] = BuildAggregationColumnSchema(aggregation, sourceSchema);
        }

        foreach (var derived in derivedMetrics)
        {
            result[derived.Alias.Trim()] = new ColumnSchema
            {
                Name = derived.Alias.Trim(),
                DataType = DataTypeKind.Decimal,
                IsNumeric = true,
                IsNullable = true
            };
        }

        return result;
    }

    private static ColumnSchema BuildAggregationColumnSchema(
        AggregationDefinition aggregation,
        IReadOnlyDictionary<string, ColumnSchema> sourceSchema)
    {
        return aggregation.Function switch
        {
            AggregateFunction.Count or AggregateFunction.CountDistinct => new ColumnSchema
            {
                Name = aggregation.Alias,
                DataType = DataTypeKind.Integer,
                IsNumeric = true,
                IsNullable = false
            },
            AggregateFunction.Sum or AggregateFunction.Average or AggregateFunction.Median or AggregateFunction.StandardDeviation => new ColumnSchema
            {
                Name = aggregation.Alias,
                DataType = DataTypeKind.Decimal,
                IsNumeric = true,
                IsNullable = true
            },
            AggregateFunction.Min or AggregateFunction.Max when sourceSchema.TryGetValue(aggregation.ColumnName, out var sourceColumn) => sourceColumn with
            {
                Name = aggregation.Alias
            },
            _ => new ColumnSchema
            {
                Name = aggregation.Alias,
                DataType = DataTypeKind.Unknown
            }
        };
    }

    private static object? EvaluateAggregation(
        IEnumerable<DatasetRow> rows,
        AggregationDefinition aggregation,
        IReadOnlyDictionary<string, ColumnSchema> sourceSchema)
    {
        var scopedRows = aggregation.Filter is null
            ? rows
            : rows.Where(row => FilterEvaluator.Matches(row, aggregation.Filter, sourceSchema));

        var materializedRows = scopedRows.ToArray();
        var values = string.IsNullOrWhiteSpace(aggregation.ColumnName)
            ? materializedRows.Select(_ => (object?)1)
            : SelectColumnValues(materializedRows, aggregation.ColumnName);

        return aggregation.Function switch
        {
            AggregateFunction.Count => materializedRows.Length,
            AggregateFunction.CountDistinct => values.Where(value => value is not null).Distinct(DatasetValueComparer.Instance).Count(),
            AggregateFunction.Sum => AnalyticsComputation.BuildNumericSummary(aggregation.ColumnName, values).Sum,
            AggregateFunction.Average => AnalyticsComputation.BuildNumericSummary(aggregation.ColumnName, values).Average,
            AggregateFunction.Min => values.Where(value => value is not null).OrderBy(value => value, DatasetValueComparer.Instance).FirstOrDefault(),
            AggregateFunction.Max => values.Where(value => value is not null).OrderByDescending(value => value, DatasetValueComparer.Instance).FirstOrDefault(),
            AggregateFunction.Median => AnalyticsComputation.BuildNumericSummary(aggregation.ColumnName, values).Median,
            AggregateFunction.StandardDeviation => AnalyticsComputation.BuildNumericSummary(aggregation.ColumnName, values).StandardDeviation,
            _ => throw new InvalidOperationException($"Unsupported aggregation function '{aggregation.Function}'.")
        };
    }
}
