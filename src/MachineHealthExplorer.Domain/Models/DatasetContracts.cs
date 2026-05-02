using System.Collections.ObjectModel;

namespace MachineHealthExplorer.Domain.Models;

public enum DataTypeKind
{
    Unknown = 0,
    Boolean = 1,
    Integer = 2,
    Decimal = 3,
    DateTime = 4,
    String = 5
}

public enum CardinalityHint
{
    Unknown = 0,
    Low = 1,
    Medium = 2,
    High = 3
}

public enum FilterOperator
{
    Equals = 0,
    NotEquals = 1,
    GreaterThan = 2,
    GreaterThanOrEqual = 3,
    LessThan = 4,
    LessThanOrEqual = 5,
    Contains = 6,
    In = 7,
    Between = 8,
    IsNull = 9,
    IsNotNull = 10,
    StartsWith = 11,
    EndsWith = 12,
    NotIn = 13
}

public enum LogicalOperator
{
    And = 0,
    Or = 1
}

public enum SortDirection
{
    Ascending = 0,
    Descending = 1
}

public enum AggregateFunction
{
    Count = 0,
    CountDistinct = 1,
    Sum = 2,
    Average = 3,
    Min = 4,
    Max = 5,
    Median = 6,
    StandardDeviation = 7
}

public interface ITabularRow
{
    IReadOnlyDictionary<string, object?> Values { get; }
    bool TryGetValue(string columnName, out object? value);
}

public sealed class DatasetRow : ITabularRow
{
    public DatasetRow(IReadOnlyDictionary<string, object?> values)
    {
        Values = new ReadOnlyDictionary<string, object?>(new Dictionary<string, object?>(values, StringComparer.OrdinalIgnoreCase));
    }

    public IReadOnlyDictionary<string, object?> Values { get; }

    public object? this[string columnName] =>
        Values.TryGetValue(columnName, out var value)
            ? value
            : throw new KeyNotFoundException($"Column '{columnName}' was not found in the row.");

    public bool TryGetValue(string columnName, out object? value) => Values.TryGetValue(columnName, out value);

    public DatasetRow SelectColumns(IReadOnlyCollection<string> selectedColumns)
    {
        var projection = Values
            .Where(pair => selectedColumns.Contains(pair.Key, StringComparer.OrdinalIgnoreCase))
            .ToDictionary(pair => pair.Key, pair => pair.Value, StringComparer.OrdinalIgnoreCase);

        return new DatasetRow(projection);
    }
}

public sealed record DatasetSnapshot
{
    public string DatasetName { get; init; } = string.Empty;
    public string SourcePath { get; init; } = string.Empty;
    public DatasetSchema Schema { get; init; } = new();
    public IReadOnlyList<DatasetRow> Rows { get; init; } = Array.Empty<DatasetRow>();
}

public sealed record DatasetSchema
{
    public string DatasetName { get; init; } = string.Empty;
    public int RowCount { get; init; }
    public DateTimeOffset GeneratedAtUtc { get; init; } = DateTimeOffset.UtcNow;
    public IReadOnlyList<ColumnSchema> Columns { get; init; } = Array.Empty<ColumnSchema>();
}

public sealed record ColumnSchema
{
    public string Name { get; init; } = string.Empty;
    public DataTypeKind DataType { get; init; } = DataTypeKind.Unknown;
    public bool IsNullable { get; init; }
    public int NonNullCount { get; init; }
    public double CompletenessRatio { get; init; }
    public bool IsNumeric { get; init; }
    public bool IsCategorical { get; init; }
    public int NullCount { get; init; }
    public int DistinctCount { get; init; }
    public double DistinctRatio { get; init; }
    public CardinalityHint CardinalityHint { get; init; } = CardinalityHint.Unknown;
    public IReadOnlyList<string> SampleValues { get; init; } = Array.Empty<string>();
    public NumericSummary? NumericSummary { get; init; }
    public CategoricalSummary? CategoricalSummary { get; init; }
}

public sealed record ColumnProfile
{
    public string ColumnName { get; init; } = string.Empty;
    public DataTypeKind DataType { get; init; } = DataTypeKind.Unknown;
    public bool IsNullable { get; init; }
    public int NonNullCount { get; init; }
    public double CompletenessRatio { get; init; }
    public bool IsNumeric { get; init; }
    public bool IsCategorical { get; init; }
    public int RowCount { get; init; }
    public int NullCount { get; init; }
    public int DistinctCount { get; init; }
    public CardinalityHint CardinalityHint { get; init; } = CardinalityHint.Unknown;
    public IReadOnlyList<string> SampleValues { get; init; } = Array.Empty<string>();
    public NumericSummary? NumericSummary { get; init; }
    public CategoricalSummary? CategoricalSummary { get; init; }
}

public sealed record NumericSummary
{
    public string ColumnName { get; init; } = string.Empty;
    public int Count { get; init; }
    public double? Sum { get; init; }
    public double? Average { get; init; }
    public double? Min { get; init; }
    public double? Max { get; init; }
    public double? Median { get; init; }
    public double? StandardDeviation { get; init; }
}

public sealed record CategoricalSummary
{
    public string ColumnName { get; init; } = string.Empty;
    public int Count { get; init; }
    public int DistinctCount { get; init; }
    public IReadOnlyList<ValueFrequency> TopValues { get; init; } = Array.Empty<ValueFrequency>();
}

public sealed record ValueFrequency
{
    public string Value { get; init; } = string.Empty;
    public int Count { get; init; }
    public double Percentage { get; init; }
}

public abstract record FilterExpression;

public sealed record FilterConditionExpression : FilterExpression
{
    public string ColumnName { get; init; } = string.Empty;
    public FilterOperator Operator { get; init; }
    public object? Value { get; init; }
    public object? SecondValue { get; init; }
    public IReadOnlyList<object?> Values { get; init; } = Array.Empty<object?>();
}

public sealed record FilterGroupExpression : FilterExpression
{
    public LogicalOperator Operator { get; init; } = LogicalOperator.And;
    public IReadOnlyList<FilterExpression> Expressions { get; init; } = Array.Empty<FilterExpression>();
}

public static class DatasetFilters
{
    public static FilterConditionExpression Equal(string columnName, object? value)
        => CreateCondition(columnName, FilterOperator.Equals, value);

    public static FilterConditionExpression NotEqual(string columnName, object? value)
        => CreateCondition(columnName, FilterOperator.NotEquals, value);

    public static FilterConditionExpression GreaterThan(string columnName, object? value)
        => CreateCondition(columnName, FilterOperator.GreaterThan, value);

    public static FilterConditionExpression GreaterThanOrEqual(string columnName, object? value)
        => CreateCondition(columnName, FilterOperator.GreaterThanOrEqual, value);

    public static FilterConditionExpression LessThan(string columnName, object? value)
        => CreateCondition(columnName, FilterOperator.LessThan, value);

    public static FilterConditionExpression LessThanOrEqual(string columnName, object? value)
        => CreateCondition(columnName, FilterOperator.LessThanOrEqual, value);

    public static FilterConditionExpression Contains(string columnName, object? value)
        => CreateCondition(columnName, FilterOperator.Contains, value);

    public static FilterConditionExpression StartsWith(string columnName, object? value)
        => CreateCondition(columnName, FilterOperator.StartsWith, value);

    public static FilterConditionExpression EndsWith(string columnName, object? value)
        => CreateCondition(columnName, FilterOperator.EndsWith, value);

    public static FilterConditionExpression In(string columnName, params object?[] values)
        => new()
        {
            ColumnName = columnName,
            Operator = FilterOperator.In,
            Values = values
        };

    public static FilterConditionExpression NotIn(string columnName, params object?[] values)
        => new()
        {
            ColumnName = columnName,
            Operator = FilterOperator.NotIn,
            Values = values
        };

    public static FilterConditionExpression Between(string columnName, object? lowerBound, object? upperBound)
        => new()
        {
            ColumnName = columnName,
            Operator = FilterOperator.Between,
            Value = lowerBound,
            SecondValue = upperBound
        };

    public static FilterConditionExpression IsNull(string columnName)
        => new()
        {
            ColumnName = columnName,
            Operator = FilterOperator.IsNull
        };

    public static FilterConditionExpression IsNotNull(string columnName)
        => new()
        {
            ColumnName = columnName,
            Operator = FilterOperator.IsNotNull
        };

    public static FilterExpression? And(params FilterExpression?[] expressions)
        => Combine(LogicalOperator.And, expressions);

    public static FilterExpression? Or(params FilterExpression?[] expressions)
        => Combine(LogicalOperator.Or, expressions);

    private static FilterConditionExpression CreateCondition(string columnName, FilterOperator @operator, object? value)
        => new()
        {
            ColumnName = columnName,
            Operator = @operator,
            Value = value
        };

    private static FilterExpression? Combine(LogicalOperator @operator, params FilterExpression?[] expressions)
    {
        var materialized = expressions
            .Where(expression => expression is not null)
            .Cast<FilterExpression>()
            .ToArray();

        return materialized.Length switch
        {
            0 => null,
            1 => materialized[0],
            _ => new FilterGroupExpression
            {
                Operator = @operator,
                Expressions = materialized
            }
        };
    }
}

public sealed record SortRule
{
    public string ColumnName { get; init; } = string.Empty;
    public SortDirection Direction { get; init; } = SortDirection.Ascending;
}

public static class DatasetSorts
{
    public static SortRule Ascending(string columnName)
        => new()
        {
            ColumnName = columnName,
            Direction = SortDirection.Ascending
        };

    public static SortRule Descending(string columnName)
        => new()
        {
            ColumnName = columnName,
            Direction = SortDirection.Descending
        };
}

public sealed record AggregationDefinition
{
    public string Alias { get; init; } = string.Empty;
    public string ColumnName { get; init; } = string.Empty;
    public AggregateFunction Function { get; init; }
    public FilterExpression? Filter { get; init; }
}

public static class DatasetAggregations
{
    public static AggregationDefinition Count(string alias, FilterExpression? filter = null)
        => Create(alias, string.Empty, AggregateFunction.Count, filter);

    public static AggregationDefinition CountDistinct(string alias, string columnName, FilterExpression? filter = null)
        => Create(alias, columnName, AggregateFunction.CountDistinct, filter);

    public static AggregationDefinition Sum(string alias, string columnName, FilterExpression? filter = null)
        => Create(alias, columnName, AggregateFunction.Sum, filter);

    public static AggregationDefinition Average(string alias, string columnName, FilterExpression? filter = null)
        => Create(alias, columnName, AggregateFunction.Average, filter);

    public static AggregationDefinition Min(string alias, string columnName, FilterExpression? filter = null)
        => Create(alias, columnName, AggregateFunction.Min, filter);

    public static AggregationDefinition Max(string alias, string columnName, FilterExpression? filter = null)
        => Create(alias, columnName, AggregateFunction.Max, filter);

    public static AggregationDefinition Median(string alias, string columnName, FilterExpression? filter = null)
        => Create(alias, columnName, AggregateFunction.Median, filter);

    public static AggregationDefinition StandardDeviation(string alias, string columnName, FilterExpression? filter = null)
        => Create(alias, columnName, AggregateFunction.StandardDeviation, filter);

    private static AggregationDefinition Create(string alias, string columnName, AggregateFunction function, FilterExpression? filter)
        => new()
        {
            Alias = alias,
            ColumnName = columnName,
            Function = function,
            Filter = filter
        };
}

public sealed record QueryRequest
{
    public FilterExpression? Filter { get; init; }
    public IReadOnlyList<string> SelectedColumns { get; init; } = Array.Empty<string>();
    public IReadOnlyList<SortRule> SortRules { get; init; } = Array.Empty<SortRule>();
    public int Page { get; init; } = 1;
    public int PageSize { get; init; } = 50;
}

public sealed record QueryResult
{
    public IReadOnlyList<string> Columns { get; init; } = Array.Empty<string>();
    public IReadOnlyList<DatasetRow> Rows { get; init; } = Array.Empty<DatasetRow>();
    public int TotalCount { get; init; }
    public int Page { get; init; }
    public int PageSize { get; init; }
}

public sealed record ColumnExtremaRequest
{
    public string ColumnName { get; init; } = string.Empty;
    /// <summary>Case-insensitive: "max" (default) or "min".</summary>
    public string Mode { get; init; } = "max";
    public FilterExpression? Filter { get; init; }
    public IReadOnlyList<string> ExtraSelectedColumns { get; init; } = Array.Empty<string>();
    public int MaxTieRows { get; init; } = 10;
}

public sealed record ColumnExtremaResult
{
    public string ColumnName { get; init; } = string.Empty;
    public string Mode { get; init; } = "max";
    public object? ExtremumValue { get; init; }
    public int ScopedRowCount { get; init; }
    public int TotalMatchingRows { get; init; }
    public IReadOnlyList<string> Columns { get; init; } = Array.Empty<string>();
    public IReadOnlyList<DatasetRow> Rows { get; init; } = Array.Empty<DatasetRow>();
}

public sealed record DistinctValuesRequest
{
    public string ColumnName { get; init; } = string.Empty;
    public int Limit { get; init; } = 25;
    public FilterExpression? Filter { get; init; }
}

public sealed record DistinctValuesResult
{
    public string ColumnName { get; init; } = string.Empty;
    public int TotalDistinctCount { get; init; }
    public IReadOnlyList<object?> Values { get; init; } = Array.Empty<object?>();
    public IReadOnlyList<ValueFrequency> Frequencies { get; init; } = Array.Empty<ValueFrequency>();
}

/// <summary>
/// Numeric histogram bin for grouping: bucket lower bound is <c>Floor(value / binWidth) * binWidth</c>
/// (same idea as <c>GROUP BY FLOOR(column / binWidth) * binWidth</c>).
/// </summary>
public sealed record NumericGroupBinSpec
{
    public string ColumnName { get; init; } = string.Empty;
    public string Alias { get; init; } = string.Empty;
    public double BinWidth { get; init; } = 1.0;
}

/// <summary>
/// Automatic numeric binning in the filtered scope (equal-width or quantile-frequency).
/// Bin keys written to <see cref="GroupAggregationRow"/> are numeric lower bounds for both methods so they can be sorted/filtered like manual bins.
/// </summary>
public enum GroupByAutoBinMethod
{
    EqualWidth = 0,
    Quantile = 1
}

public sealed record NumericGroupAutoBinSpec
{
    public string ColumnName { get; init; } = string.Empty;
    public string Alias { get; init; } = string.Empty;
    public GroupByAutoBinMethod Method { get; init; } = GroupByAutoBinMethod.EqualWidth;
    /// <summary>Number of bins; when omitted a conservative default (10) is applied at execution.</summary>
    public int? BinCount { get; init; }
}

public sealed record DerivedMetricDefinition
{
    public string Alias { get; init; } = string.Empty;
    /// <summary>Restricted arithmetic over aggregation aliases and numeric grouping dimensions (identifiers, + - * / parentheses, numeric literals).</summary>
    public string Expression { get; init; } = string.Empty;
}

/// <summary>Neutral summary of an applied auto-bin dimension (for model-facing JSON).</summary>
public sealed record GroupByAutoBinAppliedSummary
{
    public string Alias { get; init; } = string.Empty;
    public string Method { get; init; } = string.Empty;
    public int BinCount { get; init; }
    public double? ScopedMin { get; init; }
    public double? ScopedMax { get; init; }
    public int DistinctBinKeysObserved { get; init; }
}

public sealed record GroupAggregationRequest
{
    public IReadOnlyList<string> GroupByColumns { get; init; } = Array.Empty<string>();
    public IReadOnlyList<NumericGroupBinSpec> GroupByBins { get; init; } = Array.Empty<NumericGroupBinSpec>();
    public IReadOnlyList<NumericGroupAutoBinSpec> GroupByAutoBins { get; init; } = Array.Empty<NumericGroupAutoBinSpec>();
    public IReadOnlyList<AggregationDefinition> Aggregations { get; init; } = Array.Empty<AggregationDefinition>();
    public IReadOnlyList<DerivedMetricDefinition> DerivedMetrics { get; init; } = Array.Empty<DerivedMetricDefinition>();
    public FilterExpression? Filter { get; init; }
    public FilterExpression? Having { get; init; }
    public IReadOnlyList<SortRule> SortRules { get; init; } = Array.Empty<SortRule>();
    public int Page { get; init; } = 1;
    public int PageSize { get; init; } = 50;
}

public sealed record GroupAggregationRow : ITabularRow
{
    public IReadOnlyDictionary<string, object?> Values { get; init; } = new ReadOnlyDictionary<string, object?>(new Dictionary<string, object?>());

    public bool TryGetValue(string columnName, out object? value) => Values.TryGetValue(columnName, out value);
}

public sealed record GroupAggregationResult
{
    public IReadOnlyList<string> Columns { get; init; } = Array.Empty<string>();
    public IReadOnlyList<GroupAggregationRow> Rows { get; init; } = Array.Empty<GroupAggregationRow>();
    public int ScopedRowCount { get; init; }
    public int TotalGroups { get; init; }
    public int Page { get; init; }
    public int PageSize { get; init; }
    public IReadOnlyList<GroupByAutoBinAppliedSummary> GroupByAutoBinsApplied { get; init; } = Array.Empty<GroupByAutoBinAppliedSummary>();
}

public sealed record ColumnProfilingRequest
{
    public IReadOnlyList<string> Columns { get; init; } = Array.Empty<string>();
    public FilterExpression? Filter { get; init; }
    public int TopCategoryCount { get; init; } = 10;
}

public sealed record ColumnProfilingResult
{
    public int ScopedRowCount { get; init; }
    public IReadOnlyList<ColumnProfile> Profiles { get; init; } = Array.Empty<ColumnProfile>();
}

public sealed record SubsetComparisonRequest
{
    public string LeftLabel { get; init; } = "Left";
    public FilterExpression? LeftFilter { get; init; }
    public string RightLabel { get; init; } = "Right";
    public FilterExpression? RightFilter { get; init; }
    public IReadOnlyList<string> NumericColumns { get; init; } = Array.Empty<string>();
    public IReadOnlyList<string> CategoricalColumns { get; init; } = Array.Empty<string>();
    public int TopCategoryCount { get; init; } = 5;
}

public sealed record SubsetSnapshot
{
    public string Label { get; init; } = string.Empty;
    public int RowCount { get; init; }
}

public sealed record NumericComparisonMetric
{
    public string ColumnName { get; init; } = string.Empty;
    public NumericSummary Left { get; init; } = new();
    public NumericSummary Right { get; init; } = new();
    public double? AverageDelta { get; init; }
    public double? MedianDelta { get; init; }
}

public sealed record CategoricalComparisonMetric
{
    public string ColumnName { get; init; } = string.Empty;
    public IReadOnlyList<ValueFrequency> LeftTopValues { get; init; } = Array.Empty<ValueFrequency>();
    public IReadOnlyList<ValueFrequency> RightTopValues { get; init; } = Array.Empty<ValueFrequency>();
}

public sealed record SubsetComparisonResult
{
    public SubsetSnapshot Left { get; init; } = new();
    public SubsetSnapshot Right { get; init; } = new();
    public IReadOnlyList<NumericComparisonMetric> NumericComparisons { get; init; } = Array.Empty<NumericComparisonMetric>();
    public IReadOnlyList<CategoricalComparisonMetric> CategoricalComparisons { get; init; } = Array.Empty<CategoricalComparisonMetric>();
}

public sealed record ColumnSearchMatch
{
    public string ColumnName { get; init; } = string.Empty;
    public string MatchReason { get; init; } = string.Empty;
}

public sealed record SearchColumnsResult
{
    public string Keyword { get; init; } = string.Empty;
    public IReadOnlyList<ColumnSearchMatch> Matches { get; init; } = Array.Empty<ColumnSearchMatch>();
}

public sealed record DatasetDescription
{
    public string DatasetName { get; init; } = string.Empty;
    public string SourcePath { get; init; } = string.Empty;
    public int RowCount { get; init; }
    public int ColumnCount { get; init; }
    public IReadOnlyList<string> NumericColumns { get; init; } = Array.Empty<string>();
    public IReadOnlyList<string> CategoricalColumns { get; init; } = Array.Empty<string>();
    public IReadOnlyList<string> PotentialFailureColumns { get; init; } = Array.Empty<string>();
    public IReadOnlyList<string> Highlights { get; init; } = Array.Empty<string>();
}

public sealed record FailureAnalysisSummary
{
    public string FailureIndicatorColumn { get; init; } = string.Empty;
    public int FailureCount { get; init; }
    public int HealthyCount { get; init; }
    public double FailureRate { get; init; }
    public IReadOnlyList<ValueFrequency> FailureModes { get; init; } = Array.Empty<ValueFrequency>();
    public IReadOnlyList<NumericComparisonMetric> NumericComparisons { get; init; } = Array.Empty<NumericComparisonMetric>();
}

public sealed record OperatingConditionSummary
{
    public IReadOnlyList<NumericSummary> NumericSummaries { get; init; } = Array.Empty<NumericSummary>();
    public IReadOnlyList<CategoricalSummary> CategoricalSummaries { get; init; } = Array.Empty<CategoricalSummary>();
}

public sealed record ReportRequest
{
    public string Title { get; init; } = "Dataset Report";
    public FilterExpression? BaseFilter { get; init; }
    public IReadOnlyList<string> FocusColumns { get; init; } = Array.Empty<string>();
    public IReadOnlyList<string> GroupByColumns { get; init; } = Array.Empty<string>();
    public IReadOnlyList<AggregationDefinition> Aggregations { get; init; } = Array.Empty<AggregationDefinition>();
    public int TopCategoryCount { get; init; } = 5;
}

public sealed record ReportSection
{
    public string Heading { get; init; } = string.Empty;
    public string Content { get; init; } = string.Empty;
}

public sealed record DatasetReport
{
    public string Title { get; init; } = string.Empty;
    public string Summary { get; init; } = string.Empty;
    public DateTimeOffset GeneratedAtUtc { get; init; } = DateTimeOffset.UtcNow;
    public IReadOnlyList<ReportSection> Sections { get; init; } = Array.Empty<ReportSection>();
}

public enum AnalysisExampleKind
{
    RowQuery = 0,
    GroupAggregation = 1,
    SubsetComparison = 2
}

public sealed record AnalysisExample
{
    public string Name { get; init; } = string.Empty;
    public string Description { get; init; } = string.Empty;
    public string SuggestedPrompt { get; init; } = string.Empty;
    public AnalysisExampleKind Kind { get; init; }
    public QueryRequest? RowQuery { get; init; }
    public GroupAggregationRequest? GroupAggregationQuery { get; init; }
    public SubsetComparisonRequest? ComparisonQuery { get; init; }
}
