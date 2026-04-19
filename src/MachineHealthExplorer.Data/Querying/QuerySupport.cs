using MachineHealthExplorer.Domain.Models;

namespace MachineHealthExplorer.Data.Querying;

internal static class FilterEvaluator
{
    public static bool Matches(ITabularRow row, FilterExpression? expression, IReadOnlyDictionary<string, ColumnSchema> columns)
    {
        if (expression is null)
        {
            return true;
        }

        return expression switch
        {
            FilterConditionExpression condition => EvaluateCondition(row, condition, columns),
            FilterGroupExpression group => EvaluateGroup(row, group, columns),
            _ => true
        };
    }

    public static IEnumerable<string> CollectReferencedColumns(FilterExpression? expression)
    {
        if (expression is null)
        {
            yield break;
        }

        switch (expression)
        {
            case FilterConditionExpression condition:
                yield return condition.ColumnName;
                break;
            case FilterGroupExpression group:
                foreach (var nested in group.Expressions.SelectMany(CollectReferencedColumns))
                {
                    yield return nested;
                }

                break;
        }
    }

    private static bool EvaluateGroup(ITabularRow row, FilterGroupExpression group, IReadOnlyDictionary<string, ColumnSchema> columns)
    {
        if (group.Expressions.Count == 0)
        {
            return true;
        }

        return group.Operator == LogicalOperator.And
            ? group.Expressions.All(expression => Matches(row, expression, columns))
            : group.Expressions.Any(expression => Matches(row, expression, columns));
    }

    private static bool EvaluateCondition(ITabularRow row, FilterConditionExpression condition, IReadOnlyDictionary<string, ColumnSchema> columns)
    {
        var column = columns[condition.ColumnName];
        row.TryGetValue(column.Name, out var rawValue);

        return condition.Operator switch
        {
            FilterOperator.Equals => DatasetValueComparer.Instance.Equals(
                rawValue,
                Infrastructure.AnalyticsComputation.ConvertToColumnType(condition.Value, column)),
            FilterOperator.NotEquals => !DatasetValueComparer.Instance.Equals(
                rawValue,
                Infrastructure.AnalyticsComputation.ConvertToColumnType(condition.Value, column)),
            FilterOperator.GreaterThan => DatasetValueComparer.Instance.Compare(
                rawValue,
                Infrastructure.AnalyticsComputation.ConvertToColumnType(condition.Value, column)) > 0,
            FilterOperator.GreaterThanOrEqual => DatasetValueComparer.Instance.Compare(
                rawValue,
                Infrastructure.AnalyticsComputation.ConvertToColumnType(condition.Value, column)) >= 0,
            FilterOperator.LessThan => DatasetValueComparer.Instance.Compare(
                rawValue,
                Infrastructure.AnalyticsComputation.ConvertToColumnType(condition.Value, column)) < 0,
            FilterOperator.LessThanOrEqual => DatasetValueComparer.Instance.Compare(
                rawValue,
                Infrastructure.AnalyticsComputation.ConvertToColumnType(condition.Value, column)) <= 0,
            FilterOperator.Contains => EvaluateContains(rawValue, condition.Value),
            FilterOperator.StartsWith => EvaluateStartsWith(rawValue, condition.Value),
            FilterOperator.EndsWith => EvaluateEndsWith(rawValue, condition.Value),
            FilterOperator.In => condition.Values
                .Select(value => Infrastructure.AnalyticsComputation.ConvertToColumnType(value, column))
                .Any(value => DatasetValueComparer.Instance.Equals(rawValue, value)),
            FilterOperator.NotIn => !condition.Values
                .Select(value => Infrastructure.AnalyticsComputation.ConvertToColumnType(value, column))
                .Any(value => DatasetValueComparer.Instance.Equals(rawValue, value)),
            FilterOperator.Between => EvaluateBetween(rawValue, condition, column),
            FilterOperator.IsNull => rawValue is null,
            FilterOperator.IsNotNull => rawValue is not null,
            _ => false
        };
    }

    private static bool EvaluateContains(object? rawValue, object? candidate)
    {
        if (rawValue is null || candidate is null)
        {
            return false;
        }

        return (rawValue.ToString() ?? string.Empty)
            .Contains(candidate.ToString() ?? string.Empty, StringComparison.OrdinalIgnoreCase);
    }

    private static bool EvaluateStartsWith(object? rawValue, object? candidate)
    {
        if (rawValue is null || candidate is null)
        {
            return false;
        }

        return (rawValue.ToString() ?? string.Empty)
            .StartsWith(candidate.ToString() ?? string.Empty, StringComparison.OrdinalIgnoreCase);
    }

    private static bool EvaluateEndsWith(object? rawValue, object? candidate)
    {
        if (rawValue is null || candidate is null)
        {
            return false;
        }

        return (rawValue.ToString() ?? string.Empty)
            .EndsWith(candidate.ToString() ?? string.Empty, StringComparison.OrdinalIgnoreCase);
    }

    private static bool EvaluateBetween(object? rawValue, FilterConditionExpression condition, ColumnSchema column)
    {
        var lower = Infrastructure.AnalyticsComputation.ConvertToColumnType(condition.Value, column);
        var upper = Infrastructure.AnalyticsComputation.ConvertToColumnType(condition.SecondValue, column);

        return DatasetValueComparer.Instance.Compare(rawValue, lower) >= 0
            && DatasetValueComparer.Instance.Compare(rawValue, upper) <= 0;
    }
}

internal static class TabularOrdering
{
    public static IEnumerable<T> ApplySorting<T>(
        IEnumerable<T> rows,
        IReadOnlyList<SortRule> sortRules,
        Func<T, string, object?> valueAccessor)
    {
        IOrderedEnumerable<T>? ordered = null;
        foreach (var sortRule in sortRules)
        {
            Func<T, object?> selector = row => valueAccessor(row, sortRule.ColumnName);
            ordered = ordered is null
                ? sortRule.Direction == SortDirection.Ascending
                    ? rows.OrderBy(selector, DatasetValueComparer.Instance)
                    : rows.OrderByDescending(selector, DatasetValueComparer.Instance)
                : sortRule.Direction == SortDirection.Ascending
                    ? ordered.ThenBy(selector, DatasetValueComparer.Instance)
                    : ordered.ThenByDescending(selector, DatasetValueComparer.Instance);
        }

        return ordered ?? rows;
    }
}

internal static class ColumnSearchMatcher
{
    public static IReadOnlyList<ColumnSearchMatch> Search(IReadOnlyCollection<ColumnSchema> columns, string keyword)
    {
        var normalizedKeyword = keyword.Trim();
        return columns
            .Select(column => BuildColumnSearchMatch(column, normalizedKeyword))
            .Where(match => match.Match is not null)
            .OrderByDescending(match => match.Score)
            .ThenBy(match => match.Match!.ColumnName, StringComparer.OrdinalIgnoreCase)
            .Select(match => match.Match!)
            .ToArray();
    }

    private static (ColumnSearchMatch? Match, int Score) BuildColumnSearchMatch(ColumnSchema column, string keyword)
    {
        var reasons = new List<string>();
        var score = 0;

        if (column.Name.Equals(keyword, StringComparison.OrdinalIgnoreCase))
        {
            reasons.Add("Exact column name match.");
            score += 300;
        }
        else if (column.Name.StartsWith(keyword, StringComparison.OrdinalIgnoreCase))
        {
            reasons.Add("Column name prefix match.");
            score += 200;
        }
        else if (column.Name.Contains(keyword, StringComparison.OrdinalIgnoreCase))
        {
            reasons.Add("Matched column name.");
            score += 150;
        }

        if (column.SampleValues.Any(sample => sample.Contains(keyword, StringComparison.OrdinalIgnoreCase)))
        {
            reasons.Add("Matched sample values.");
            score += 75;
        }

        if (column.CategoricalSummary?.TopValues.Any(value => value.Value.Contains(keyword, StringComparison.OrdinalIgnoreCase)) == true)
        {
            reasons.Add("Matched common categorical values.");
            score += 50;
        }

        return score == 0
            ? (null, 0)
            : (new ColumnSearchMatch
            {
                ColumnName = column.Name,
                MatchReason = string.Join(" ", reasons)
            }, score);
    }
}

internal sealed class DatasetValueComparer : IComparer<object?>, IEqualityComparer<object?>
{
    public static DatasetValueComparer Instance { get; } = new();

    public int Compare(object? x, object? y)
    {
        if (ReferenceEquals(x, y))
        {
            return 0;
        }

        if (x is null)
        {
            return -1;
        }

        if (y is null)
        {
            return 1;
        }

        var leftNumber = Infrastructure.AnalyticsComputation.TryConvertToDouble(x);
        var rightNumber = Infrastructure.AnalyticsComputation.TryConvertToDouble(y);
        if (leftNumber.HasValue && rightNumber.HasValue)
        {
            return leftNumber.Value.CompareTo(rightNumber.Value);
        }

        if (Infrastructure.AnalyticsComputation.TryParseDateTime(x, out var leftDate)
            && Infrastructure.AnalyticsComputation.TryParseDateTime(y, out var rightDate))
        {
            return leftDate.CompareTo(rightDate);
        }

        if (Infrastructure.AnalyticsComputation.TryParseBoolean(x, out var leftBool)
            && Infrastructure.AnalyticsComputation.TryParseBoolean(y, out var rightBool))
        {
            return leftBool.CompareTo(rightBool);
        }

        return StringComparer.OrdinalIgnoreCase.Compare(x.ToString(), y.ToString());
    }

    public new bool Equals(object? x, object? y) => Compare(x, y) == 0;

    public int GetHashCode(object? obj)
    {
        if (obj is null)
        {
            return 0;
        }

        var number = Infrastructure.AnalyticsComputation.TryConvertToDouble(obj);
        if (number.HasValue)
        {
            return number.Value.GetHashCode();
        }

        if (Infrastructure.AnalyticsComputation.TryParseDateTime(obj, out var date))
        {
            return date.GetHashCode();
        }

        if (Infrastructure.AnalyticsComputation.TryParseBoolean(obj, out var booleanValue))
        {
            return booleanValue.GetHashCode();
        }

        return StringComparer.OrdinalIgnoreCase.GetHashCode(obj.ToString() ?? string.Empty);
    }
}

internal sealed class GroupKey
{
    public GroupKey(IReadOnlyList<object?> values)
    {
        Values = values;
    }

    public IReadOnlyList<object?> Values { get; }
}

internal sealed class GroupKeyComparer : IEqualityComparer<GroupKey>
{
    public static GroupKeyComparer Instance { get; } = new();

    public bool Equals(GroupKey? x, GroupKey? y)
    {
        if (ReferenceEquals(x, y))
        {
            return true;
        }

        if (x is null || y is null || x.Values.Count != y.Values.Count)
        {
            return false;
        }

        for (var index = 0; index < x.Values.Count; index++)
        {
            if (!DatasetValueComparer.Instance.Equals(x.Values[index], y.Values[index]))
            {
                return false;
            }
        }

        return true;
    }

    public int GetHashCode(GroupKey obj)
    {
        var hash = new HashCode();
        foreach (var value in obj.Values)
        {
            hash.Add(DatasetValueComparer.Instance.GetHashCode(value));
        }

        return hash.ToHashCode();
    }
}
