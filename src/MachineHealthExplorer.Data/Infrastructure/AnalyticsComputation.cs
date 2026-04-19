using MachineHealthExplorer.Domain.Models;
using System.Globalization;

namespace MachineHealthExplorer.Data.Infrastructure;

internal static class AnalyticsComputation
{
    public static ColumnProfile BuildColumnProfile(ColumnSchema column, IEnumerable<object?> values, int rowCount, int top)
    {
        ArgumentNullException.ThrowIfNull(column);

        var materializedValues = values.ToArray();
        var nonNullValues = materializedValues
            .Where(value => value is not null)
            .ToArray();
        var distinctCount = nonNullValues
            .Distinct(Querying.DatasetValueComparer.Instance)
            .Count();
        var cardinalityHint = DetermineCardinalityHint(distinctCount, rowCount);
        var isCategorical = column.IsCategorical
            || column.DataType == DataTypeKind.Boolean
            || (!column.IsNumeric && IsCategoricalColumn(column.DataType, cardinalityHint));

        return new ColumnProfile
        {
            ColumnName = column.Name,
            DataType = column.DataType,
            IsNullable = materializedValues.Any(value => value is null),
            NonNullCount = nonNullValues.Length,
            CompletenessRatio = rowCount == 0 ? 0 : nonNullValues.Length / (double)rowCount,
            IsNumeric = column.IsNumeric,
            IsCategorical = isCategorical,
            RowCount = rowCount,
            NullCount = rowCount - nonNullValues.Length,
            DistinctCount = distinctCount,
            CardinalityHint = cardinalityHint,
            SampleValues = nonNullValues
                .Select(value => value?.ToString() ?? string.Empty)
                .Where(value => !string.IsNullOrWhiteSpace(value))
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .Take(5)
                .ToArray(),
            NumericSummary = column.IsNumeric ? BuildNumericSummary(column.Name, materializedValues) : null,
            CategoricalSummary = isCategorical ? BuildCategoricalSummary(column.Name, materializedValues, top) : null
        };
    }

    public static NumericSummary BuildNumericSummary(string columnName, IEnumerable<object?> values)
    {
        var numericValues = values
            .Select(TryConvertToDouble)
            .Where(value => value.HasValue)
            .Select(value => value!.Value)
            .OrderBy(value => value)
            .ToArray();

        if (numericValues.Length == 0)
        {
            return new NumericSummary
            {
                ColumnName = columnName,
                Count = 0
            };
        }

        var sum = numericValues.Sum();
        var average = sum / numericValues.Length;
        var variance = numericValues
            .Select(value => Math.Pow(value - average, 2))
            .Sum() / numericValues.Length;

        return new NumericSummary
        {
            ColumnName = columnName,
            Count = numericValues.Length,
            Sum = sum,
            Average = average,
            Min = numericValues.First(),
            Max = numericValues.Last(),
            Median = CalculateMedian(numericValues),
            StandardDeviation = Math.Sqrt(variance)
        };
    }

    public static CategoricalSummary BuildCategoricalSummary(string columnName, IEnumerable<object?> values, int top)
    {
        var normalizedValues = values
            .Where(value => value is not null)
            .Select(value => value!.ToString() ?? string.Empty)
            .ToArray();

        var total = normalizedValues.Length;
        var groups = normalizedValues
            .GroupBy(value => value, StringComparer.OrdinalIgnoreCase)
            .Select(group => new ValueFrequency
            {
                Value = group.First(),
                Count = group.Count(),
                Percentage = total == 0 ? 0 : group.Count() / (double)total
            })
            .OrderByDescending(item => item.Count)
            .ThenBy(item => item.Value, StringComparer.OrdinalIgnoreCase)
            .Take(Math.Max(1, top))
            .ToArray();

        return new CategoricalSummary
        {
            ColumnName = columnName,
            Count = total,
            DistinctCount = normalizedValues.Distinct(StringComparer.OrdinalIgnoreCase).Count(),
            TopValues = groups
        };
    }

    public static CardinalityHint DetermineCardinalityHint(int distinctCount, int rowCount)
    {
        if (rowCount <= 0 || distinctCount <= 0)
        {
            return CardinalityHint.Unknown;
        }

        var ratio = distinctCount / (double)rowCount;
        if (distinctCount <= Math.Min(12, Math.Max(3, rowCount / 20)) || ratio <= 0.02)
        {
            return CardinalityHint.Low;
        }

        if (distinctCount <= Math.Min(100, Math.Max(10, rowCount / 4)) || ratio <= 0.20)
        {
            return CardinalityHint.Medium;
        }

        return CardinalityHint.High;
    }

    public static bool IsCategoricalColumn(DataTypeKind dataType, CardinalityHint cardinalityHint)
    {
        if (dataType == DataTypeKind.Boolean)
        {
            return true;
        }

        if (dataType is DataTypeKind.Integer or DataTypeKind.Decimal)
        {
            return false;
        }

        return cardinalityHint is CardinalityHint.Low or CardinalityHint.Medium;
    }

    public static bool IsFailureLikeColumn(ColumnSchema column)
    {
        if (column.Name.Contains("fail", StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        return column.Name is "TWF" or "HDF" or "PWF" or "OSF" or "RNF";
    }

    public static bool IsTruthy(object? value)
    {
        return value switch
        {
            bool booleanValue => booleanValue,
            string text => bool.TryParse(text, out var parsedBool)
                ? parsedBool
                : long.TryParse(text, out var parsedLong) && parsedLong != 0,
            sbyte signedByte => signedByte != 0,
            byte unsignedByte => unsignedByte != 0,
            short shortValue => shortValue != 0,
            ushort unsignedShort => unsignedShort != 0,
            int intValue => intValue != 0,
            uint unsignedInt => unsignedInt != 0,
            long longValue => longValue != 0,
            ulong unsignedLong => unsignedLong != 0,
            float singleValue => Math.Abs(singleValue) > double.Epsilon,
            double doubleValue => Math.Abs(doubleValue) > double.Epsilon,
            decimal decimalValue => decimalValue != 0,
            _ => false
        };
    }

    public static object? ConvertToColumnType(object? candidate, ColumnSchema column)
    {
        if (candidate is null)
        {
            return null;
        }

        if (candidate is string text && string.IsNullOrWhiteSpace(text))
        {
            return null;
        }

        return column.DataType switch
        {
            DataTypeKind.Boolean => TryParseBoolean(candidate, out var booleanValue) ? booleanValue : null,
            DataTypeKind.Integer => TryParseInteger(candidate, out var integerValue) ? integerValue : null,
            DataTypeKind.Decimal => TryParseDouble(candidate, out var doubleValue) ? doubleValue : null,
            DataTypeKind.DateTime => TryParseDateTime(candidate, out var dateValue) ? dateValue : null,
            _ => candidate.ToString()
        };
    }

    public static double? TryConvertToDouble(object? value)
    {
        if (value is null)
        {
            return null;
        }

        return value switch
        {
            byte byteValue => byteValue,
            sbyte signedByteValue => signedByteValue,
            short shortValue => shortValue,
            ushort unsignedShortValue => unsignedShortValue,
            int intValue => intValue,
            uint unsignedIntValue => unsignedIntValue,
            long longValue => longValue,
            ulong unsignedLongValue => unsignedLongValue,
            float floatValue => floatValue,
            double doubleValue => doubleValue,
            decimal decimalValue => (double)decimalValue,
            string text when TryParseDouble(text, out var parsed) => parsed,
            bool booleanValue => booleanValue ? 1d : 0d,
            _ => null
        };
    }

    public static bool TryParseBoolean(object candidate, out bool value)
    {
        switch (candidate)
        {
            case bool booleanValue:
                value = booleanValue;
                return true;
            case string text:
                text = text.Trim();
                if (bool.TryParse(text, out value))
                {
                    return true;
                }

                if (text == "1")
                {
                    value = true;
                    return true;
                }

                if (text == "0")
                {
                    value = false;
                    return true;
                }

                break;
            default:
                var numeric = TryConvertToDouble(candidate);
                if (numeric.HasValue && (numeric.Value == 0d || numeric.Value == 1d))
                {
                    value = numeric.Value == 1d;
                    return true;
                }

                break;
        }

        value = false;
        return false;
    }

    public static bool TryParseInteger(object candidate, out long value)
    {
        switch (candidate)
        {
            case long longValue:
                value = longValue;
                return true;
            case int intValue:
                value = intValue;
                return true;
            case string text when long.TryParse(text.Trim(), out value):
                return true;
            default:
                value = default;
                return false;
        }
    }

    public static bool TryParseDouble(object candidate, out double value)
    {
        switch (candidate)
        {
            case double doubleValue:
                value = doubleValue;
                return true;
            case float floatValue:
                value = floatValue;
                return true;
            case decimal decimalValue:
                value = (double)decimalValue;
                return true;
            case long longValue:
                value = longValue;
                return true;
            case int intValue:
                value = intValue;
                return true;
            case string text when double.TryParse(text.Trim(), NumberStyles.Float, CultureInfo.InvariantCulture, out value):
                return true;
            default:
                value = default;
                return false;
        }
    }

    public static bool TryParseDateTime(object candidate, out DateTime value)
    {
        switch (candidate)
        {
            case DateTime dateTime:
                value = dateTime;
                return true;
            case string text when DateTime.TryParse(
                text.Trim(),
                CultureInfo.InvariantCulture,
                DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal,
                out value):
                return true;
            default:
                value = default;
                return false;
        }
    }

    private static double CalculateMedian(IReadOnlyList<double> values)
    {
        var middle = values.Count / 2;
        return values.Count % 2 == 0
            ? (values[middle - 1] + values[middle]) / 2d
            : values[middle];
    }
}
