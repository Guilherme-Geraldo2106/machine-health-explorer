using System.Runtime.CompilerServices;
using MachineHealthExplorer.Domain.Models;

namespace MachineHealthExplorer.Data.Querying;

internal static class GroupByAutoBinAssignments
{
    public const int DefaultBinCount = 10;
    public const int MinBinCount = 2;
    public const int MaxBinCount = 100;

    public static int ResolveBinCount(int? requested)
    {
        var n = requested ?? DefaultBinCount;
        return n;
    }

    public static void ValidateBinCount(int binCount)
    {
        if (binCount < MinBinCount || binCount > MaxBinCount)
        {
            throw new ArgumentException(
                $"binCount must be between {MinBinCount} and {MaxBinCount} (inclusive). Received: {binCount}.");
        }
    }

    /// <summary>
    /// Builds a row -&gt; numeric bin lower bound map. Rows missing numeric values are absent from the map (caller skips them).
    /// </summary>
    public static Dictionary<DatasetRow, double> BuildEqualWidthRowToLowerBound(
        IReadOnlyList<DatasetRow> rows,
        string columnName,
        int binCount)
    {
        ValidateBinCount(binCount);
        var measurements = new List<(DatasetRow Row, double Value)>();
        foreach (var row in rows)
        {
            if (!TryGetNumericMeasurement(
                    row.TryGetValue(columnName, out var v) ? v : null,
                    out var m))
            {
                continue;
            }

            measurements.Add((row, m));
        }

        if (measurements.Count == 0)
        {
            throw new ArgumentException(
                $"groupByAutoBins EqualWidth on '{columnName}' found no numeric values in the filtered scope.");
        }

        var min = measurements.Min(t => t.Value);
        var max = measurements.Max(t => t.Value);
        var map = new Dictionary<DatasetRow, double>(ReferenceEqualityComparer.Instance);
        if (min == max)
        {
            foreach (var (row, _) in measurements)
            {
                map[row] = min;
            }

            return map;
        }

        var width = (max - min) / binCount;
        if (width <= 0 || double.IsNaN(width) || double.IsInfinity(width))
        {
            throw new ArgumentException($"Could not compute EqualWidth bins for '{columnName}'.");
        }

        foreach (var (row, value) in measurements)
        {
            var idx = (int)Math.Floor((value - min) / width);
            if (idx < 0)
            {
                idx = 0;
            }

            if (idx >= binCount)
            {
                idx = binCount - 1;
            }

            var lower = min + idx * width;
            map[row] = lower;
        }

        return map;
    }

    public static Dictionary<DatasetRow, double> BuildQuantileRowToLowerBound(
        IReadOnlyList<DatasetRow> rows,
        string columnName,
        int binCount)
    {
        ValidateBinCount(binCount);
        var measurements = new List<(DatasetRow Row, double Value)>();
        foreach (var row in rows)
        {
            if (!TryGetNumericMeasurement(
                    row.TryGetValue(columnName, out var v) ? v : null,
                    out var m))
            {
                continue;
            }

            measurements.Add((row, m));
        }

        if (measurements.Count == 0)
        {
            throw new ArgumentException(
                $"groupByAutoBins Quantile on '{columnName}' found no numeric values in the filtered scope.");
        }

        measurements.Sort((a, b) => a.Value.CompareTo(b.Value));
        var n = measurements.Count;
        var lowers = new double[binCount];
        for (var b = 0; b < binCount; b++)
        {
            var start = (int)((long)b * n / binCount);
            if (start < 0)
            {
                start = 0;
            }

            if (start >= n)
            {
                start = n - 1;
            }

            lowers[b] = measurements[start].Value;
        }

        var map = new Dictionary<DatasetRow, double>(ReferenceEqualityComparer.Instance);
        for (var i = 0; i < n; i++)
        {
            var binIdx = (int)Math.Min(binCount - 1, (long)i * binCount / n);
            var lower = lowers[binIdx];
            map[measurements[i].Row] = lower;
        }

        return map;
    }

    private sealed class ReferenceEqualityComparer : IEqualityComparer<DatasetRow>
    {
        public static ReferenceEqualityComparer Instance { get; } = new();

        public bool Equals(DatasetRow? x, DatasetRow? y) => ReferenceEquals(x, y);

        public int GetHashCode(DatasetRow obj) => RuntimeHelpers.GetHashCode(obj);
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
}
