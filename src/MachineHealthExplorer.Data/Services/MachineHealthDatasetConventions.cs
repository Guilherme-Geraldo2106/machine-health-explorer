using MachineHealthExplorer.Domain.Models;

namespace MachineHealthExplorer.Data.Services;

internal static class MachineHealthDatasetConventions
{
    private static readonly string[] KnownFailureModes = ["TWF", "HDF", "PWF", "OSF", "RNF"];

    public static ColumnSchema? ResolveFailureIndicator(DatasetSchema schema)
    {
        return schema.Columns.FirstOrDefault(column => column.Name.Equals("Machine failure", StringComparison.OrdinalIgnoreCase))
            ?? schema.Columns.FirstOrDefault(column =>
                column.CategoricalSummary is not null
                && IsFailureColumn(column)
                && !KnownFailureModes.Contains(column.Name, StringComparer.OrdinalIgnoreCase))
            ?? schema.Columns.FirstOrDefault(IsFailureColumn);
    }

    public static ColumnSchema[] ResolveFailureModeColumns(DatasetSchema schema, ColumnSchema failureIndicator)
    {
        return schema.Columns
            .Where(column =>
                !column.Name.Equals(failureIndicator.Name, StringComparison.OrdinalIgnoreCase)
                && (KnownFailureModes.Contains(column.Name, StringComparer.OrdinalIgnoreCase)
                    || (IsFailureColumn(column) && column.CategoricalSummary is not null)))
            .ToArray();
    }

    public static string[] GetOperationalNumericColumns(DatasetSchema schema)
    {
        return schema.Columns
            .Where(column =>
                column.IsNumeric
                && !column.Name.Contains("id", StringComparison.OrdinalIgnoreCase)
                && !IsFailureColumn(column))
            .Select(column => column.Name)
            .ToArray();
    }

    public static string[] GetComparisonCategoricalColumns(DatasetSchema schema)
    {
        return schema.Columns
            .Where(column =>
                column.CategoricalSummary is not null
                && !column.Name.Equals("Product ID", StringComparison.OrdinalIgnoreCase)
                && !column.Name.Equals("UDI", StringComparison.OrdinalIgnoreCase))
            .Select(column => column.Name)
            .ToArray();
    }

    public static string[] FilterAvailableColumns(DatasetSchema schema, params string?[] candidates)
    {
        var availableColumns = schema.Columns
            .Select(column => column.Name)
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        return candidates
            .Where(candidate => !string.IsNullOrWhiteSpace(candidate))
            .Select(candidate => candidate!)
            .Where(availableColumns.Contains)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray();
    }

    public static string? ResolvePreferredColumn(DatasetSchema schema, params string[] candidates)
        => FilterAvailableColumns(schema, candidates.Cast<string?>().ToArray()).FirstOrDefault();

    public static bool IsFailureColumn(ColumnSchema column)
        => column.Name.Contains("fail", StringComparison.OrdinalIgnoreCase)
            || KnownFailureModes.Contains(column.Name, StringComparer.OrdinalIgnoreCase);

    public static string SanitizeAlias(string value)
        => value
            .ToLowerInvariant()
            .Replace(" ", "_", StringComparison.Ordinal)
            .Replace("[", string.Empty, StringComparison.Ordinal)
            .Replace("]", string.Empty, StringComparison.Ordinal)
            .Replace("/", "_", StringComparison.Ordinal);
}
