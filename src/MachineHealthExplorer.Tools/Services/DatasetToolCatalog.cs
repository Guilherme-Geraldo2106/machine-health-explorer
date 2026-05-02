using MachineHealthExplorer.Tools.Abstractions;

namespace MachineHealthExplorer.Tools.Services;

public sealed class DatasetToolCatalog : IDatasetToolCatalog
{
    private static readonly DatasetToolDescriptor[] Tools =
    [
        new()
        {
            Name = "get_schema",
            Description = "Returns the inferred schema for the loaded dataset.",
            InputHints = ["No arguments"],
            ExamplePrompts = ["What columns are available in the dataset?"]
        },
        new()
        {
            Name = "describe_dataset",
            Description = "Returns a neutral overview: row/column counts, numeric columns, categorical columns, and boolean-like columns (name heuristic only).",
            InputHints = ["No arguments"],
            ExamplePrompts = ["Summarize the loaded tabular dataset at a high level."]
        },
        new()
        {
            Name = "profile_columns",
            Description = "Profiles one or more columns with scoped completeness, distinct counts, and summaries.",
            InputHints = ["ColumnProfilingRequest"],
            ExamplePrompts = ["Profile Type and Torque [Nm] for rows matching a filter."]
        },
        new()
        {
            Name = "query_rows",
            Description = "Executes a filtered, sorted, paged row query (tabular data only).",
            InputHints = ["QueryRequest"],
            ExamplePrompts = ["List rows matching a numeric range and sort by a column."]
        },
        new()
        {
            Name = "get_distinct_values",
            Description = "Returns distinct values and frequencies for a column.",
            InputHints = ["DistinctValuesRequest"],
            ExamplePrompts = ["What distinct categories exist for a column?"]
        },
        new()
        {
            Name = "group_and_aggregate",
            Description =
                "Groups rows by explicit columns and/or numeric histogram bins (bucket lower = floor(value/binWidth)*binWidth), optional automatic equal-width or quantile bins, then computes aggregations (count, sum, etc.) and optional derivedMetrics (restricted arithmetic on aggregation outputs and numeric grouping keys). " +
                "Semantics: Count with no per-aggregation filter counts every row in the group (total group size). " +
                "To count only a subset (events, flags, labels), use Count on the same grouping with a per-aggregation filter; pick the filter column from the schema. " +
                "Common pattern: alias row_count = Count without filter; alias event_count = Count with a filter on a boolean/categorical column the model selects from the schema; use derivedMetrics for ratios such as event_count/row_count when both exist. " +
                "groupByBins and groupByAutoBins only build neutral numeric ranges or quantile keys; they do not interpret which range is important.",
            InputHints = ["GroupAggregationRequest"],
            ExamplePrompts =
            [
                "Per numeric-bin band: total rows (Count, no filter) and conditional Count with a filter on the same grouping.",
                "Group by two categorical columns and compute averages."
            ]
        },
        new()
        {
            Name = "search_columns",
            Description = "Searches the inferred schema for columns relevant to a keyword.",
            InputHints = ["keyword"],
            ExamplePrompts = ["Find columns related to torque or temperature."]
        }
    ];

    public IReadOnlyList<DatasetToolDescriptor> GetTools() => Tools;
}
