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
                "Groups rows by explicit columns and/or numeric histogram bins (bucket lower = floor(value/binWidth)*binWidth), then computes aggregations (count, sum, etc.) with optional per-aggregation filters.",
            InputHints = ["GroupAggregationRequest"],
            ExamplePrompts =
            [
                "Count rows and conditional counts per 1-unit bins of a numeric column.",
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
