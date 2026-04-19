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
            Description = "Returns a concise overview of numeric, categorical, and failure-oriented columns.",
            InputHints = ["No arguments"],
            ExamplePrompts = ["Summarize this predictive maintenance dataset."]
        },
        new()
        {
            Name = "profile_columns",
            Description = "Profiles one or more columns with scoped completeness, distinct counts, and summaries.",
            InputHints = ["ColumnProfilingRequest"],
            ExamplePrompts = ["Profile Type and Torque [Nm] for failed machines only."]
        },
        new()
        {
            Name = "query_rows",
            Description = "Executes a filtered, sorted, paged row query.",
            InputHints = ["QueryRequest"],
            ExamplePrompts = ["Show failed Type L or M machines with high torque and heavy tool wear."]
        },
        new()
        {
            Name = "find_column_extrema",
            Description = "Finds rows where a numeric column reaches its global max or min (ties), including UDI when present.",
            InputHints = ["ColumnExtremaRequest"],
            ExamplePrompts = ["Which machine row has the highest process temperature?", "At what UDI does air temperature peak?"]
        },
        new()
        {
            Name = "get_distinct_values",
            Description = "Returns distinct values and frequencies for a column.",
            InputHints = ["DistinctValuesRequest"],
            ExamplePrompts = ["What machine types exist in the dataset?"]
        },
        new()
        {
            Name = "group_and_aggregate",
            Description = "Groups rows and computes reusable aggregate metrics with optional having filters.",
            InputHints = ["GroupAggregationRequest"],
            ExamplePrompts = ["Group the dataset by Type and compare failure counts with average torque."]
        },
        new()
        {
            Name = "compare_subsets",
            Description = "Compares two filtered cohorts using numeric and categorical summaries.",
            InputHints = ["SubsetComparisonRequest"],
            ExamplePrompts = ["Compare high-wear failed machines against low-wear healthy machines."]
        },
        new()
        {
            Name = "search_columns",
            Description = "Searches the inferred schema for columns relevant to a keyword.",
            InputHints = ["keyword"],
            ExamplePrompts = ["Find columns related to torque or wear."]
        },
        new()
        {
            Name = "build_report",
            Description = "Builds a structured report from generic analytical requests.",
            InputHints = ["ReportRequest"],
            ExamplePrompts = ["Build a report for failures grouped by Type."]
        },
        new()
        {
            Name = "get_failure_analysis",
            Description = "Returns failure counts, rate, common failure modes, and top metric deltas.",
            InputHints = ["No arguments"],
            ExamplePrompts = ["Summarize failure patterns in the AI4I dataset."]
        },
        new()
        {
            Name = "compare_failure_cohorts",
            Description = "Compares failed rows against healthy rows using reusable analytics services.",
            InputHints = ["No arguments"],
            ExamplePrompts = ["Compare failed and healthy machines."]
        },
        new()
        {
            Name = "get_operating_condition_summary",
            Description = "Summarizes operating metrics and categorical context for an optional filter.",
            InputHints = ["Optional FilterExpression"],
            ExamplePrompts = ["Summarize operating conditions for failed Type L machines."]
        },
        new()
        {
            Name = "build_executive_report",
            Description = "Builds a concise executive report tailored to machine-health analysis.",
            InputHints = ["No arguments"],
            ExamplePrompts = ["Create an executive summary for this predictive maintenance dataset."]
        },
        new()
        {
            Name = "get_analysis_examples",
            Description = "Returns reusable multi-filter, grouping, and comparison examples.",
            InputHints = ["No arguments"],
            ExamplePrompts = ["Show me example analytics requests I can run."]
        }
    ];

    public IReadOnlyList<DatasetToolDescriptor> GetTools() => Tools;
}
