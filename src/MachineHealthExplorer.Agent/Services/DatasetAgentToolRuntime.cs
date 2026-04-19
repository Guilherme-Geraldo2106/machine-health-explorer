using MachineHealthExplorer.Agent.Abstractions;
using MachineHealthExplorer.Agent.Models;
using MachineHealthExplorer.Agent.Serialization;
using MachineHealthExplorer.Domain.Models;
using MachineHealthExplorer.Tools.Abstractions;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace MachineHealthExplorer.Agent.Services;

public sealed class DatasetAgentToolRuntime : IAgentToolRuntime
{
    private const string EmptyObjectSchema = """{"type":"object","properties":{},"additionalProperties":false}""";

    private readonly IDatasetToolCatalog _toolCatalog;
    private readonly IDatasetToolService _toolService;
    private readonly IReadOnlyDictionary<string, string> _schemas;

    public DatasetAgentToolRuntime(IDatasetToolCatalog toolCatalog, IDatasetToolService toolService)
    {
        _toolCatalog = toolCatalog ?? throw new ArgumentNullException(nameof(toolCatalog));
        _toolService = toolService ?? throw new ArgumentNullException(nameof(toolService));
        _schemas = CreateSchemas();
    }

    public IReadOnlyList<AgentToolDefinition> GetTools()
        => _toolCatalog.GetTools()
            .Select(tool => new AgentToolDefinition
            {
                Name = tool.Name,
                Description = tool.Description,
                ParametersJsonSchema = _schemas.TryGetValue(tool.Name, out var schema) ? schema : EmptyObjectSchema
            })
            .ToArray();

    public async Task<AgentToolExecutionRecord> ExecuteAsync(string toolName, string argumentsJson, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(toolName);
        toolName = AgentToolInvocationCanonicalizer.ApplyKnownAliases(toolName.Trim());

        try
        {
            var normalizedArguments = NormalizeArguments(argumentsJson);
            var result = await ExecuteCoreAsync(toolName, normalizedArguments, cancellationToken).ConfigureAwait(false);
            return new AgentToolExecutionRecord
            {
                ToolName = toolName,
                ArgumentsJson = normalizedArguments,
                ResultJson = AgentJsonSerializer.Serialize(result)
            };
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            return new AgentToolExecutionRecord
            {
                ToolName = toolName,
                ArgumentsJson = NormalizeArguments(argumentsJson),
                ResultJson = AgentJsonSerializer.Serialize(new { error = exception.Message }),
                IsError = true
            };
        }
    }

    private async Task<object?> ExecuteCoreAsync(string toolName, string argumentsJson, CancellationToken cancellationToken)
        => toolName switch
        {
            "get_schema" => await _toolService.GetSchemaAsync(cancellationToken).ConfigureAwait(false),
            "describe_dataset" => await _toolService.DescribeDatasetAsync(cancellationToken).ConfigureAwait(false),
            "profile_columns" => await _toolService.ProfileColumnsAsync(
                DeserializeArguments<ColumnProfilingRequest>(argumentsJson),
                cancellationToken).ConfigureAwait(false),
            "query_rows" => await _toolService.QueryRowsAsync(
                DeserializeArguments<QueryRequest>(argumentsJson),
                cancellationToken).ConfigureAwait(false),
            "find_column_extrema" => await _toolService.FindColumnExtremaRowsAsync(
                DeserializeArguments<ColumnExtremaRequest>(argumentsJson),
                cancellationToken).ConfigureAwait(false),
            "get_distinct_values" => await _toolService.GetDistinctValuesAsync(
                DeserializeArguments<DistinctValuesRequest>(argumentsJson),
                cancellationToken).ConfigureAwait(false),
            "group_and_aggregate" => await _toolService.GroupAndAggregateAsync(
                DeserializeArguments<GroupAggregationRequest>(argumentsJson),
                cancellationToken).ConfigureAwait(false),
            "compare_subsets" => await _toolService.CompareSubsetsAsync(
                DeserializeArguments<SubsetComparisonRequest>(argumentsJson),
                cancellationToken).ConfigureAwait(false),
            "search_columns" => await _toolService.SearchColumnsAsync(
                DeserializeArguments<SearchColumnsArguments>(argumentsJson).Keyword,
                cancellationToken).ConfigureAwait(false),
            "build_report" => await _toolService.BuildReportAsync(
                DeserializeArguments<ReportRequest>(argumentsJson),
                cancellationToken).ConfigureAwait(false),
            "get_failure_analysis" => await _toolService.GetFailureAnalysisAsync(cancellationToken).ConfigureAwait(false),
            "compare_failure_cohorts" => await _toolService.CompareFailureCohortsAsync(cancellationToken).ConfigureAwait(false),
            "get_operating_condition_summary" => await _toolService.GetOperatingConditionSummaryAsync(
                DeserializeArguments<OperatingConditionSummaryArguments>(argumentsJson).Filter,
                cancellationToken).ConfigureAwait(false),
            "build_executive_report" => await _toolService.BuildExecutiveReportAsync(cancellationToken).ConfigureAwait(false),
            "get_analysis_examples" => await _toolService.GetAnalysisExamplesAsync(cancellationToken).ConfigureAwait(false),
            _ => throw new InvalidOperationException($"Unknown tool '{toolName}'.")
        };

    private static T DeserializeArguments<T>(string argumentsJson)
        => AgentJsonSerializer.Deserialize<T>(NormalizeArguments(argumentsJson));

    private static string NormalizeArguments(string? argumentsJson)
        => string.IsNullOrWhiteSpace(argumentsJson)
            ? "{}"
            : argumentsJson.Trim();

    private static IReadOnlyDictionary<string, string> CreateSchemas()
    {
        static string SerializeSchema(object schema)
            => JsonSerializer.Serialize(
                schema,
                new JsonSerializerOptions(JsonSerializerDefaults.Web)
                {
                    DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
                });

        var stringArraySchema = new
        {
            type = "array",
            items = new
            {
                type = "string"
            }
        };

        var filterSchema = new
        {
            type = "object",
            description = "Optional filter expression. Use either a condition object with columnName/operator/value or a group object with operator/expressions.",
            additionalProperties = true
        };

        var sortRuleSchema = new
        {
            type = "object",
            properties = new
            {
                columnName = new
                {
                    type = "string",
                    description = "Dataset column or aggregation alias to sort by."
                },
                direction = new
                {
                    type = "string",
                    @enum = Enum.GetNames<SortDirection>()
                }
            },
            required = new[] { "columnName" },
            additionalProperties = false
        };

        var aggregationSchema = new
        {
            type = "object",
            properties = new
            {
                alias = new
                {
                    type = "string",
                    description = "Unique output name for the aggregation."
                },
                columnName = new
                {
                    type = "string",
                    description = "Dataset column to aggregate. Leave empty for count."
                },
                function = new
                {
                    type = "string",
                    @enum = Enum.GetNames<AggregateFunction>()
                },
                filter = filterSchema
            },
            required = new[] { "alias", "function" },
            additionalProperties = false
        };

        return new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["get_schema"] = EmptyObjectSchema,
            ["describe_dataset"] = EmptyObjectSchema,
            ["profile_columns"] = SerializeSchema(new
            {
                type = "object",
                properties = new
                {
                    columns = stringArraySchema,
                    filter = filterSchema,
                    topCategoryCount = new
                    {
                        type = "integer",
                        minimum = 1,
                        maximum = 50
                    }
                },
                required = new[] { "columns" },
                additionalProperties = false
            }),
            ["query_rows"] = SerializeSchema(new
            {
                type = "object",
                properties = new
                {
                    filter = filterSchema,
                    selectedColumns = stringArraySchema,
                    sortRules = new
                    {
                        type = "array",
                        items = sortRuleSchema
                    },
                    page = new
                    {
                        type = "integer",
                        minimum = 1
                    },
                    pageSize = new
                    {
                        type = "integer",
                        minimum = 1,
                        maximum = 200
                    }
                },
                additionalProperties = false
            }),
            ["find_column_extrema"] = SerializeSchema(new
            {
                type = "object",
                properties = new
                {
                    columnName = new
                    {
                        type = "string",
                        description = "Numeric dataset column (exact name, e.g. Process temperature [K])."
                    },
                    mode = new
                    {
                        type = "string",
                        description = "Whether to find the global maximum or minimum.",
                        @enum = new[] { "max", "min" }
                    },
                    filter = filterSchema,
                    extraSelectedColumns = stringArraySchema,
                    maxTieRows = new
                    {
                        type = "integer",
                        minimum = 1,
                        maximum = 50
                    }
                },
                required = new[] { "columnName" },
                additionalProperties = false
            }),
            ["get_distinct_values"] = SerializeSchema(new
            {
                type = "object",
                properties = new
                {
                    columnName = new
                    {
                        type = "string"
                    },
                    limit = new
                    {
                        type = "integer",
                        minimum = 1,
                        maximum = 200
                    },
                    filter = filterSchema
                },
                required = new[] { "columnName" },
                additionalProperties = false
            }),
            ["group_and_aggregate"] = SerializeSchema(new
            {
                type = "object",
                properties = new
                {
                    groupByColumns = stringArraySchema,
                    aggregations = new
                    {
                        type = "array",
                        items = aggregationSchema
                    },
                    filter = filterSchema,
                    having = filterSchema,
                    sortRules = new
                    {
                        type = "array",
                        items = sortRuleSchema
                    },
                    page = new
                    {
                        type = "integer",
                        minimum = 1
                    },
                    pageSize = new
                    {
                        type = "integer",
                        minimum = 1,
                        maximum = 200
                    }
                },
                required = new[] { "aggregations" },
                additionalProperties = false
            }),
            ["compare_subsets"] = SerializeSchema(new
            {
                type = "object",
                properties = new
                {
                    leftLabel = new { type = "string" },
                    leftFilter = filterSchema,
                    rightLabel = new { type = "string" },
                    rightFilter = filterSchema,
                    numericColumns = stringArraySchema,
                    categoricalColumns = stringArraySchema,
                    topCategoryCount = new
                    {
                        type = "integer",
                        minimum = 1,
                        maximum = 20
                    }
                },
                additionalProperties = false
            }),
            ["search_columns"] = SerializeSchema(new
            {
                type = "object",
                properties = new
                {
                    keyword = new
                    {
                        type = "string",
                        description = "Keyword to match against schema names and sample values."
                    }
                },
                required = new[] { "keyword" },
                additionalProperties = false
            }),
            ["build_report"] = SerializeSchema(new
            {
                type = "object",
                properties = new
                {
                    title = new { type = "string" },
                    baseFilter = filterSchema,
                    focusColumns = stringArraySchema,
                    groupByColumns = stringArraySchema,
                    aggregations = new
                    {
                        type = "array",
                        items = aggregationSchema
                    },
                    topCategoryCount = new
                    {
                        type = "integer",
                        minimum = 1,
                        maximum = 20
                    }
                },
                additionalProperties = false
            }),
            ["get_failure_analysis"] = EmptyObjectSchema,
            ["compare_failure_cohorts"] = EmptyObjectSchema,
            ["get_operating_condition_summary"] = SerializeSchema(new
            {
                type = "object",
                properties = new
                {
                    filter = filterSchema
                },
                additionalProperties = false
            }),
            ["build_executive_report"] = EmptyObjectSchema,
            ["get_analysis_examples"] = EmptyObjectSchema
        };
    }

    private sealed record SearchColumnsArguments
    {
        public string Keyword { get; init; } = string.Empty;
    }

    private sealed record OperatingConditionSummaryArguments
    {
        public FilterExpression? Filter { get; init; }
    }
}
