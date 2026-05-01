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
            "get_schema" => BuildLeanSchemaPayload(await _toolService.GetSchemaAsync(cancellationToken).ConfigureAwait(false)),
            "describe_dataset" => await _toolService.DescribeDatasetAsync(cancellationToken).ConfigureAwait(false),
            "profile_columns" => await _toolService.ProfileColumnsAsync(
                DeserializeToolArguments<ColumnProfilingRequest>(argumentsJson, "profile_columns"),
                cancellationToken).ConfigureAwait(false),
            "query_rows" => await _toolService.QueryRowsAsync(
                DeserializeToolArguments<QueryRequest>(argumentsJson, "query_rows"),
                cancellationToken).ConfigureAwait(false),
            "get_distinct_values" => await _toolService.GetDistinctValuesAsync(
                DeserializeToolArguments<DistinctValuesRequest>(argumentsJson, "get_distinct_values"),
                cancellationToken).ConfigureAwait(false),
            "group_and_aggregate" => await ExecuteGroupAndAggregateAsync(argumentsJson, cancellationToken).ConfigureAwait(false),
            "search_columns" => await _toolService.SearchColumnsAsync(
                DeserializeToolArguments<SearchColumnsArguments>(argumentsJson, "search_columns").Keyword,
                cancellationToken).ConfigureAwait(false),
            _ => throw new InvalidOperationException($"Unknown tool '{toolName}'.")
        };

    private async Task<GroupAggregationResult> ExecuteGroupAndAggregateAsync(string argumentsJson, CancellationToken cancellationToken)
    {
        var request = DeserializeToolArguments<GroupAggregationRequest>(argumentsJson, "group_and_aggregate");
        if (request.Aggregations.Count == 0)
        {
            throw new InvalidOperationException(
                "group_and_aggregate requires a non-empty 'aggregations' array (each item needs 'alias' and 'function').");
        }

        return await _toolService.GroupAndAggregateAsync(request, cancellationToken).ConfigureAwait(false);
    }

    private static T DeserializeToolArguments<T>(string argumentsJson, string toolName)
    {
        if (!AgentToolJsonDeserializer.TryDeserialize<T>(NormalizeArguments(argumentsJson), out var parsed, out var error)
            || parsed is null)
        {
            throw new InvalidOperationException($"{toolName}: invalid tool arguments JSON ({error})");
        }

        return parsed;
    }

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

        var aggregationFilterSchema = new
        {
            type = "object",
            description =
                "Optional filter applied only while computing this aggregation (other aggregations in the same request are unaffected). " +
                "Use this to combine an unfiltered row Count (group size) with a filtered Count (subset/event/label count) in one grouping. " +
                "Omit this field for Count to include every row in the group.",
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
                    description =
                        "Source column for non-Count functions. For Count, omit; counting uses group rows, narrowed only by this aggregation's optional filter (not by a column value)."
                },
                function = new
                {
                    type = "string",
                    @enum = Enum.GetNames<AggregateFunction>()
                },
                filter = aggregationFilterSchema
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
                description =
                    "Tabular group-by with aggregations. Count without a per-aggregation filter counts all rows in each group; add a per-aggregation filter on Count to count only rows matching that predicate (e.g. row_count vs event_count in one call). " +
                    "groupByBins only assigns numeric bucket labels; it does not mark any bucket as critical.",
                properties = new
                {
                    groupByColumns = stringArraySchema,
                    groupByBins = new
                    {
                        type = "array",
                        description =
                            "Optional numeric histogram dimensions. Bucket lower bound = floor(value/binWidth)*binWidth; upper bound (exclusive) is lower+binWidth (not returned as a column). " +
                            "Bins are neutral numeric ranges only; this tool does not interpret business importance of any range.",
                        items = new
                        {
                            type = "object",
                            properties = new
                            {
                                columnName = new
                                {
                                    type = "string",
                                    description = "Exact numeric column name from the dataset schema."
                                },
                                alias = new
                                {
                                    type = "string",
                                    description = "Output column name for the bucket lower bound (use in sortRules/having)."
                                },
                                binWidth = new
                                {
                                    type = "number",
                                    description = "Positive bin width in the same units as the source column.",
                                    minimum = 1e-9,
                                    maximum = 1000000
                                }
                            },
                            required = new[] { "columnName", "alias", "binWidth" },
                            additionalProperties = false
                        }
                    },
                    aggregations = new
                    {
                        type = "array",
                        description =
                            "One object per output metric. Each aggregation may carry its own filter so different metrics can share the same group keys (e.g. unfiltered Count plus filtered Count).",
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
        };
    }

    private sealed record SearchColumnsArguments
    {
        public string Keyword { get; init; } = string.Empty;
    }

    /// <summary>
    /// Tool-facing schema: column identity and coarse typing only (summaries and samples belong in profile_columns).
    /// </summary>
    private static LeanDatasetSchemaPayload BuildLeanSchemaPayload(DatasetSchema schema)
    {
        var columns = schema.Columns
            .Select(column => new LeanColumnSchemaPayload(
                column.Name,
                column.DataType.ToString(),
                column.IsNullable,
                column.IsNumeric,
                column.IsCategorical))
            .ToArray();

        return new LeanDatasetSchemaPayload(
            schema.DatasetName,
            schema.RowCount,
            schema.GeneratedAtUtc,
            columns);
    }

    private sealed record LeanDatasetSchemaPayload(
        string DatasetName,
        int RowCount,
        DateTimeOffset GeneratedAtUtc,
        IReadOnlyList<LeanColumnSchemaPayload> Columns);

    private sealed record LeanColumnSchemaPayload(
        string Name,
        string DataType,
        bool IsNullable,
        bool IsNumeric,
        bool IsCategorical);
}
