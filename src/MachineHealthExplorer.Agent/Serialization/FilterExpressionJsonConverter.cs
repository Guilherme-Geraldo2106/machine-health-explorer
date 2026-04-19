using MachineHealthExplorer.Domain.Models;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace MachineHealthExplorer.Agent.Serialization;

internal sealed class FilterExpressionJsonConverter : JsonConverter<FilterExpression>
{
    public override FilterExpression? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        if (reader.TokenType == JsonTokenType.Null)
        {
            return null;
        }

        using var document = JsonDocument.ParseValue(ref reader);
        return ReadExpression(document.RootElement);
    }

    public override void Write(Utf8JsonWriter writer, FilterExpression value, JsonSerializerOptions options)
        => JsonSerializer.Serialize(writer, value, value.GetType(), options);

    private static FilterExpression ReadExpression(JsonElement element)
    {
        if (TryGetProperty(element, "expressions", out var expressionsElement))
        {
            return new FilterGroupExpression
            {
                Operator = ReadEnum(element, "operator", LogicalOperator.And),
                Expressions = expressionsElement.ValueKind == JsonValueKind.Array
                    ? expressionsElement.EnumerateArray().Select(ReadExpression).ToArray()
                    : Array.Empty<FilterExpression>()
            };
        }

        return new FilterConditionExpression
        {
            ColumnName = ReadString(element, "columnName"),
            Operator = ReadEnum(element, "operator", FilterOperator.Equals),
            Value = TryGetProperty(element, "value", out var valueElement) ? Materialize(valueElement) : null,
            SecondValue = TryGetProperty(element, "secondValue", out var secondValueElement) ? Materialize(secondValueElement) : null,
            Values = TryGetProperty(element, "values", out var valuesElement) && valuesElement.ValueKind == JsonValueKind.Array
                ? valuesElement.EnumerateArray().Select(Materialize).ToArray()
                : Array.Empty<object?>()
        };
    }

    private static string ReadString(JsonElement element, string propertyName)
        => TryGetProperty(element, propertyName, out var property) && property.ValueKind == JsonValueKind.String
            ? property.GetString() ?? string.Empty
            : string.Empty;

    private static TEnum ReadEnum<TEnum>(JsonElement element, string propertyName, TEnum fallback)
        where TEnum : struct, Enum
    {
        if (!TryGetProperty(element, propertyName, out var property))
        {
            return fallback;
        }

        if (property.ValueKind == JsonValueKind.Number && property.TryGetInt32(out var numericValue))
        {
            return Enum.IsDefined(typeof(TEnum), numericValue)
                ? (TEnum)Enum.ToObject(typeof(TEnum), numericValue)
                : fallback;
        }

        if (property.ValueKind == JsonValueKind.String
            && Enum.TryParse<TEnum>(property.GetString(), ignoreCase: true, out var parsed))
        {
            return parsed;
        }

        return fallback;
    }

    private static bool TryGetProperty(JsonElement element, string propertyName, out JsonElement property)
    {
        foreach (var candidate in element.EnumerateObject())
        {
            if (candidate.Name.Equals(propertyName, StringComparison.OrdinalIgnoreCase))
            {
                property = candidate.Value;
                return true;
            }
        }

        property = default;
        return false;
    }

    private static object? Materialize(JsonElement element)
        => element.ValueKind switch
        {
            JsonValueKind.String => element.GetString(),
            JsonValueKind.Number when element.TryGetInt64(out var integerValue) => integerValue,
            JsonValueKind.Number => element.GetDouble(),
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            JsonValueKind.Null or JsonValueKind.Undefined => null,
            JsonValueKind.Array => element.EnumerateArray().Select(Materialize).ToArray(),
            JsonValueKind.Object => element.EnumerateObject()
                .ToDictionary(property => property.Name, property => Materialize(property.Value), StringComparer.OrdinalIgnoreCase),
            _ => element.ToString()
        };
}
