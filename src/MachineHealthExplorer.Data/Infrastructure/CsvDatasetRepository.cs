using CsvHelper;
using CsvHelper.Configuration;
using MachineHealthExplorer.Data.Querying;
using MachineHealthExplorer.Domain.Abstractions;
using MachineHealthExplorer.Domain.Models;
using System.Globalization;

namespace MachineHealthExplorer.Data.Infrastructure;

public sealed class CsvDatasetRepository : IDatasetRepository, IDatasetSchemaProvider
{
    private readonly DatasetOptions _options;
    private readonly SemaphoreSlim _gate = new(1, 1);
    private DatasetSnapshot? _snapshot;

    public CsvDatasetRepository(DatasetOptions options)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
    }

    public async Task<DatasetSnapshot> GetDatasetAsync(CancellationToken cancellationToken = default)
    {
        if (_snapshot is not null)
        {
            return _snapshot;
        }

        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_snapshot is null)
            {
                _snapshot = await LoadSnapshotAsync(cancellationToken).ConfigureAwait(false);
            }
        }
        finally
        {
            _gate.Release();
        }

        return _snapshot;
    }

    public async Task<DatasetSchema> GetSchemaAsync(CancellationToken cancellationToken = default)
        => (await GetDatasetAsync(cancellationToken).ConfigureAwait(false)).Schema;

    public async Task<ColumnProfile> GetColumnProfileAsync(string columnName, CancellationToken cancellationToken = default)
    {
        var schema = await GetSchemaAsync(cancellationToken).ConfigureAwait(false);
        var column = schema.Columns.FirstOrDefault(item => item.Name.Equals(columnName, StringComparison.OrdinalIgnoreCase))
            ?? throw new ArgumentException($"Column '{columnName}' was not found.", nameof(columnName));

        return new ColumnProfile
        {
            ColumnName = column.Name,
            DataType = column.DataType,
            IsNullable = column.IsNullable,
            NonNullCount = column.NonNullCount,
            CompletenessRatio = column.CompletenessRatio,
            IsNumeric = column.IsNumeric,
            IsCategorical = column.IsCategorical,
            RowCount = schema.RowCount,
            NullCount = column.NullCount,
            DistinctCount = column.DistinctCount,
            CardinalityHint = column.CardinalityHint,
            SampleValues = column.SampleValues,
            NumericSummary = column.NumericSummary,
            CategoricalSummary = column.CategoricalSummary
        };
    }

    public async Task<SearchColumnsResult> SearchColumnsAsync(string keyword, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(keyword))
        {
            throw new ArgumentException("Search keyword must be provided.", nameof(keyword));
        }

        var schema = await GetSchemaAsync(cancellationToken).ConfigureAwait(false);
        var normalizedKeyword = keyword.Trim();

        return new SearchColumnsResult
        {
            Keyword = normalizedKeyword,
            Matches = ColumnSearchMatcher.Search(schema.Columns, normalizedKeyword)
        };
    }

    private Task<DatasetSnapshot> LoadSnapshotAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var resolvedPath = DatasetPathResolver.Resolve(_options.DatasetPath);
        using var stream = File.OpenRead(resolvedPath);
        using var reader = new StreamReader(stream);
        using var csv = new CsvReader(reader, new CsvConfiguration(CultureInfo.InvariantCulture)
        {
            DetectColumnCountChanges = true,
            IgnoreBlankLines = true,
            MissingFieldFound = null,
            BadDataFound = null,
            TrimOptions = TrimOptions.Trim
        });

        if (!csv.Read() || !csv.ReadHeader())
        {
            throw new InvalidOperationException("The CSV file is empty or does not contain a header row.");
        }

        var headers = csv.HeaderRecord?
            .Where(header => !string.IsNullOrWhiteSpace(header))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray()
            ?? Array.Empty<string>();

        if (headers.Length == 0)
        {
            throw new InvalidOperationException("The CSV header row does not contain usable column names.");
        }

        var accumulators = headers.ToDictionary(
            header => header,
            header => new ColumnAccumulator(header, _options.SampleValueCount, _options.TopValueCount),
            StringComparer.OrdinalIgnoreCase);

        var rawRows = new List<Dictionary<string, string?>>();
        while (csv.Read())
        {
            cancellationToken.ThrowIfCancellationRequested();

            var rawRow = new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase);
            foreach (var header in headers)
            {
                var rawValue = csv.GetField(header);
                var normalized = string.IsNullOrWhiteSpace(rawValue) ? null : rawValue.Trim();
                rawRow[header] = normalized;
                accumulators[header].Observe(normalized);
            }

            rawRows.Add(rawRow);
        }

        var datasetName = string.IsNullOrWhiteSpace(_options.DatasetName)
            ? Path.GetFileNameWithoutExtension(resolvedPath)
            : _options.DatasetName;

        var columns = headers
            .Select(header => accumulators[header].BuildSchema(rawRows.Count))
            .ToArray();

        var columnLookup = columns.ToDictionary(column => column.Name, StringComparer.OrdinalIgnoreCase);
        var rows = rawRows
            .Select(rawRow => new DatasetRow(rawRow.ToDictionary(
                pair => pair.Key,
                pair => AnalyticsComputation.ConvertToColumnType(pair.Value, columnLookup[pair.Key]),
                StringComparer.OrdinalIgnoreCase)))
            .ToArray();

        return Task.FromResult(new DatasetSnapshot
        {
            DatasetName = datasetName,
            SourcePath = resolvedPath,
            Rows = rows,
            Schema = new DatasetSchema
            {
                DatasetName = datasetName,
                RowCount = rows.Length,
                GeneratedAtUtc = DateTimeOffset.UtcNow,
                Columns = columns
            }
        });
    }

    private sealed class ColumnAccumulator
    {
        private readonly int _sampleValueCount;
        private readonly int _topValueCount;
        private readonly Dictionary<string, int> _frequency = new(StringComparer.OrdinalIgnoreCase);
        private readonly List<string> _sampleValues = [];
        private readonly List<double> _numericValues = [];
        private int _nullCount;
        private int _nonNullCount;
        private bool _isBooleanCandidate = true;
        private bool _isIntegerCandidate = true;
        private bool _isDecimalCandidate = true;
        private bool _isDateTimeCandidate = true;

        public ColumnAccumulator(string name, int sampleValueCount, int topValueCount)
        {
            Name = name;
            _sampleValueCount = Math.Max(1, sampleValueCount);
            _topValueCount = Math.Max(1, topValueCount);
        }

        public string Name { get; }

        public void Observe(string? rawValue)
        {
            if (string.IsNullOrWhiteSpace(rawValue))
            {
                _nullCount++;
                return;
            }

            _nonNullCount++;
            if (_sampleValues.Count < _sampleValueCount && !_sampleValues.Contains(rawValue, StringComparer.OrdinalIgnoreCase))
            {
                _sampleValues.Add(rawValue);
            }

            _frequency.TryGetValue(rawValue, out var current);
            _frequency[rawValue] = current + 1;

            if (!AnalyticsComputation.TryParseBoolean(rawValue, out _))
            {
                _isBooleanCandidate = false;
            }

            if (!AnalyticsComputation.TryParseInteger(rawValue, out _))
            {
                _isIntegerCandidate = false;
            }

            if (AnalyticsComputation.TryParseDouble(rawValue, out var numericValue))
            {
                _numericValues.Add(numericValue);
            }
            else
            {
                _isDecimalCandidate = false;
            }

            if (!AnalyticsComputation.TryParseDateTime(rawValue, out _))
            {
                _isDateTimeCandidate = false;
            }
        }

        public ColumnSchema BuildSchema(int rowCount)
        {
            var dataType = InferDataType();
            var cardinalityHint = AnalyticsComputation.DetermineCardinalityHint(_frequency.Count, rowCount);
            var isNumeric = dataType is DataTypeKind.Integer or DataTypeKind.Decimal;
            var categoricalSummary = dataType == DataTypeKind.Boolean || AnalyticsComputation.IsCategoricalColumn(dataType, cardinalityHint)
                ? AnalyticsComputation.BuildCategoricalSummary(Name, ExpandFrequencyValues(), _topValueCount)
                : null;

            return new ColumnSchema
            {
                Name = Name,
                DataType = dataType,
                IsNullable = _nullCount > 0,
                NonNullCount = _nonNullCount,
                CompletenessRatio = rowCount == 0 ? 0 : _nonNullCount / (double)rowCount,
                IsNumeric = isNumeric,
                IsCategorical = categoricalSummary is not null,
                NullCount = _nullCount,
                DistinctCount = _frequency.Count,
                DistinctRatio = rowCount == 0 ? 0 : _frequency.Count / (double)rowCount,
                CardinalityHint = cardinalityHint,
                SampleValues = _sampleValues.ToArray(),
                NumericSummary = isNumeric ? AnalyticsComputation.BuildNumericSummary(Name, _numericValues.Cast<object?>()) : null,
                CategoricalSummary = categoricalSummary
            };
        }

        private IEnumerable<object?> ExpandFrequencyValues()
        {
            foreach (var pair in _frequency)
            {
                for (var index = 0; index < pair.Value; index++)
                {
                    yield return pair.Key;
                }
            }
        }

        private DataTypeKind InferDataType()
        {
            if (_nonNullCount == 0)
            {
                return DataTypeKind.String;
            }

            if (_isBooleanCandidate)
            {
                return DataTypeKind.Boolean;
            }

            if (_isIntegerCandidate)
            {
                return DataTypeKind.Integer;
            }

            if (_isDecimalCandidate)
            {
                return DataTypeKind.Decimal;
            }

            if (_isDateTimeCandidate)
            {
                return DataTypeKind.DateTime;
            }

            return DataTypeKind.String;
        }
    }
}
