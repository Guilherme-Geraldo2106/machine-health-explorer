using MachineHealthExplorer.Data.Infrastructure;
using MachineHealthExplorer.Data.Querying;
using MachineHealthExplorer.Data.Services;
using MachineHealthExplorer.Domain.Abstractions;
using MachineHealthExplorer.Tools.Abstractions;
using MachineHealthExplorer.Tools.Services;

namespace MachineHealthExplorer.Tests.Infrastructure;

internal static class TestDatasetFactory
{
    public static CsvDatasetRepository CreateRepository()
        => new(new DatasetOptions
        {
            DatasetName = "AI4I 2020 Predictive Maintenance Dataset",
            DatasetPath = ResolveDatasetPath(),
            SampleValueCount = 5,
            TopValueCount = 8
        });

    public static IDatasetAnalyticsEngine CreateAnalyticsEngine(CsvDatasetRepository repository)
        => new DatasetAnalyticsEngine(repository);

    public static IDatasetQueryEngine CreateQueryEngine(CsvDatasetRepository repository)
        => CreateAnalyticsEngine(repository);

    public static IDatasetAnalyticsService CreateAnalyticsService(CsvDatasetRepository repository)
        => CreateAnalyticsEngine(repository);

    public static IMachineHealthAnalyticsService CreateMachineHealthAnalyticsService(CsvDatasetRepository repository)
        => new MachineHealthAnalyticsService(CreateAnalyticsEngine(repository));

    public static IDatasetToolService CreateToolService(CsvDatasetRepository repository)
    {
        var analyticsEngine = CreateAnalyticsEngine(repository);
        return new DatasetToolService(analyticsEngine);
    }

    private static string ResolveDatasetPath()
    {
        var current = new DirectoryInfo(AppContext.BaseDirectory);
        while (current is not null)
        {
            var candidate = Path.Combine(current.FullName, "data", "ai4i2020.csv");
            if (File.Exists(candidate))
            {
                return candidate;
            }

            current = current.Parent;
        }

        throw new FileNotFoundException("Unable to locate data/ai4i2020.csv for the test suite.");
    }
}
