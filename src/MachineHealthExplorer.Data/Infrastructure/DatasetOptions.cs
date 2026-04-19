namespace MachineHealthExplorer.Data.Infrastructure;

public sealed record DatasetOptions
{
    public string DatasetName { get; init; } = "Machine Health Explorer Dataset";
    public string DatasetPath { get; init; } = "data/ai4i2020.csv";
    public int SampleValueCount { get; init; } = 5;
    public int TopValueCount { get; init; } = 8;
}
