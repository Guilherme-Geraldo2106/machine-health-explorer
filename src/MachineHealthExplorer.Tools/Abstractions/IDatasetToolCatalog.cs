namespace MachineHealthExplorer.Tools.Abstractions;

public interface IDatasetToolCatalog
{
    IReadOnlyList<DatasetToolDescriptor> GetTools();
}

public sealed record DatasetToolDescriptor
{
    public string Name { get; init; } = string.Empty;
    public string Description { get; init; } = string.Empty;
    public IReadOnlyList<string> InputHints { get; init; } = Array.Empty<string>();
    public IReadOnlyList<string> ExamplePrompts { get; init; } = Array.Empty<string>();
}
