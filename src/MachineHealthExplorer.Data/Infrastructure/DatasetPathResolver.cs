namespace MachineHealthExplorer.Data.Infrastructure;

internal static class DatasetPathResolver
{
    public static string Resolve(string configuredPath)
    {
        if (string.IsNullOrWhiteSpace(configuredPath))
        {
            throw new InvalidOperationException("Dataset path must be provided.");
        }

        if (Path.IsPathRooted(configuredPath))
        {
            return File.Exists(configuredPath)
                ? Path.GetFullPath(configuredPath)
                : throw new FileNotFoundException($"Dataset file was not found at '{configuredPath}'.", configuredPath);
        }

        var candidates = EnumerateCandidatePaths(configuredPath)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray();

        foreach (var candidate in candidates)
        {
            if (File.Exists(candidate))
            {
                return Path.GetFullPath(candidate);
            }
        }

        throw new FileNotFoundException(
            $"Dataset file '{configuredPath}' was not found. Checked: {string.Join(", ", candidates)}",
            configuredPath);
    }

    private static IEnumerable<string> EnumerateCandidatePaths(string relativePath)
    {
        var baseDirectories = new[]
        {
            Directory.GetCurrentDirectory(),
            AppContext.BaseDirectory
        }
        .Where(path => !string.IsNullOrWhiteSpace(path))
        .Distinct(StringComparer.OrdinalIgnoreCase);

        foreach (var baseDirectory in baseDirectories)
        {
            var current = new DirectoryInfo(baseDirectory);

            while (current is not null)
            {
                yield return Path.Combine(current.FullName, relativePath);
                current = current.Parent;
            }
        }
    }
}
