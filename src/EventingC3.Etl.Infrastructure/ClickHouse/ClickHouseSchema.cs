using System.Text;

namespace EventingC3.Etl.Infrastructure.ClickHouse;

/// <summary>The warehouse DDL (<c>deploy/clickhouse/001_schema.sql</c>, embedded at build time).</summary>
public static class ClickHouseSchema
{
    private const string ResourceName = "EventingC3.Etl.Infrastructure.ClickHouse.001_schema.sql";

    public static string Sql()
    {
        using var stream = typeof(ClickHouseSchema).Assembly.GetManifestResourceStream(ResourceName)
            ?? throw new InvalidOperationException($"Embedded resource {ResourceName} not found.");
        using var reader = new StreamReader(stream, Encoding.UTF8);
        return reader.ReadToEnd();
    }

    /// <summary>Individual statements with comment lines removed (the HTTP interface runs one statement per request).</summary>
    public static IEnumerable<string> Statements()
    {
        var withoutComments = string.Join('\n', Sql().Split('\n').Where(l => !l.TrimStart().StartsWith("--", StringComparison.Ordinal)));
        return withoutComments.Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Where(s => s.Length > 0);
    }
}
