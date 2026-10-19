namespace EventingC3.Etl.Infrastructure.ClickHouse;

/// <summary>ClickHouse HTTP interface settings.</summary>
public sealed class ClickHouseOptions
{
    /// <summary>HTTP(S) endpoint, e.g. <c>http://clickhouse:8123</c>. Point at a load balancer / Distributed-table node in production.</summary>
    public string Endpoint { get; set; } = "http://localhost:8123";

    public string Database { get; set; } = "eventing";

    public string Table { get; set; } = "tracking_events";

    public string Username { get; set; } = "default";

    public string Password { get; set; } = string.Empty;

    /// <summary>Per-insert timeout. A timed-out insert is retried with the same deduplication token, so it cannot double-write.</summary>
    public TimeSpan RequestTimeout { get; set; } = TimeSpan.FromSeconds(120);

    /// <summary>Request body compression: <c>none</c>, <c>gzip</c> or <c>br</c>. Use gzip across zones / regions.</summary>
    public string Compression { get; set; } = "none";

    /// <summary>Maximum concurrent HTTP connections to ClickHouse.</summary>
    public int MaxConnections { get; set; } = 32;

    /// <summary>Create the database and apply <c>001_schema.sql</c> at startup (idempotent). Prefer migrations in production.</summary>
    public bool ApplySchemaOnStartup { get; set; }

    /// <summary>Extra ClickHouse settings appended to every insert (e.g. <c>insert_quorum</c>).</summary>
    public Dictionary<string, string> Settings { get; set; } = new(StringComparer.Ordinal);
}
