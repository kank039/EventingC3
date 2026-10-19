namespace EventingC3.Etl.Infrastructure.Files;

/// <summary>
/// Settings for bulk / backfill loads from newline-delimited JSON files (e.g. SFTP drops). Files may be
/// gzip-compressed (<c>.gz</c>). A file moves to <see cref="ProcessedDirectory"/> only after every line
/// has been loaded or dead-lettered.
/// </summary>
public sealed class FileSourceOptions
{
    public string InputDirectory { get; set; } = "./data/inbox";

    public string ProcessedDirectory { get; set; } = "./data/processed";

    public string SearchPattern { get; set; } = "*.ndjson*";

    public TimeSpan PollInterval { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>Finish (and let the worker exit) once no unread files remain — for one-off backfill jobs.</summary>
    public bool StopWhenIdle { get; set; }

    /// <summary>Lines longer than this are cut and dead-lettered as too large rather than buffered without bound.</summary>
    public int MaxLineBytes { get; set; } = 1024 * 1024;
}

/// <summary>Settings for the file-based dead-letter sink.</summary>
public sealed class FileDeadLetterOptions
{
    public string Directory { get; set; } = "./data/dlq";
}
