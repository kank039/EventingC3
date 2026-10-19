using System.Text.Json;
using EventingC3.Etl.Core.Abstractions;

namespace EventingC3.Etl.Infrastructure.Files;

/// <summary>
/// Appends dead letters as NDJSON (payload base64-encoded, so non-UTF-8 input survives) to a daily file,
/// fsync'd before returning so a committed batch's rejects are never lost.
/// </summary>
public sealed class FileDeadLetterSink : IDeadLetterSink
{
    private readonly FileDeadLetterOptions _options;
    private readonly SemaphoreSlim _gate = new(1, 1);

    public FileDeadLetterSink(FileDeadLetterOptions options)
    {
        _options = options;
        Directory.CreateDirectory(options.Directory);
    }

    public async Task PublishAsync(IReadOnlyList<DeadLetter> letters, CancellationToken cancellationToken)
    {
        var path = Path.Combine(_options.Directory, $"dlq-{DateTime.UtcNow:yyyyMMdd}.ndjson");
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await using var file = new FileStream(path, FileMode.Append, FileAccess.Write, FileShare.Read, 1 << 16, FileOptions.Asynchronous);
            await using (var json = new Utf8JsonWriter(file))
            {
                foreach (var letter in letters)
                {
                    json.WriteStartObject();
                    json.WriteString("reason", letter.Reason.ToString());
                    json.WriteString("detail", letter.Detail);
                    json.WriteString("source", letter.Source);
                    json.WriteNumber("partition", letter.Partition);
                    json.WriteNumber("offset", letter.Offset);
                    json.WriteString("rejectedAt", letter.RejectedAt);
                    json.WriteBase64String("payload", letter.Payload);
                    json.WriteEndObject();
                    await json.FlushAsync(cancellationToken).ConfigureAwait(false);
                    json.Reset();
                    file.WriteByte((byte)'\n');
                }
            }

            file.Flush(flushToDisk: true);
        }
        finally
        {
            _gate.Release();
        }
    }
}
