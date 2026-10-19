using System.Collections.Concurrent;
using System.IO.Compression;
using System.Threading.Channels;
using EventingC3.Etl.Core.Abstractions;
using EventingC3.Etl.Core.Observability;
using EventingC3.Etl.Core.Pipeline;
using Microsoft.Extensions.Logging;

namespace EventingC3.Etl.Infrastructure.Files;

/// <summary>
/// Reads NDJSON files from a directory. Each file is a "partition" whose offsets are line numbers; when the
/// whole file is committed it is moved to the processed directory. A crash mid-file means the file is read
/// again on restart and the overlap is deduplicated in the warehouse.
/// </summary>
public sealed class FileRecordSource : IRecordSource
{
    private readonly FileSourceOptions _options;
    private readonly EtlMetrics _metrics;
    private readonly ILogger<FileRecordSource> _logger;
    private readonly ConcurrentDictionary<string, FileState> _files = new(StringComparer.Ordinal);
    private int _nextFileIndex;

    public FileRecordSource(FileSourceOptions options, EtlMetrics metrics, ILogger<FileRecordSource> logger)
    {
        _options = options;
        _metrics = metrics;
        _logger = logger;
    }

    /// <summary>Number of files fully loaded and moved during this run.</summary>
    public int FilesCompleted => _files.Values.Count(f => f.Completed);

    public async Task RunAsync(ChannelWriter<RawRecord> output, CancellationToken stopReading)
    {
        try
        {
            Directory.CreateDirectory(_options.InputDirectory);
            Directory.CreateDirectory(_options.ProcessedDirectory);

            while (!stopReading.IsCancellationRequested)
            {
                var next = Directory.EnumerateFiles(_options.InputDirectory, _options.SearchPattern)
                    .Order(StringComparer.Ordinal)
                    .FirstOrDefault(path => !_files.ContainsKey(path));

                if (next is null)
                {
                    if (_options.StopWhenIdle)
                    {
                        _logger.LogInformation("No more input files; source finished");
                        return;
                    }

                    await Task.Delay(_options.PollInterval, stopReading).ConfigureAwait(false);
                    continue;
                }

                await ReadFileAsync(next, output, stopReading).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (stopReading.IsCancellationRequested)
        {
        }
        finally
        {
            output.TryComplete();
        }
    }

    public Task CompleteAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    private async Task ReadFileAsync(string path, ChannelWriter<RawRecord> output, CancellationToken cancellationToken)
    {
        var state = new FileState(path);
        var lease = new PartitionLease(path, Interlocked.Increment(ref _nextFileIndex), (_, next) => state.OnCommittable(next, this));
        _files[path] = state;
        _logger.LogInformation("Reading {File}", path);

        long lastLine = 0;
        await using (var file = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, 1 << 16, FileOptions.Asynchronous | FileOptions.SequentialScan))
        await using (var stream = path.EndsWith(".gz", StringComparison.OrdinalIgnoreCase)
                         ? new GZipStream(file, CompressionMode.Decompress)
                         : (Stream)file)
        {
            await foreach (var (line, number) in ReadLinesAsync(stream, _options.MaxLineBytes, cancellationToken).ConfigureAwait(false))
            {
                _metrics.RecordRead();
                await output.WriteAsync(new RawRecord(line, lease, number), cancellationToken).ConfigureAwait(false);
                lastLine = number;
            }
        }

        state.Seal(lastLine, lease.CommittableOffset, this);
    }

    /// <summary>Splits a byte stream on <c>\n</c> without decoding, skipping blank lines and bounding line length.</summary>
    internal static async IAsyncEnumerable<(byte[] Line, long Number)> ReadLinesAsync(
        Stream stream,
        int maxLineBytes,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var buffer = new byte[Math.Min(1 << 20, maxLineBytes + 1)];
        int start = 0, end = 0;
        long number = 0;
        var discarding = false;

        while (true)
        {
            var read = await stream.ReadAsync(buffer.AsMemory(end), cancellationToken).ConfigureAwait(false);
            end += read;

            int newline;
            while ((newline = buffer.AsSpan(start, end - start).IndexOf((byte)'\n')) >= 0)
            {
                number++;
                if (discarding)
                {
                    discarding = false;
                }
                else if (Trimmed(buffer.AsSpan(start, newline)) is { Length: > 0 } line)
                {
                    yield return (line.ToArray(), number);
                }

                start += newline + 1;
            }

            if (read == 0)
            {
                if (end > start)
                {
                    number++;
                    if (!discarding && Trimmed(buffer.AsSpan(start, end - start)) is { Length: > 0 } last)
                    {
                        yield return (last.ToArray(), number);
                    }
                }

                yield break;
            }

            // Compact, then grow (bounded) if a single line fills the buffer.
            if (start > 0)
            {
                buffer.AsSpan(start, end - start).CopyTo(buffer);
                end -= start;
                start = 0;
            }

            if (end == buffer.Length)
            {
                if (buffer.Length > maxLineBytes)
                {
                    // Oversized line: emit its head (the parser rejects it as TooLarge) and skip to the next newline.
                    if (!discarding)
                    {
                        yield return (buffer.AsSpan(0, end).ToArray(), number + 1);
                    }

                    discarding = true;
                    end = 0;
                }
                else
                {
                    Array.Resize(ref buffer, Math.Min(buffer.Length * 2, maxLineBytes + 1));
                }
            }
        }
    }

    private static ReadOnlySpan<byte> Trimmed(ReadOnlySpan<byte> line) => line.Trim(" \t\r"u8);

    private void MoveToProcessed(FileState state)
    {
        try
        {
            var target = Path.Combine(_options.ProcessedDirectory, Path.GetFileName(state.Path));
            File.Move(state.Path, target, overwrite: true);
            _logger.LogInformation("Completed {File}", state.Path);
        }
        catch (Exception ex)
        {
            // Left in place: it stays marked as seen for this run and is re-read (and deduplicated) next run.
            _logger.LogError(ex, "Could not move completed file {File}", state.Path);
        }
    }

    private sealed class FileState(string path)
    {
        private readonly object _gate = new();
        private long _lastLine = -1;
        private long _committable = -1;

        public string Path { get; } = path;

        public bool Completed { get; private set; }

        public void OnCommittable(long nextOffset, FileRecordSource owner)
        {
            lock (_gate)
            {
                _committable = nextOffset;
                TryComplete(owner);
            }
        }

        public void Seal(long lastLine, long committable, FileRecordSource owner)
        {
            lock (_gate)
            {
                _lastLine = lastLine;
                _committable = Math.Max(_committable, committable);
                TryComplete(owner);
            }
        }

        private void TryComplete(FileRecordSource owner)
        {
            // Empty files (lastLine 0) complete immediately; otherwise wait until the last line is committed.
            if (!Completed && _lastLine >= 0 && (_lastLine == 0 || _committable > _lastLine))
            {
                Completed = true;
                ThreadPool.UnsafeQueueUserWorkItem(_ => owner.MoveToProcessed(this), null);
            }
        }
    }
}
