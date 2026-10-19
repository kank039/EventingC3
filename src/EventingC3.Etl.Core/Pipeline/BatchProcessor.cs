using System.Buffers;
using System.Diagnostics;
using EventingC3.Etl.Core.Abstractions;
using EventingC3.Etl.Core.Domain;
using EventingC3.Etl.Core.Serialization;
using EventingC3.Etl.Core.Observability;
using EventingC3.Etl.Core.Transform;
using Microsoft.Extensions.Logging;

namespace EventingC3.Etl.Core.Pipeline;

/// <summary>
/// Transforms, encodes and loads one batch, then dead-letters its rejects and marks its offsets committable.
/// </summary>
/// <remarks>
/// Guarantees, in order of priority:
/// <list type="number">
/// <item>No silent loss: a batch's offsets only become committable after every record is either in the warehouse
/// or acknowledged by the dead-letter sink. Transient failures are retried forever with jittered back-off.</item>
/// <item>No duplicates from retries: every insert carries a deterministic deduplication token.</item>
/// <item>One bad row never blocks a partition: rows the warehouse refuses are isolated by bisection and dead-lettered.</item>
/// </list>
/// </remarks>
public sealed class BatchProcessor
{
    private readonly TrackingEventParser _parser;
    private readonly IEventSink _sink;
    private readonly IDeadLetterSink _deadLetters;
    private readonly PipelineOptions _options;
    private readonly EtlMetrics _metrics;
    private readonly EtlHealth _health;
    private readonly TimeProvider _time;
    private readonly ILogger<BatchProcessor> _logger;

    public BatchProcessor(
        TrackingEventParser parser,
        IEventSink sink,
        IDeadLetterSink deadLetters,
        PipelineOptions options,
        EtlMetrics metrics,
        EtlHealth health,
        TimeProvider time,
        ILogger<BatchProcessor> logger)
    {
        _parser = parser;
        _sink = sink;
        _deadLetters = deadLetters;
        _options = options;
        _metrics = metrics;
        _health = health;
        _time = time;
        _logger = logger;
    }

    public async Task ProcessAsync(RawBatch batch, CancellationToken cancellationToken)
    {
        _metrics.BatchStarted();
        try
        {
            var rejects = new List<DeadLetter>();
            var events = Transform(batch, rejects, out var sourceIndex);

            if (events.Count > 0)
            {
                using var encoded = Encode(events);
                await LoadAsync(encoded, events, sourceIndex, batch, 0, events.Count, batch.DeduplicationToken(), rejects, cancellationToken)
                    .ConfigureAwait(false);
            }

            if (rejects.Count > 0)
            {
                await PublishDeadLettersAsync(rejects, cancellationToken).ConfigureAwait(false);
            }

            batch.Complete();
        }
        finally
        {
            _metrics.BatchFinished();
        }
    }

    // ---- Transform ----------------------------------------------------------------------------------------------

    private List<TrackingEvent> Transform(RawBatch batch, List<DeadLetter> rejects, out List<int> sourceIndex)
    {
        var events = new List<TrackingEvent>(batch.Records.Count);
        sourceIndex = new List<int>(batch.Records.Count);
        var seen = new HashSet<Guid>(batch.Records.Count);
        var now = _time.GetUtcNow();
        long duplicates = 0, revoked = 0;

        for (var i = 0; i < batch.Records.Count; i++)
        {
            var record = batch.Records[i];
            if (record.Lease.IsRevoked)
            {
                // Partition moved to another worker, which will re-read it from the last commit.
                revoked++;
                continue;
            }

            ParseResult result;
            try
            {
                result = _parser.Parse(record.Payload, record.Lease.Partition, record.Offset);
            }
            catch (Exception ex)
            {
                // Defence in depth: the parser should never throw, but a single record must never fail a batch.
                result = ParseResult.Reject(RejectReason.MalformedJson, ex.GetType().Name + ": " + ex.Message);
            }

            if (result.Event is { } e)
            {
                if (seen.Add(e.EventId))
                {
                    events.Add(e);
                    sourceIndex.Add(i);
                }
                else
                {
                    duplicates++;
                }
            }
            else
            {
                _metrics.Reject(result.Reason);
                rejects.Add(ToDeadLetter(record, result.Reason, result.Detail, now));
            }
        }

        if (duplicates > 0)
        {
            _metrics.Duplicates(duplicates);
        }

        if (revoked > 0)
        {
            _metrics.RevokedRecordsDropped(revoked);
        }

        return events;
    }

    // ---- Encode -------------------------------------------------------------------------------------------------

    private EncodedRows Encode(List<TrackingEvent> events)
    {
        var buffer = new PooledBuffer(events.Count * 192);
        var rowStarts = ArrayPool<int>.Shared.Rent(events.Count + 1);
        var ingestedAt = _time.GetUtcNow().ToUnixTimeMilliseconds();
        var writer = new RowBinaryWriter(buffer);
        for (var i = 0; i < events.Count; i++)
        {
            rowStarts[i] = buffer.Length;
            TrackingEventEncoder.Write(writer, events[i], ingestedAt);
        }

        rowStarts[events.Count] = buffer.Length;
        return new EncodedRows(buffer, rowStarts);
    }

    private sealed class EncodedRows(PooledBuffer buffer, int[] rowStarts) : IDisposable
    {
        public ReadOnlyMemory<byte> Slice(int fromRow, int toRow) =>
            buffer.WrittenMemory[rowStarts[fromRow]..rowStarts[toRow]];

        public void Dispose()
        {
            buffer.Dispose();
            ArrayPool<int>.Shared.Return(rowStarts);
        }
    }

    // ---- Load ---------------------------------------------------------------------------------------------------

    private async Task LoadAsync(
        EncodedRows encoded,
        List<TrackingEvent> events,
        List<int> sourceIndex,
        RawBatch batch,
        int fromRow,
        int toRow,
        string token,
        List<DeadLetter> rejects,
        CancellationToken cancellationToken)
    {
        var rows = encoded.Slice(fromRow, toRow);
        for (var attempt = 1; ; attempt++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var started = Stopwatch.GetTimestamp();
            try
            {
                await _sink.InsertAsync(rows, toRow - fromRow, token, cancellationToken).ConfigureAwait(false);
                _metrics.Loaded(toRow - fromRow, rows.Length, Stopwatch.GetElapsedTime(started));
                _health.Succeeded();
                return;
            }
            catch (SinkException ex) when (ex.Kind == SinkErrorKind.InvalidData)
            {
                if (toRow - fromRow == 1)
                {
                    var record = batch.Records[sourceIndex[fromRow]];
                    _logger.LogWarning("Warehouse refused event {EventId} ({Lease}@{Offset}); dead-lettering: {Error}",
                        events[fromRow].EventId, record.Lease, record.Offset, ex.Message);
                    _metrics.Reject(RejectReason.SinkRejected);
                    rejects.Add(ToDeadLetter(record, RejectReason.SinkRejected, ex.Message, _time.GetUtcNow()));
                    return;
                }

                // Split and retry each half: isolates bad rows in O(k·log n) inserts instead of failing the batch.
                _metrics.SinkBisection();
                var mid = fromRow + (toRow - fromRow) / 2;
                _logger.LogWarning("Warehouse refused rows [{From},{To}) of batch {BatchId}; bisecting: {Error}",
                    fromRow, toRow, batch.Id, ex.Message);
                await LoadAsync(encoded, events, sourceIndex, batch, fromRow, mid, $"{token}:{fromRow}-{mid}", rejects, cancellationToken)
                    .ConfigureAwait(false);
                await LoadAsync(encoded, events, sourceIndex, batch, mid, toRow, $"{token}:{mid}-{toRow}", rejects, cancellationToken)
                    .ConfigureAwait(false);
                return;
            }
            catch (Exception ex) when (!cancellationToken.IsCancellationRequested)
            {
                // Transient, fatal-config and unclassified errors: never drop data — back off and retry the identical
                // insert (same token, so a write that actually landed is deduplicated by the server).
                _metrics.SinkRetry();
                _health.Failed(ex.Message);
                var delay = Backoff.Delay(attempt, _options.RetryInitialDelay, _options.RetryMaxDelay);
                var level = ex is SinkException { Kind: SinkErrorKind.Fatal } || attempt >= _options.UnhealthyAfterConsecutiveFailures
                    ? LogLevel.Error
                    : LogLevel.Warning;
                _logger.Log(level, ex, "Insert of batch {BatchId} ({Rows} rows) failed (attempt {Attempt}); retrying in {Delay}",
                    batch.Id, toRow - fromRow, attempt, delay);
                await Task.Delay(delay, _time, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    // ---- Dead letters -------------------------------------------------------------------------------------------

    private async Task PublishDeadLettersAsync(List<DeadLetter> rejects, CancellationToken cancellationToken)
    {
        for (var attempt = 1; ; attempt++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            try
            {
                await _deadLetters.PublishAsync(rejects, cancellationToken).ConfigureAwait(false);
                return;
            }
            catch (Exception ex) when (!cancellationToken.IsCancellationRequested)
            {
                _metrics.DeadLetterRetry();
                var delay = Backoff.Delay(attempt, _options.RetryInitialDelay, _options.RetryMaxDelay);
                _logger.LogError(ex, "Publishing {Count} dead letters failed (attempt {Attempt}); retrying in {Delay}",
                    rejects.Count, attempt, delay);
                await Task.Delay(delay, _time, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    private static DeadLetter ToDeadLetter(in RawRecord record, RejectReason reason, string? detail, DateTimeOffset at) =>
        new(record.Payload, reason, detail, record.Lease.Source, record.Lease.Partition, record.Offset, at);
}
