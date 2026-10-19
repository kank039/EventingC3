using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using System.Globalization;
using System.Text;
using EventingC3.Etl.Core.Transform;

namespace EventingC3.Etl.Core.Observability;

/// <summary>
/// Pipeline counters. Exposed both as a <see cref="Meter"/> (for OpenTelemetry / dotnet-counters) and as
/// Prometheus text via <see cref="RenderPrometheus"/>. Updates are lock-free.
/// </summary>
public sealed class EtlMetrics : IDisposable
{
    public const string MeterName = "EventingC3.Etl";

    private readonly Meter _meter;
    private readonly ConcurrentDictionary<RejectReason, long> _rejected = new();
    private long _recordsRead;
    private long _recordsLoaded;
    private long _duplicatesDropped;
    private long _revokedDropped;
    private long _batchesLoaded;
    private long _bytesLoaded;
    private long _sinkRetries;
    private long _sinkBisections;
    private long _deadLetterRetries;
    private long _backpressureEvents;
    private long _loadMillisTotal;
    private long _inflightBatches;
    private long _lastLoadUnixMs;

    public EtlMetrics()
    {
        _meter = new Meter(MeterName);
        _meter.CreateObservableCounter("etl.records.read", () => Volatile.Read(ref _recordsRead));
        _meter.CreateObservableCounter("etl.records.loaded", () => Volatile.Read(ref _recordsLoaded));
        _meter.CreateObservableCounter("etl.records.rejected", () => _rejected.Values.Sum());
        _meter.CreateObservableCounter("etl.records.duplicates", () => Volatile.Read(ref _duplicatesDropped));
        _meter.CreateObservableCounter("etl.batches.loaded", () => Volatile.Read(ref _batchesLoaded));
        _meter.CreateObservableCounter("etl.sink.retries", () => Volatile.Read(ref _sinkRetries));
        _meter.CreateObservableGauge("etl.batches.inflight", () => Volatile.Read(ref _inflightBatches));
    }

    public long RecordsRead => Volatile.Read(ref _recordsRead);
    public long RecordsLoaded => Volatile.Read(ref _recordsLoaded);
    public long RecordsRejected => _rejected.Values.Sum();
    public long DuplicatesDropped => Volatile.Read(ref _duplicatesDropped);
    public long BatchesLoaded => Volatile.Read(ref _batchesLoaded);
    public long SinkRetries => Volatile.Read(ref _sinkRetries);
    public long SinkBisections => Volatile.Read(ref _sinkBisections);
    public long InflightBatches => Volatile.Read(ref _inflightBatches);
    public long RevokedDropped => Volatile.Read(ref _revokedDropped);

    public long Rejected(RejectReason reason) => _rejected.GetValueOrDefault(reason);

    public void RecordRead() => Interlocked.Increment(ref _recordsRead);
    public void Backpressure() => Interlocked.Increment(ref _backpressureEvents);
    public void Duplicates(long count) => Interlocked.Add(ref _duplicatesDropped, count);
    public void RevokedRecordsDropped(long count) => Interlocked.Add(ref _revokedDropped, count);
    public void Reject(RejectReason reason) => _rejected.AddOrUpdate(reason, 1, static (_, v) => v + 1);
    public void SinkRetry() => Interlocked.Increment(ref _sinkRetries);
    public void SinkBisection() => Interlocked.Increment(ref _sinkBisections);
    public void DeadLetterRetry() => Interlocked.Increment(ref _deadLetterRetries);
    public void BatchStarted() => Interlocked.Increment(ref _inflightBatches);
    public void BatchFinished() => Interlocked.Decrement(ref _inflightBatches);

    public void Loaded(long rows, long bytes, TimeSpan elapsed)
    {
        Interlocked.Add(ref _recordsLoaded, rows);
        Interlocked.Add(ref _bytesLoaded, bytes);
        Interlocked.Add(ref _loadMillisTotal, (long)elapsed.TotalMilliseconds);
        Interlocked.Increment(ref _batchesLoaded);
        Volatile.Write(ref _lastLoadUnixMs, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
    }

    /// <summary>Prometheus text exposition format (v0.0.4).</summary>
    public string RenderPrometheus()
    {
        var sb = new StringBuilder(2048);
        Counter(sb, "etl_records_read_total", "Raw records read from the source.", RecordsRead);
        Counter(sb, "etl_records_loaded_total", "Rows inserted into the warehouse.", RecordsLoaded);
        Counter(sb, "etl_records_duplicates_total", "In-batch duplicates dropped (same event id).", DuplicatesDropped);
        Counter(sb, "etl_records_revoked_total", "Records skipped because their partition was reassigned.", RevokedDropped);
        sb.Append("# HELP etl_records_rejected_total Records routed to the dead-letter queue.\n# TYPE etl_records_rejected_total counter\n");
        foreach (var (reason, count) in _rejected.OrderBy(kv => kv.Key))
        {
            sb.Append(CultureInfo.InvariantCulture, $"etl_records_rejected_total{{reason=\"{reason}\"}} {count}\n");
        }

        Counter(sb, "etl_batches_loaded_total", "Batches inserted.", BatchesLoaded);
        Counter(sb, "etl_bytes_loaded_total", "RowBinary bytes inserted.", Volatile.Read(ref _bytesLoaded));
        Counter(sb, "etl_load_milliseconds_total", "Time spent in successful inserts.", Volatile.Read(ref _loadMillisTotal));
        Counter(sb, "etl_sink_retries_total", "Insert attempts retried.", SinkRetries);
        Counter(sb, "etl_sink_bisections_total", "Batch splits performed to isolate rows the warehouse refused.", SinkBisections);
        Counter(sb, "etl_deadletter_retries_total", "Dead-letter publish attempts retried.", Volatile.Read(ref _deadLetterRetries));
        Counter(sb, "etl_backpressure_total", "Times the source paused because the pipeline was full.", Volatile.Read(ref _backpressureEvents));
        Gauge(sb, "etl_batches_inflight", "Batches currently transforming or loading.", InflightBatches);
        Gauge(sb, "etl_last_load_timestamp_seconds", "Unix time of the last successful insert.", Volatile.Read(ref _lastLoadUnixMs) / 1000);
        return sb.ToString();
    }

    private static void Counter(StringBuilder sb, string name, string help, long value) =>
        sb.Append(CultureInfo.InvariantCulture, $"# HELP {name} {help}\n# TYPE {name} counter\n{name} {value}\n");

    private static void Gauge(StringBuilder sb, string name, string help, long value) =>
        sb.Append(CultureInfo.InvariantCulture, $"# HELP {name} {help}\n# TYPE {name} gauge\n{name} {value}\n");

    public void Dispose() => _meter.Dispose();
}
