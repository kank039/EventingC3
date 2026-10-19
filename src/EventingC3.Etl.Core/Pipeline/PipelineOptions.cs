namespace EventingC3.Etl.Core.Pipeline;

/// <summary>Batching, concurrency and resilience settings. Defaults are sized for ~1B records/day per cluster.</summary>
public sealed class PipelineOptions
{
    /// <summary>
    /// Cut a batch at this many records. ClickHouse prefers few large inserts (each insert creates a part);
    /// 100k rows ≈ 15–40 MB is in its sweet spot.
    /// </summary>
    public int MaxBatchRecords { get; set; } = 100_000;

    /// <summary>Cut a batch at this many raw payload bytes, bounding per-batch memory.</summary>
    public long MaxBatchBytes { get; set; } = 64L * 1024 * 1024;

    /// <summary>Cut a batch after this long even if small, bounding end-to-end latency at low volume.</summary>
    public TimeSpan MaxBatchLatency { get; set; } = TimeSpan.FromSeconds(2);

    /// <summary>Raw records buffered between the source and the batcher (back-pressure point #1).</summary>
    public int RecordBufferCapacity { get; set; } = 200_000;

    /// <summary>Batches transforming/loading concurrently. Also bounds memory: roughly this × <see cref="MaxBatchBytes"/> × 3.</summary>
    public int MaxConcurrentBatches { get; set; } = 4;

    /// <summary>First retry delay for transient sink / dead-letter failures.</summary>
    public TimeSpan RetryInitialDelay { get; set; } = TimeSpan.FromMilliseconds(200);

    /// <summary>Cap on the exponential retry delay. Retries never give up: data is never dropped, the pipeline stalls instead.</summary>
    public TimeSpan RetryMaxDelay { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>After this many consecutive sink failures the worker reports itself not ready.</summary>
    public int UnhealthyAfterConsecutiveFailures { get; set; } = 5;

    /// <summary>How long to let in-flight batches finish after shutdown is requested before abandoning them (they will be replayed).</summary>
    public TimeSpan ShutdownGracePeriod { get; set; } = TimeSpan.FromSeconds(25);
}
