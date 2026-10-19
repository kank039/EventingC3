using System.Threading.Channels;

namespace EventingC3.Etl.Core.Pipeline;

/// <summary>
/// Groups the record stream into batches, cutting on whichever comes first: record count, payload bytes or
/// age. Runs as a single task so per-partition offsets enter batches strictly in order.
/// </summary>
public sealed class Batcher
{
    private readonly PipelineOptions _options;
    private readonly TimeProvider _time;
    private long _nextBatchId;

    public Batcher(PipelineOptions options, TimeProvider time)
    {
        _options = options;
        _time = time;
    }

    /// <summary>
    /// Runs until <paramref name="input"/> completes (then flushes the partial batch and completes
    /// <paramref name="output"/>) or <paramref name="cancellationToken"/> fires (partial batch abandoned).
    /// </summary>
    public async Task RunAsync(ChannelReader<RawRecord> input, ChannelWriter<RawBatch> output, CancellationToken cancellationToken)
    {
        RawBatch? current = null;
        try
        {
            while (true)
            {
                if (current is null)
                {
                    // Nothing pending: wait indefinitely. False means the input is completed and drained.
                    if (!await input.WaitToReadAsync(cancellationToken).ConfigureAwait(false))
                    {
                        break;
                    }
                }
                else
                {
                    var remaining = current.CreatedAt + _options.MaxBatchLatency - _time.GetUtcNow();
                    if (remaining <= TimeSpan.Zero
                        || (!input.TryPeek(out _) && !await WaitForDataAsync(input, remaining, cancellationToken).ConfigureAwait(false)))
                    {
                        // Latency budget spent (or input completed): ship what we have.
                        await output.WriteAsync(current, cancellationToken).ConfigureAwait(false);
                        current = null;
                        continue;
                    }
                }

                while (input.TryRead(out var record))
                {
                    current ??= new RawBatch(++_nextBatchId, Math.Min(_options.MaxBatchRecords, 16_384), _time.GetUtcNow());
                    current.Add(record);
                    if (current.Records.Count >= _options.MaxBatchRecords || current.Bytes >= _options.MaxBatchBytes)
                    {
                        await output.WriteAsync(current, cancellationToken).ConfigureAwait(false);
                        current = null;
                    }
                }
            }

            output.TryComplete();
        }
        catch (Exception ex)
        {
            output.TryComplete(ex is OperationCanceledException ? null : ex);
            throw;
        }
    }

    /// <summary>Waits up to <paramref name="timeout"/> for data. Returns false on timeout or when the input completes.</summary>
    private async Task<bool> WaitForDataAsync(ChannelReader<RawRecord> input, TimeSpan timeout, CancellationToken cancellationToken)
    {
        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeoutCts.CancelAfter(timeout);
        try
        {
            return await input.WaitToReadAsync(timeoutCts.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            return false;
        }
    }
}
