using System.Threading.Channels;
using EventingC3.Etl.Core.Abstractions;
using EventingC3.Etl.Core.Observability;
using Microsoft.Extensions.Logging;

namespace EventingC3.Etl.Core.Pipeline;

/// <summary>
/// Wires the stages together:
/// <code>
/// source ──(bounded records)──▶ batcher ──(bounded batches)──▶ N × BatchProcessor ──▶ sink / DLQ
///   ▲                                                                   │
///   └──────────────── committable offsets (PartitionLease) ◀────────────┘
/// </code>
/// Both channels are bounded, so a slow warehouse propagates back-pressure all the way to the source
/// instead of growing memory. Any stage fault stops the whole pipeline (fail fast; the host restarts it
/// and uncommitted records are replayed).
/// </summary>
public sealed class EtlPipeline
{
    private readonly IRecordSource _source;
    private readonly Batcher _batcher;
    private readonly BatchProcessor _processor;
    private readonly PipelineOptions _options;
    private readonly EtlHealth _health;
    private readonly ILogger<EtlPipeline> _logger;

    public EtlPipeline(
        IRecordSource source,
        Batcher batcher,
        BatchProcessor processor,
        PipelineOptions options,
        EtlHealth health,
        ILogger<EtlPipeline> logger)
    {
        _source = source;
        _batcher = batcher;
        _processor = processor;
        _options = options;
        _health = health;
        _logger = logger;
    }

    /// <summary>
    /// Runs until <paramref name="stoppingToken"/> fires or the source is exhausted. On stop, in-flight
    /// batches get <see cref="PipelineOptions.ShutdownGracePeriod"/> to finish before final offsets are committed.
    /// </summary>
    public async Task RunAsync(CancellationToken stoppingToken)
    {
        var records = Channel.CreateBounded<RawRecord>(new BoundedChannelOptions(_options.RecordBufferCapacity)
        {
            SingleWriter = true,
            SingleReader = true,
            FullMode = BoundedChannelFullMode.Wait,
        });
        var batches = Channel.CreateBounded<RawBatch>(new BoundedChannelOptions(Math.Max(1, _options.MaxConcurrentBatches))
        {
            SingleWriter = true,
            SingleReader = false,
            FullMode = BoundedChannelFullMode.Wait,
        });

        using var stopReading = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        using var abort = new CancellationTokenSource();
        await using var grace = stoppingToken.Register(() =>
        {
            _logger.LogInformation("Shutdown requested; draining in-flight batches for up to {Grace}", _options.ShutdownGracePeriod);
            abort.CancelAfter(_options.ShutdownGracePeriod);
        });

        void FailFast(Task task, string stage)
        {
            if (task.Exception is { } ex)
            {
                _logger.LogCritical(ex.GetBaseException(), "Pipeline stage {Stage} faulted; stopping", stage);
                _health.Faulted($"{stage}: {ex.GetBaseException().Message}");
                stopReading.Cancel();
                abort.Cancel();
            }
        }

        var sourceTask = Task.Run(() => _source.RunAsync(records.Writer, stopReading.Token), CancellationToken.None);
        _ = sourceTask.ContinueWith(t =>
        {
            records.Writer.TryComplete(); // in case the source did not
            FailFast(t, "source");
        }, TaskScheduler.Default);

        var batcherTask = Task.Run(() => _batcher.RunAsync(records.Reader, batches.Writer, abort.Token), CancellationToken.None);
        _ = batcherTask.ContinueWith(t => FailFast(t, "batcher"), TaskScheduler.Default);

        var workers = new Task[Math.Max(1, _options.MaxConcurrentBatches)];
        for (var i = 0; i < workers.Length; i++)
        {
            workers[i] = Task.Run(() => WorkAsync(batches.Reader, abort.Token), CancellationToken.None);
            _ = workers[i].ContinueWith(t => FailFast(t, "processor"), TaskScheduler.Default);
        }

        var all = new List<Task>(workers) { sourceTask, batcherTask };
        try
        {
            await Task.WhenAll(all).ConfigureAwait(false);
        }
        catch
        {
            // Inspected below: cancellations during shutdown are expected, faults are not.
        }

        var faults = all.Where(t => t.IsFaulted).SelectMany(t => t.Exception!.InnerExceptions)
            .Where(e => e is not OperationCanceledException).ToList();

        try
        {
            // Commit whatever completed — even after a fault, completed batches are durable.
            using var commitTimeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
            await _source.CompleteAsync(commitTimeout.Token).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Final offset commit failed; uncommitted records will be replayed and deduplicated");
        }

        if (faults.Count > 0)
        {
            throw new AggregateException("ETL pipeline faulted", faults);
        }

        _logger.LogInformation("Pipeline stopped cleanly");
    }

    private async Task WorkAsync(ChannelReader<RawBatch> batches, CancellationToken cancellationToken)
    {
        await foreach (var batch in batches.ReadAllAsync(cancellationToken).ConfigureAwait(false))
        {
            await _processor.ProcessAsync(batch, cancellationToken).ConfigureAwait(false);
        }
    }
}
