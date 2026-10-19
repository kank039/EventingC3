using System.Collections.Concurrent;
using System.Threading.Channels;
using Confluent.Kafka;
using EventingC3.Etl.Core.Abstractions;
using EventingC3.Etl.Core.Observability;
using EventingC3.Etl.Core.Pipeline;
using Microsoft.Extensions.Logging;

namespace EventingC3.Etl.Infrastructure.Kafka;

/// <summary>
/// Kafka / Event Hubs consumer. Scale-out unit is the partition: run as many workers as there are partitions
/// and the consumer group spreads them (cooperative-sticky, so a rebalance only moves the partitions it must).
/// </summary>
/// <remarks>
/// <para><b>Delivery semantics.</b> Auto-commit is off. Offsets are committed only after the
/// <see cref="PartitionLease"/> reports a contiguous prefix of the partition loaded, so a crash at any point
/// replays (never skips) records; replays are absorbed by insert tokens and ReplacingMergeTree.</para>
/// <para><b>Back-pressure.</b> When the pipeline is full the consumer pauses its partitions but keeps polling,
/// so it stays in the group (no <c>max.poll.interval.ms</c> eviction) however long the warehouse is down.</para>
/// <para><b>Threading.</b> librdkafka consumers are not thread-safe for most calls, so every consumer call —
/// including commits requested by processor threads — happens on the single dedicated poll thread.</para>
/// </remarks>
public sealed class KafkaRecordSource : IRecordSource, IDisposable
{
    private static readonly TimeSpan PollTimeout = TimeSpan.FromMilliseconds(100);

    private readonly KafkaSourceOptions _options;
    private readonly EtlMetrics _metrics;
    private readonly ILogger<KafkaRecordSource> _logger;
    private readonly ConcurrentDictionary<TopicPartition, PartitionLease> _leases = new();
    private readonly ConcurrentDictionary<TopicPartition, long> _pendingCommits = new();
    private readonly Queue<RawRecord> _overflow = new();
    private IConsumer<Ignore, byte[]>? _consumer;
    private Task? _loop;

    public KafkaRecordSource(KafkaSourceOptions options, EtlMetrics metrics, ILogger<KafkaRecordSource> logger)
    {
        _options = options;
        _metrics = metrics;
        _logger = logger;
    }

    /// <summary>Partitions currently owned by this worker.</summary>
    public IReadOnlyCollection<TopicPartition> Assignment => _leases.Keys.ToList();

    public Task RunAsync(ChannelWriter<RawRecord> output, CancellationToken stopReading)
    {
        _loop = Task.Factory.StartNew(
            () => PollLoop(output, stopReading),
            CancellationToken.None,
            TaskCreationOptions.LongRunning,
            TaskScheduler.Default);
        return _loop;
    }

    public async Task CompleteAsync(CancellationToken cancellationToken)
    {
        if (_loop is not null)
        {
            await _loop.ContinueWith(_ => { }, TaskScheduler.Default).ConfigureAwait(false);
        }

        var consumer = Interlocked.Exchange(ref _consumer, null);
        if (consumer is null)
        {
            return;
        }

        try
        {
            CommitPending(consumer);
            consumer.Close(); // leaves the group cleanly so partitions move immediately
        }
        finally
        {
            consumer.Dispose();
        }
    }

    private void PollLoop(ChannelWriter<RawRecord> output, CancellationToken stop)
    {
        try
        {
            var consumer = BuildConsumer();
            _consumer = consumer;
            consumer.Subscribe(_options.Topic);
            _logger.LogInformation("Consuming {Topic} as group {Group}", _options.Topic, _options.GroupId);

            var paused = false;
            var nextCommit = DateTime.UtcNow + _options.CommitInterval;
            while (!stop.IsCancellationRequested)
            {
                if (DateTime.UtcNow >= nextCommit)
                {
                    CommitPending(consumer);
                    nextCommit = DateTime.UtcNow + _options.CommitInterval;
                }

                while (_overflow.Count > 0 && output.TryWrite(_overflow.Peek()))
                {
                    _overflow.Dequeue();
                }

                if (_overflow.Count > 0)
                {
                    // Pipeline full: pause fetching but keep polling to stay in the group and serve rebalances.
                    if (!paused)
                    {
                        _metrics.Backpressure();
                        paused = true;
                    }

                    SafePause(consumer);
                    var held = Consume(consumer);
                    if (held is not null)
                    {
                        _overflow.Enqueue(held.Value);
                    }

                    continue;
                }

                if (paused)
                {
                    SafeResume(consumer);
                    paused = false;
                }

                if (Consume(consumer) is { } record && !output.TryWrite(record))
                {
                    _overflow.Enqueue(record);
                }
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogCritical(ex, "Kafka poll loop failed");
            throw;
        }
        finally
        {
            output.TryComplete();
        }
    }

    private RawRecord? Consume(IConsumer<Ignore, byte[]> consumer)
    {
        ConsumeResult<Ignore, byte[]>? result;
        try
        {
            result = consumer.Consume(PollTimeout);
        }
        catch (ConsumeException ex) when (!ex.Error.IsFatal)
        {
            _logger.LogWarning("Kafka consume error (will retry): {Error}", ex.Error);
            return null;
        }

        if (result?.Message is null)
        {
            return null;
        }

        if (!_leases.TryGetValue(result.TopicPartition, out var lease))
        {
            // Should not happen (assignment handler runs first); skipping is safe because nothing commits past it.
            _logger.LogWarning("Message from unowned partition {TopicPartition}; skipping", result.TopicPartition);
            return null;
        }

        _metrics.RecordRead();
        // Tombstones / empty values become an empty payload and are dead-lettered by the parser.
        return new RawRecord(result.Message.Value ?? [], lease, result.Offset.Value);
    }

    // ---- Offset commits -----------------------------------------------------------------------------------------

    /// <summary>Called by processor threads (via the lease) when a partition's loaded prefix advances.</summary>
    private void OnCommittable(PartitionLease lease, long nextOffset)
    {
        var tp = new TopicPartition(lease.Source, new Partition(lease.Partition));
        if (_leases.TryGetValue(tp, out var current) && ReferenceEquals(current, lease))
        {
            _pendingCommits.AddOrUpdate(tp, nextOffset, (_, existing) => Math.Max(existing, nextOffset));
        }
    }

    /// <summary>Commits pending offsets. Poll thread only.</summary>
    private void CommitPending(IConsumer<Ignore, byte[]> consumer, IEnumerable<TopicPartition>? only = null)
    {
        var offsets = new List<TopicPartitionOffset>();
        foreach (var tp in only ?? _pendingCommits.Keys)
        {
            if (_pendingCommits.TryRemove(tp, out var offset) && _leases.ContainsKey(tp))
            {
                offsets.Add(new TopicPartitionOffset(tp, new Offset(offset)));
            }
        }

        if (offsets.Count == 0)
        {
            return;
        }

        try
        {
            consumer.Commit(offsets);
        }
        catch (KafkaException ex)
        {
            // Not fatal: the records are loaded; worst case they are replayed and deduplicated.
            _logger.LogWarning("Offset commit failed for {Count} partitions: {Error}", offsets.Count, ex.Error);
        }
    }

    // ---- Rebalance handling -------------------------------------------------------------------------------------

    private IConsumer<Ignore, byte[]> BuildConsumer()
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = _options.BootstrapServers,
            GroupId = _options.GroupId,
            ClientId = $"eventingc3-etl-{Environment.MachineName}",
            EnableAutoCommit = false,
            EnableAutoOffsetStore = false,
            AutoOffsetReset = AutoOffsetReset.Earliest,          // never skip data for a new group
            PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky,
            IsolationLevel = IsolationLevel.ReadCommitted,
            EnablePartitionEof = false,
            SessionTimeoutMs = 45_000,
            MaxPollIntervalMs = 300_000,
            FetchMaxBytes = 64 * 1024 * 1024,
            MaxPartitionFetchBytes = 4 * 1024 * 1024,
            QueuedMaxMessagesKbytes = 256 * 1024,
            FetchWaitMaxMs = 100,
            SocketKeepaliveEnable = true,
        };
        foreach (var (key, value) in _options.ConsumerConfig)
        {
            config.Set(key, value);
        }

        return new ConsumerBuilder<Ignore, byte[]>(config)
            .SetPartitionsAssignedHandler((_, partitions) =>
            {
                foreach (var tp in partitions)
                {
                    var lease = new PartitionLease(tp.Topic, tp.Partition.Value, OnCommittable);
                    _leases.AddOrUpdate(tp, lease, (_, stale) =>
                    {
                        stale.Revoke();
                        return lease;
                    });
                }

                _logger.LogInformation("Assigned {Partitions}", string.Join(",", partitions.Select(p => p.Partition.Value)));
            })
            .SetPartitionsRevokedHandler((consumer, partitions) =>
            {
                var revoked = partitions.Select(p => p.TopicPartition).ToList();
                // Commit what is already loaded while we still own the partitions, then stop tracking them.
                CommitPending(consumer, revoked);
                RevokeLeases(revoked);
                _logger.LogInformation("Revoked {Partitions}", string.Join(",", revoked.Select(p => p.Partition.Value)));
            })
            .SetPartitionsLostHandler((_, partitions) =>
            {
                var lost = partitions.Select(p => p.TopicPartition).ToList();
                RevokeLeases(lost);
                _logger.LogWarning("Lost {Partitions} (session expired); they will be replayed by the new owner",
                    string.Join(",", lost.Select(p => p.Partition.Value)));
            })
            .SetErrorHandler((_, error) =>
            {
                if (error.IsFatal)
                {
                    _logger.LogCritical("Fatal Kafka error: {Error}", error);
                }
                else
                {
                    _logger.LogWarning("Kafka error: {Error}", error);
                }
            })
            .SetLogHandler((_, log) => _logger.LogDebug("librdkafka {Facility}: {Message}", log.Facility, log.Message))
            .Build();
    }

    private void RevokeLeases(List<TopicPartition> partitions)
    {
        foreach (var tp in partitions)
        {
            if (_leases.TryRemove(tp, out var lease))
            {
                lease.Revoke();
            }

            _pendingCommits.TryRemove(tp, out _);
        }

        // Records buffered for revoked partitions would only be skipped downstream; drop them now.
        if (_overflow.Count > 0)
        {
            var keep = _overflow.Where(r => !r.Lease.IsRevoked).ToList();
            _overflow.Clear();
            foreach (var r in keep)
            {
                _overflow.Enqueue(r);
            }
        }
    }

    private void SafePause(IConsumer<Ignore, byte[]> consumer)
    {
        try
        {
            consumer.Pause(consumer.Assignment);
        }
        catch (KafkaException ex)
        {
            _logger.LogDebug("Pause failed: {Error}", ex.Error);
        }
    }

    private void SafeResume(IConsumer<Ignore, byte[]> consumer)
    {
        try
        {
            consumer.Resume(consumer.Assignment);
        }
        catch (KafkaException ex)
        {
            _logger.LogDebug("Resume failed: {Error}", ex.Error);
        }
    }

    public void Dispose() => Interlocked.Exchange(ref _consumer, null)?.Dispose();
}
