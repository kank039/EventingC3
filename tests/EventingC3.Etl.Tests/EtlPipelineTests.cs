using EventingC3.Etl.Core.Abstractions;
using EventingC3.Etl.Core.Domain;
using EventingC3.Etl.Core.Observability;
using EventingC3.Etl.Core.Pipeline;
using EventingC3.Etl.Core.Transform;
using EventingC3.LoadGen;
using EventingC3.Etl.TestSupport;
using Microsoft.Extensions.Logging.Abstractions;

namespace EventingC3.Etl.Tests;

/// <summary>Whole-pipeline behaviour with in-memory source and sinks, including a chaotic sink.</summary>
public class EtlPipelineTests
{
    private static readonly DateTimeOffset Now = DateTimeOffset.UtcNow;

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Loads_every_valid_event_exactly_once_and_commits_every_partition(bool chaos)
    {
        var generator = new TrackingEventGenerator(seed: 3, invalidRatio: 0.02, duplicateRatio: 0.03, missingIdRatio: 0.02, now: Now);
        var payloads = Enumerable.Range(0, 60_000).Select(i => generator.Next(out _, out _)).ToList();
        var source = new InMemorySource(payloads, partitions: 8);
        var sink = new RecordingSink();
        if (chaos)
        {
            // ~30% of inserts fail transiently, half of those *after* the write landed (lost ack) —
            // the classic duplicate-producing failure that the deduplication token must absorb.
            var random = new Random(11);
            double Roll()
            {
                lock (random)
                {
                    return random.NextDouble();
                }
            }

            sink.FailWhen = (_, _) => Roll() < 0.15 ? new SinkException(SinkErrorKind.Transient, "connection refused") : null;
            sink.FailAfterWriteWhen = _ => Roll() < 0.15 ? new SinkException(SinkErrorKind.Transient, "timeout after write") : null;
        }

        var dlq = new RecordingDeadLetterSink();
        var metrics = new EtlMetrics();
        var options = new PipelineOptions
        {
            MaxBatchRecords = 2_500,
            MaxConcurrentBatches = 4,
            MaxBatchLatency = TimeSpan.FromMilliseconds(50),
            RecordBufferCapacity = 5_000,
            RetryInitialDelay = TimeSpan.FromMilliseconds(1),
            RetryMaxDelay = TimeSpan.FromMilliseconds(10),
        };
        var pipeline = BuildPipeline(source, sink, dlq, metrics, options);

        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        await pipeline.RunAsync(timeout.Token);

        // Every valid event is present. Rows beyond that can only be producer duplicates that landed in different
        // batches (collapsed by ReplacingMergeTree in ClickHouse) — never retry duplicates, which the token absorbs.
        var distinct = sink.Rows.Select(r => r.EventId).Distinct().LongCount();
        Assert.Equal(generator.ExpectedUniqueEvents, distinct);
        Assert.InRange(sink.Rows.Count - distinct, 0, generator.Duplicates);
        Assert.Equal(generator.Invalid, dlq.Letters.Count);

        // Every record accounted for exactly once: loaded + rejected + in-batch duplicates == read.
        Assert.Equal(payloads.Count, metrics.RecordsLoaded + metrics.RecordsRejected + metrics.DuplicatesDropped);

        // Every partition fully committed.
        for (var p = 0; p < 8; p++)
        {
            Assert.Equal(source.ExpectedNextOffset(p), source.Committed[p]);
        }
    }

    [Fact]
    public async Task Stops_gracefully_and_commits_only_loaded_work()
    {
        var generator = new TrackingEventGenerator(seed: 5, invalidRatio: 0, duplicateRatio: 0, missingIdRatio: 0, now: Now);
        var payloads = Enumerable.Range(0, 200_000).Select(i => generator.Next(out _, out _)).ToList();
        var source = new InMemorySource(payloads, partitions: 4);
        var sink = new RecordingSink();
        var options = new PipelineOptions { MaxBatchRecords = 1_000, MaxConcurrentBatches = 2, RecordBufferCapacity = 2_000 };
        var pipeline = BuildPipeline(source, sink, new RecordingDeadLetterSink(), new EtlMetrics(), options);

        using var stop = new CancellationTokenSource(TimeSpan.FromMilliseconds(200));
        await pipeline.RunAsync(stop.Token);

        // Whatever was committed must be loaded: no partition's commit point may run ahead of the warehouse.
        var loaded = sink.Rows.GroupBy(r => r.SourcePartition).ToDictionary(g => g.Key, g => g.Select(r => r.SourceOffset).ToHashSet());
        foreach (var (partition, next) in source.Committed)
        {
            for (long offset = 0; offset < next; offset++)
            {
                Assert.Contains(offset, loaded[partition]);
            }
        }
    }

    [Fact]
    public async Task A_faulting_stage_fails_the_pipeline_instead_of_hanging()
    {
        var source = new InMemorySource([[1]], partitions: 1);
        var pipeline = new EtlPipeline(
            source,
            new Batcher(new PipelineOptions(), TimeProvider.System),
            null!, // a processor bug: NullReferenceException on first batch
            new PipelineOptions { MaxBatchLatency = TimeSpan.FromMilliseconds(10) },
            new EtlHealth(),
            NullLogger<EtlPipeline>.Instance);

        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        await Assert.ThrowsAsync<AggregateException>(() => pipeline.RunAsync(timeout.Token));
        Assert.False(timeout.IsCancellationRequested);
    }

    private static EtlPipeline BuildPipeline(IRecordSource source, IEventSink sink, IDeadLetterSink dlq, EtlMetrics metrics, PipelineOptions options)
    {
        var time = TimeProvider.System;
        var parser = new TrackingEventParser(new TransformOptions(), HubDirectory.Default, time);
        var health = new EtlHealth();
        var processor = new BatchProcessor(parser, sink, dlq, options, metrics, health, time, NullLogger<BatchProcessor>.Instance);
        return new EtlPipeline(source, new Batcher(options, time), processor, options, health, NullLogger<EtlPipeline>.Instance);
    }
}
