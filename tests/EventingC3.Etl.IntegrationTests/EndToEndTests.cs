using System.IO.Compression;
using System.Net;
using System.Text.Json;
using System.Threading.Channels;
using Confluent.Kafka;
using EventingC3.Etl.Core.Abstractions;
using EventingC3.Etl.Core.Observability;
using EventingC3.Etl.Core.Pipeline;
using EventingC3.Etl.Infrastructure.Files;
using EventingC3.Etl.Infrastructure.Kafka;
using EventingC3.LoadGen;
using Microsoft.Extensions.Logging;
using Xunit.Abstractions;

namespace EventingC3.Etl.IntegrationTests;

/// <summary>
/// Kafka → pipeline → ClickHouse with the real adapters, under the failure modes the design must survive.
/// Every test asserts the same invariant: the warehouse ends with exactly the set of valid events
/// (no loss, duplicates collapse), every invalid record is in the DLQ, and offsets are fully committed.
/// </summary>
public class EndToEndTests(ITestOutputHelper output)
{
    private const int Partitions = 6;

    private static KafkaSourceOptions SourceOptions(string topic, string group) => new()
    {
        BootstrapServers = Env.Kafka,
        Topic = topic,
        GroupId = group,
        CommitInterval = TimeSpan.FromMilliseconds(500),
        ConsumerConfig = { ["session.timeout.ms"] = "10000", ["heartbeat.interval.ms"] = "1000" },
    };

    private static KafkaRecordSource Source(string topic, string group, EtlMetrics? metrics = null) =>
        new(SourceOptions(topic, group), metrics ?? new EtlMetrics(), PipelineFactory.Logging.CreateLogger<KafkaRecordSource>());

    private static KafkaDeadLetterSink DeadLetters(string topic) =>
        new(new KafkaDeadLetterOptions { Topic = topic + ".dlq" }, SourceOptions(topic, "unused"));

    private async Task AssertExactOutcomeAsync(TestWarehouse warehouse, TrackingEventGenerator generator, string topic, string group)
    {
        var unique = await warehouse.ScalarAsync("SELECT uniqExact(event_id) FROM tracking_events");
        var final = await warehouse.ScalarAsync("SELECT count() FROM tracking_events FINAL");
        var raw = await warehouse.ScalarAsync("SELECT count() FROM tracking_events");
        var dlq = KafkaFixture.MessageCount(topic + ".dlq");
        output.WriteLine($"expected={generator.ExpectedUniqueEvents} unique={unique} final={final} raw={raw} dlq={dlq} invalid={generator.Invalid} dupes={generator.Duplicates}");

        Assert.Equal(generator.ExpectedUniqueEvents, unique);
        Assert.Equal(generator.ExpectedUniqueEvents, final);
        Assert.True(dlq >= generator.Invalid, $"DLQ has {dlq}, expected at least {generator.Invalid}");
        Assert.True(KafkaFixture.FullyCommitted(topic, group), "consumer group has uncommitted offsets");
    }

    private static Func<Task<bool>> Loaded(TestWarehouse warehouse, TrackingEventGenerator generator) =>
        async () => await warehouse.ScalarAsync("SELECT uniqExact(event_id) FROM tracking_events") >= generator.ExpectedUniqueEvents;

    [IntegrationFact]
    public async Task Streams_a_topic_into_clickhouse_exactly()
    {
        var topic = Env.Unique("it_topic");
        var group = Env.Unique("it_group");
        await KafkaFixture.CreateTopicsAsync(topic, Partitions);
        var generator = KafkaFixture.Produce(topic, 300_000, seed: 1);
        await using var warehouse = await TestWarehouse.CreateAsync();

        var (pipeline, metrics) = PipelineFactory.Build(Source(topic, group), warehouse.Sink, DeadLetters(topic));
        using var stop = new CancellationTokenSource();
        var run = pipeline.RunAsync(stop.Token);

        await PipelineFactory.WaitUntilAsync(Loaded(warehouse, generator), TimeSpan.FromMinutes(3), "all events loaded");
        await PipelineFactory.WaitUntilAsync(() => Task.FromResult(metrics.RecordsLoaded + metrics.RecordsRejected + metrics.DuplicatesDropped >= 300_000),
            TimeSpan.FromMinutes(1), "all records processed");
        stop.Cancel();
        await run;

        Assert.Equal(generator.Invalid, metrics.RecordsRejected);
        await AssertExactOutcomeAsync(warehouse, generator, topic, group);
        Assert.Equal(generator.Invalid, KafkaFixture.MessageCount(topic + ".dlq"));
    }

    [IntegrationFact]
    public async Task Survives_a_worker_crash_without_loss_or_duplicates()
    {
        var topic = Env.Unique("it_topic");
        var group = Env.Unique("it_group");
        await KafkaFixture.CreateTopicsAsync(topic, Partitions);
        var generator = KafkaFixture.Produce(topic, 300_000, seed: 2);
        await using var warehouse = await TestWarehouse.CreateAsync();

        // Run 1: crash part-way — no graceful drain, no final commit (process killed).
        var crashing = new CrashingSource(Source(topic, group));
        var (first, firstMetrics) = PipelineFactory.Build(crashing, warehouse.Sink, DeadLetters(topic));
        using (var crash = new CancellationTokenSource())
        {
            var run = first.RunAsync(crash.Token);
            await PipelineFactory.WaitUntilAsync(() => Task.FromResult(firstMetrics.RecordsLoaded > 60_000), TimeSpan.FromMinutes(2), "partial load");
            crashing.Crash();
            crash.Cancel();
            await run;
        }

        var afterCrash = await warehouse.ScalarAsync("SELECT uniqExact(event_id) FROM tracking_events");
        output.WriteLine($"after crash: {afterCrash} unique events loaded");
        Assert.True(afterCrash < generator.ExpectedUniqueEvents);

        // Run 2: a replacement worker resumes from the last committed offsets.
        var (second, _) = PipelineFactory.Build(Source(topic, group), warehouse.Sink, DeadLetters(topic));
        using var stop = new CancellationTokenSource();
        var resumed = second.RunAsync(stop.Token);
        await PipelineFactory.WaitUntilAsync(Loaded(warehouse, generator), TimeSpan.FromMinutes(3), "all events loaded after restart");
        await PipelineFactory.WaitUntilAsync(() => Task.FromResult(KafkaFixture.FullyCommitted(topic, group)), TimeSpan.FromMinutes(1), "offsets committed");
        stop.Cancel();
        await resumed;

        await AssertExactOutcomeAsync(warehouse, generator, topic, group);
    }

    [IntegrationFact]
    public async Task Rides_out_a_clickhouse_outage()
    {
        var topic = Env.Unique("it_topic");
        var group = Env.Unique("it_group");
        await KafkaFixture.CreateTopicsAsync(topic, Partitions);
        var generator = KafkaFixture.Produce(topic, 300_000, seed: 3);
        await using var warehouse = await TestWarehouse.CreateAsync();
        var target = new Uri(Env.ClickHouse);
        await using var proxy = new ChaosProxy(new IPEndPoint(IPAddress.Loopback, target.Port));
        using var sink = warehouse.SinkVia(proxy.Endpoint);

        var (pipeline, metrics) = PipelineFactory.Build(Source(topic, group), sink, DeadLetters(topic));
        using var stop = new CancellationTokenSource();
        var run = pipeline.RunAsync(stop.Token);

        await PipelineFactory.WaitUntilAsync(() => Task.FromResult(metrics.RecordsLoaded > 30_000), TimeSpan.FromMinutes(2), "partial load");
        proxy.Enabled = false; // ClickHouse "goes down": in-flight inserts are cut, new connections refused
        var loadedAtOutage = metrics.RecordsLoaded;
        await Task.Delay(TimeSpan.FromSeconds(8));
        output.WriteLine($"during outage: retries={metrics.SinkRetries}, loaded {loadedAtOutage} -> {metrics.RecordsLoaded}");
        Assert.True(metrics.SinkRetries > 0);
        proxy.Enabled = true;

        await PipelineFactory.WaitUntilAsync(Loaded(warehouse, generator), TimeSpan.FromMinutes(3), "all events loaded after outage");
        await PipelineFactory.WaitUntilAsync(() => Task.FromResult(KafkaFixture.FullyCommitted(topic, group)), TimeSpan.FromMinutes(1), "offsets committed");
        stop.Cancel();
        await run;

        await AssertExactOutcomeAsync(warehouse, generator, topic, group);
    }

    [IntegrationFact]
    public async Task Rebalances_across_workers_without_loss()
    {
        var topic = Env.Unique("it_topic");
        var group = Env.Unique("it_group");
        await KafkaFixture.CreateTopicsAsync(topic, Partitions);
        var generator = KafkaFixture.Produce(topic, 300_000, seed: 4);
        await using var warehouse = await TestWarehouse.CreateAsync();

        // A throttled warehouse keeps both workers under back-pressure (partitions paused) while partitions move —
        // the hardest case for pause/resume bookkeeping.
        var slow = new ThrottledSink(warehouse.Sink, TimeSpan.FromMilliseconds(300));
        var options = new PipelineOptions
        {
            MaxBatchRecords = 10_000,
            MaxBatchLatency = TimeSpan.FromMilliseconds(500),
            MaxConcurrentBatches = 1,
            RecordBufferCapacity = 10_000,
            RetryInitialDelay = TimeSpan.FromMilliseconds(100),
            RetryMaxDelay = TimeSpan.FromSeconds(1),
            ShutdownGracePeriod = TimeSpan.FromSeconds(20),
        };

        // Scale out: A starts alone, then B joins and takes over some partitions.
        var (workerA, metricsA) = PipelineFactory.Build(Source(topic, group), slow, DeadLetters(topic), options);
        using var stopA = new CancellationTokenSource();
        var runA = workerA.RunAsync(stopA.Token);
        await PipelineFactory.WaitUntilAsync(() => Task.FromResult(metricsA.RecordsLoaded > 20_000), TimeSpan.FromMinutes(1), "worker A loading");

        var (workerB, metricsB) = PipelineFactory.Build(Source(topic, group), slow, DeadLetters(topic), options);
        using var stopB = new CancellationTokenSource();
        var runB = workerB.RunAsync(stopB.Token);
        await PipelineFactory.WaitUntilAsync(() => Task.FromResult(metricsB.RecordsLoaded > 20_000), TimeSpan.FromMinutes(1), "worker B loading");

        // Scale in: A leaves mid-stream; B must pick up A's partitions (including ones it had paused before).
        stopA.Cancel();
        await runA;

        try
        {
            await PipelineFactory.WaitUntilAsync(Loaded(warehouse, generator), TimeSpan.FromMinutes(2), "all events loaded after rebalance");
        }
        catch (TimeoutException)
        {
            output.WriteLine($"DIAG A read={metricsA.RecordsRead} loaded={metricsA.RecordsLoaded} revoked={metricsA.RevokedDropped}; " +
                             $"B read={metricsB.RecordsRead} loaded={metricsB.RecordsLoaded} revoked={metricsB.RevokedDropped} inflight={metricsB.InflightBatches}");
            output.WriteLine("DIAG " + KafkaFixture.Describe(topic, group));
            var perPartition = await warehouse.RowsAsync("SELECT source_partition, count() c, uniqExact(event_id) u, max(source_offset) mx FROM tracking_events GROUP BY source_partition ORDER BY source_partition");
            output.WriteLine("DIAG ch " + string.Join(" | ", perPartition.Select(r => r.ToString())));
            throw;
        }
        await PipelineFactory.WaitUntilAsync(() => Task.FromResult(KafkaFixture.FullyCommitted(topic, group)), TimeSpan.FromMinutes(1), "offsets committed");
        stopB.Cancel();
        await runB;

        output.WriteLine($"worker A loaded {metricsA.RecordsLoaded}, worker B loaded {metricsB.RecordsLoaded}");
        await AssertExactOutcomeAsync(warehouse, generator, topic, group);
    }

    private sealed class ThrottledSink(IEventSink inner, TimeSpan delay) : IEventSink
    {
        public async Task InsertAsync(ReadOnlyMemory<byte> rows, int rowCount, string deduplicationToken, CancellationToken cancellationToken)
        {
            await Task.Delay(delay, cancellationToken);
            await inner.InsertAsync(rows, rowCount, deduplicationToken, cancellationToken);
        }
    }

    [IntegrationFact]
    public async Task Backfills_gzip_ndjson_files()
    {
        var root = Path.Combine(Path.GetTempPath(), Env.Unique("it_files"));
        var inbox = Path.Combine(root, "inbox");
        Directory.CreateDirectory(inbox);
        var generator = new TrackingEventGenerator(5, 0.02, 0.03, 0.02);
        var blankLines = 0; // empty payloads become blank lines, which a file source skips rather than dead-letters
        for (var f = 0; f < 3; f++)
        {
            await using var file = File.Create(Path.Combine(inbox, $"part-{f}.ndjson.gz"));
            await using var gzip = new GZipStream(file, CompressionLevel.Fastest);
            for (var i = 0; i < 40_000; i++)
            {
                var payload = generator.Next(out _, out _);
                blankLines += payload.Length == 0 ? 1 : 0;
                gzip.Write(payload);
                gzip.WriteByte((byte)'\n');
            }
        }

        await using var warehouse = await TestWarehouse.CreateAsync();
        var metrics = new EtlMetrics();
        var source = new FileRecordSource(
            new FileSourceOptions { InputDirectory = inbox, ProcessedDirectory = Path.Combine(root, "done"), StopWhenIdle = true },
            metrics, PipelineFactory.Logging.CreateLogger<FileRecordSource>());
        var dlqDir = Path.Combine(root, "dlq");
        var (pipeline, pipelineMetrics) = PipelineFactory.Build(source, warehouse.Sink, new FileDeadLetterSink(new FileDeadLetterOptions { Directory = dlqDir }));

        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        await pipeline.RunAsync(timeout.Token); // bounded source: returns when everything is loaded
        Assert.False(timeout.IsCancellationRequested);

        await PipelineFactory.WaitUntilAsync(() => Task.FromResult(Directory.GetFiles(Path.Combine(root, "done")).Length == 3),
            TimeSpan.FromSeconds(10), "files moved to processed");
        Assert.Empty(Directory.GetFiles(inbox));
        Assert.Equal(generator.ExpectedUniqueEvents, await warehouse.ScalarAsync("SELECT count() FROM tracking_events FINAL"));

        var dlqLines = Directory.GetFiles(dlqDir).SelectMany(File.ReadLines).ToList();
        Assert.Equal(generator.Invalid - blankLines, dlqLines.Count);
        Assert.All(dlqLines, l => Assert.False(string.IsNullOrEmpty(JsonDocument.Parse(l).RootElement.GetProperty("reason").GetString())));
        Directory.Delete(root, recursive: true);
    }

    /// <summary>Simulates a hard process kill: on completion the consumer is dropped without committing or leaving the group.</summary>
    private sealed class CrashingSource(KafkaRecordSource inner) : IRecordSource
    {
        private volatile bool _crashed;

        public void Crash() => _crashed = true;

        public Task RunAsync(ChannelWriter<RawRecord> output, CancellationToken stopReading) => inner.RunAsync(output, stopReading);

        public Task CompleteAsync(CancellationToken cancellationToken)
        {
            if (_crashed)
            {
                inner.Dispose(); // no final commit, no group leave: partitions are reassigned after session timeout
                return Task.CompletedTask;
            }

            return inner.CompleteAsync(cancellationToken);
        }
    }
}
