using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Text.Json;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using EventingC3.Etl.Core.Abstractions;
using EventingC3.Etl.Core.Domain;
using EventingC3.Etl.Core.Observability;
using EventingC3.Etl.Core.Pipeline;
using EventingC3.Etl.Core.Transform;
using EventingC3.Etl.Infrastructure.ClickHouse;
using EventingC3.LoadGen;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace EventingC3.Etl.IntegrationTests;

/// <summary>
/// Runs only when <c>ETL_IT=1</c>; expects ClickHouse on <c>ETL_IT_CLICKHOUSE</c> (default http://localhost:8123)
/// and a Kafka-compatible broker on <c>ETL_IT_KAFKA</c> (default localhost:9092). See docker-compose.yml.
/// </summary>
public sealed class IntegrationFactAttribute : FactAttribute
{
    public IntegrationFactAttribute()
    {
        if (Environment.GetEnvironmentVariable("ETL_IT") != "1")
        {
            Skip = "Integration tests disabled (set ETL_IT=1 with ClickHouse and Kafka running).";
        }
    }
}

public static class Env
{
    public static string ClickHouse => Environment.GetEnvironmentVariable("ETL_IT_CLICKHOUSE") ?? "http://localhost:8123";

    public static string Kafka => Environment.GetEnvironmentVariable("ETL_IT_KAFKA") ?? "localhost:9092";

    public static string Unique(string prefix) => $"{prefix}_{Guid.NewGuid():N}"[..(prefix.Length + 13)];
}

/// <summary>A throwaway ClickHouse database with the production schema applied.</summary>
public sealed class TestWarehouse : IAsyncDisposable
{
    private TestWarehouse(ClickHouseOptions options, ClickHouseSink sink)
    {
        Options = options;
        Sink = sink;
    }

    public ClickHouseOptions Options { get; }

    public ClickHouseSink Sink { get; }

    public static async Task<TestWarehouse> CreateAsync(string? endpoint = null)
    {
        var options = new ClickHouseOptions
        {
            Endpoint = endpoint ?? Env.ClickHouse,
            Database = Env.Unique("it"),
            RequestTimeout = TimeSpan.FromSeconds(30),
        };
        var sink = new ClickHouseSink(options, NullLogger<ClickHouseSink>.Instance);
        await sink.ApplySchemaAsync(CancellationToken.None);
        return new TestWarehouse(options, sink);
    }

    /// <summary>A sink for the same database through a different endpoint (e.g. a chaos proxy).</summary>
    public ClickHouseSink SinkVia(string endpoint) => new(new ClickHouseOptions
    {
        Endpoint = endpoint,
        Database = Options.Database,
        RequestTimeout = TimeSpan.FromSeconds(10),
    }, NullLogger<ClickHouseSink>.Instance);

    public async Task<long> ScalarAsync(string sql) =>
        long.Parse((await Sink.ExecuteAsync(sql, Options.Database)).Trim(), System.Globalization.CultureInfo.InvariantCulture);

    public async Task<List<JsonElement>> RowsAsync(string sql)
    {
        var body = await Sink.ExecuteAsync(sql + " FORMAT JSONEachRow", Options.Database);
        return body.Split('\n', StringSplitOptions.RemoveEmptyEntries).Select(l => JsonDocument.Parse(l).RootElement.Clone()).ToList();
    }

    public async ValueTask DisposeAsync()
    {
        try
        {
            await Sink.ExecuteAsync($"DROP DATABASE IF EXISTS `{Options.Database}`");
        }
        finally
        {
            Sink.Dispose();
        }
    }
}

/// <summary>Creates a topic and fills it with generated load.</summary>
public static class KafkaFixture
{
    public static async Task CreateTopicsAsync(string topic, int partitions)
    {
        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = Env.Kafka }).Build();
        await admin.CreateTopicsAsync(
        [
            new TopicSpecification { Name = topic, NumPartitions = partitions, ReplicationFactor = 1 },
            new TopicSpecification { Name = topic + ".dlq", NumPartitions = 1, ReplicationFactor = 1 },
        ]);
    }

    public static TrackingEventGenerator Produce(string topic, int count, int seed, double invalid = 0.02, double duplicates = 0.03, double missingId = 0.02)
    {
        var generator = new TrackingEventGenerator(seed, invalid, duplicates, missingId);
        using var producer = new ProducerBuilder<string?, byte[]>(new ProducerConfig
        {
            BootstrapServers = Env.Kafka,
            LingerMs = 10,
            CompressionType = CompressionType.Lz4,
            QueueBufferingMaxMessages = 500_000,
        }).Build();

        for (var i = 0; i < count; i++)
        {
            var payload = generator.Next(out _, out var key);
            while (true)
            {
                try
                {
                    producer.Produce(topic, new Message<string?, byte[]> { Key = key, Value = payload });
                    break;
                }
                catch (ProduceException<string?, byte[]> ex) when (ex.Error.Code == ErrorCode.Local_QueueFull)
                {
                    producer.Poll(TimeSpan.FromMilliseconds(20));
                }
            }
        }

        producer.Flush(TimeSpan.FromMinutes(2));
        return generator;
    }

    /// <summary>Total messages in a topic (sum of high watermarks).</summary>
    public static long MessageCount(string topic)
    {
        using var consumer = new ConsumerBuilder<Ignore, Ignore>(new ConsumerConfig { BootstrapServers = Env.Kafka, GroupId = "it-inspect" }).Build();
        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = Env.Kafka }).Build();
        var metadata = admin.GetMetadata(topic, TimeSpan.FromSeconds(10));
        return metadata.Topics[0].Partitions.Sum(p =>
            consumer.QueryWatermarkOffsets(new TopicPartition(topic, p.PartitionId), TimeSpan.FromSeconds(10)).High.Value);
    }

    /// <summary>Per-partition committed offset / high watermark, for diagnostics.</summary>
    public static string Describe(string topic, string groupId)
    {
        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = Env.Kafka }).Build();
        var partitions = admin.GetMetadata(topic, TimeSpan.FromSeconds(10)).Topics[0].Partitions
            .Select(p => new TopicPartition(topic, p.PartitionId)).OrderBy(p => p.Partition.Value).ToList();
        using var consumer = new ConsumerBuilder<Ignore, Ignore>(new ConsumerConfig { BootstrapServers = Env.Kafka, GroupId = groupId }).Build();
        var committed = consumer.Committed(partitions, TimeSpan.FromSeconds(10));
        return string.Join(" ", committed.Select(c =>
            $"p{c.Partition.Value}={c.Offset.Value}/{consumer.QueryWatermarkOffsets(c.TopicPartition, TimeSpan.FromSeconds(10)).High.Value}"));
    }

    /// <summary>Committed offsets for a group vs. high watermarks: true when the group has committed everything.</summary>
    public static bool FullyCommitted(string topic, string groupId)
    {
        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = Env.Kafka }).Build();
        var partitions = admin.GetMetadata(topic, TimeSpan.FromSeconds(10)).Topics[0].Partitions
            .Select(p => new TopicPartition(topic, p.PartitionId)).ToList();
        using var consumer = new ConsumerBuilder<Ignore, Ignore>(new ConsumerConfig { BootstrapServers = Env.Kafka, GroupId = groupId }).Build();
        var committed = consumer.Committed(partitions, TimeSpan.FromSeconds(10));
        return committed.All(c => c.Offset.Value == consumer.QueryWatermarkOffsets(c.TopicPartition, TimeSpan.FromSeconds(10)).High.Value);
    }
}

/// <summary>Builds the production pipeline graph around the given adapters.</summary>
public static class PipelineFactory
{
    public static readonly ILoggerFactory Logging = LoggerFactory.Create(b => b.AddSimpleConsole(o => o.SingleLine = true).SetMinimumLevel(
        Enum.TryParse<LogLevel>(Environment.GetEnvironmentVariable("ETL_IT_LOGLEVEL"), out var level) ? level : LogLevel.Warning));

    public static (EtlPipeline Pipeline, EtlMetrics Metrics) Build(IRecordSource source, IEventSink sink, IDeadLetterSink dlq, PipelineOptions? options = null)
    {
        options ??= new PipelineOptions
        {
            MaxBatchRecords = 20_000,
            MaxBatchLatency = TimeSpan.FromMilliseconds(500),
            MaxConcurrentBatches = 4,
            RecordBufferCapacity = 50_000,
            RetryInitialDelay = TimeSpan.FromMilliseconds(100),
            RetryMaxDelay = TimeSpan.FromSeconds(1),
            ShutdownGracePeriod = TimeSpan.FromSeconds(20),
        };
        var metrics = new EtlMetrics();
        var health = new EtlHealth();
        var time = TimeProvider.System;
        var parser = new TrackingEventParser(new TransformOptions(), HubDirectory.Default, time);
        var processor = new BatchProcessor(parser, sink, dlq, options, metrics, health, time, Logging.CreateLogger<BatchProcessor>());
        return (new EtlPipeline(source, new Batcher(options, time), processor, options, health, Logging.CreateLogger<EtlPipeline>()), metrics);
    }

    /// <summary>Polls until <paramref name="condition"/> holds or the timeout elapses.</summary>
    public static async Task WaitUntilAsync(Func<Task<bool>> condition, TimeSpan timeout, string what)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (await condition())
            {
                return;
            }

            await Task.Delay(250);
        }

        throw new TimeoutException($"Timed out waiting for {what}");
    }
}

/// <summary>
/// A TCP proxy that can be "unplugged": while disabled it kills open connections and refuses new ones,
/// simulating a ClickHouse node crash / network partition.
/// </summary>
public sealed class ChaosProxy : IAsyncDisposable
{
    private readonly TcpListener _listener;
    private readonly IPEndPoint _target;
    private readonly ConcurrentDictionary<TcpClient, byte> _connections = new();
    private readonly CancellationTokenSource _stop = new();
    private readonly Task _acceptLoop;
    private volatile bool _enabled = true;

    public ChaosProxy(IPEndPoint target)
    {
        _target = target;
        _listener = new TcpListener(IPAddress.Loopback, 0);
        _listener.Start();
        _acceptLoop = AcceptLoopAsync();
    }

    public string Endpoint => $"http://127.0.0.1:{((IPEndPoint)_listener.LocalEndpoint).Port}";

    public bool Enabled
    {
        get => _enabled;
        set
        {
            _enabled = value;
            if (!value)
            {
                foreach (var client in _connections.Keys)
                {
                    client.Dispose();
                }
            }
        }
    }

    private async Task AcceptLoopAsync()
    {
        while (!_stop.IsCancellationRequested)
        {
            TcpClient client;
            try
            {
                client = await _listener.AcceptTcpClientAsync(_stop.Token);
            }
            catch
            {
                return;
            }

            if (!_enabled)
            {
                client.Client.LingerState = new LingerOption(true, 0); // RST, like a dead node
                client.Dispose();
                continue;
            }

            _ = PipeAsync(client);
        }
    }

    private async Task PipeAsync(TcpClient client)
    {
        using var upstream = new TcpClient();
        _connections[client] = 0;
        _connections[upstream] = 0;
        try
        {
            await upstream.ConnectAsync(_target);
            var a = client.GetStream().CopyToAsync(upstream.GetStream());
            var b = upstream.GetStream().CopyToAsync(client.GetStream());
            await Task.WhenAny(a, b);
        }
        catch
        {
            // Connection killed by chaos — expected.
        }
        finally
        {
            _connections.TryRemove(client, out _);
            _connections.TryRemove(upstream, out _);
            client.Dispose();
        }
    }

    public async ValueTask DisposeAsync()
    {
        _stop.Cancel();
        _listener.Stop();
        Enabled = false;
        await _acceptLoop;
    }
}
