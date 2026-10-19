using System.Diagnostics;
using System.Globalization;
using System.IO.Compression;
using System.Text;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using EventingC3.LoadGen;

// Synthetic tracking-event producer for load tests and benchmarks.
//
//   dotnet run -c Release --project tools/EventingC3.LoadGen -- \
//       --bootstrap localhost:9092 --topic tracking-events --partitions 12 --create-topic \
//       --count 10000000 --invalid 0.01 --duplicates 0.02 --missing-id 0.01 --summary summary.json
//
//   File mode (bulk/backfill testing): --out ./data/inbox --files 4 --count 1000000

var opts = Args.Parse(args);
var generator = new TrackingEventGenerator(opts.Int("seed", 42), opts.Double("invalid", 0.01), opts.Double("duplicates", 0.02),
    opts.Double("missing-id", 0.01), opts.Int("waybills", 2_000_000));
var count = opts.Long("count", 1_000_000);
var stopwatch = Stopwatch.StartNew();

if (opts.Has("out"))
{
    await WriteFilesAsync(opts.String("out", "./data/inbox"), opts.Int("files", 1), count, generator);
}
else
{
    await ProduceAsync(opts, count, generator);
}

stopwatch.Stop();
var summary = generator.SummaryJson(count, stopwatch.Elapsed);
Console.WriteLine(summary);
if (opts.Has("summary"))
{
    await File.WriteAllTextAsync(opts.String("summary", "summary.json"), summary);
}

static async Task ProduceAsync(Args opts, long count, TrackingEventGenerator generator)
{
    var bootstrap = opts.String("bootstrap", "localhost:9092");
    var topic = opts.String("topic", "tracking-events");

    if (opts.Has("create-topic"))
    {
        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrap }).Build();
        try
        {
            await admin.CreateTopicsAsync(
            [
                new TopicSpecification { Name = topic, NumPartitions = opts.Int("partitions", 12), ReplicationFactor = 1 },
                new TopicSpecification { Name = topic + ".dlq", NumPartitions = 3, ReplicationFactor = 1 },
            ]);
            Console.Error.WriteLine($"Created topics {topic} and {topic}.dlq");
        }
        catch (CreateTopicsException ex) when (ex.Results.All(r => r.Error.Code is ErrorCode.TopicAlreadyExists or ErrorCode.NoError))
        {
            Console.Error.WriteLine("Topics already exist");
        }
    }

    var config = new ProducerConfig
    {
        BootstrapServers = bootstrap,
        Acks = opts.String("acks", "1") == "all" ? Acks.All : Acks.Leader,
        LingerMs = 20,
        BatchSize = 1024 * 1024,
        CompressionType = CompressionType.Lz4,
        QueueBufferingMaxMessages = 1_000_000,
        QueueBufferingMaxKbytes = 1024 * 1024,
        EnableIdempotence = false,
    };

    long failed = 0;
    using var producer = new ProducerBuilder<string?, byte[]>(config).Build();
    var rate = opts.Long("rate", 0);
    var started = Stopwatch.StartNew();
    for (long i = 0; i < count; i++)
    {
        var payload = generator.Next(out _, out var key);
        var message = new Message<string?, byte[]> { Key = key, Value = payload };
        while (true)
        {
            try
            {
                producer.Produce(topic, message, report =>
                {
                    if (report.Error.IsError)
                    {
                        Interlocked.Increment(ref failed);
                    }
                });
                break;
            }
            catch (ProduceException<string?, byte[]> ex) when (ex.Error.Code == ErrorCode.Local_QueueFull)
            {
                producer.Poll(TimeSpan.FromMilliseconds(50));
            }
        }

        if (rate > 0 && i % 1000 == 0)
        {
            var due = TimeSpan.FromSeconds(i / (double)rate);
            if (due > started.Elapsed)
            {
                Thread.Sleep(due - started.Elapsed);
            }
        }

        if (i > 0 && i % 1_000_000 == 0)
        {
            Console.Error.WriteLine($"{i:N0} produced ({i / started.Elapsed.TotalSeconds:N0}/s)");
        }
    }

    producer.Flush(TimeSpan.FromMinutes(5));
    if (failed > 0)
    {
        Console.Error.WriteLine($"WARNING: {failed} messages failed delivery");
        Environment.ExitCode = 2;
    }
}

static async Task WriteFilesAsync(string directory, int files, long count, TrackingEventGenerator generator)
{
    Directory.CreateDirectory(directory);
    var perFile = (count + files - 1) / files;
    long written = 0;
    for (var f = 0; f < files && written < count; f++)
    {
        var path = Path.Combine(directory, $"tracking-{DateTime.UtcNow:yyyyMMddHHmmss}-{f:D4}.ndjson.gz");
        var temp = path + ".tmp";
        await using (var file = File.Create(temp))
        await using (var gzip = new GZipStream(file, CompressionLevel.Fastest))
        {
            for (long i = 0; i < perFile && written < count; i++, written++)
            {
                var payload = generator.Next(out _, out _);
                // Payloads containing raw newlines would split the record; JSON writers escape them, invalid samples have none.
                gzip.Write(payload);
                gzip.WriteByte((byte)'\n');
            }
        }

        File.Move(temp, path); // atomic publish: the worker never sees a half-written file
        Console.Error.WriteLine($"Wrote {path}");
    }
}

/// <summary>Minimal <c>--name value</c> / <c>--flag</c> parser.</summary>
internal sealed class Args
{
    private readonly Dictionary<string, string> _values = new(StringComparer.OrdinalIgnoreCase);

    public static Args Parse(string[] args)
    {
        var result = new Args();
        for (var i = 0; i < args.Length; i++)
        {
            if (!args[i].StartsWith("--", StringComparison.Ordinal))
            {
                continue;
            }

            var name = args[i][2..];
            var hasValue = i + 1 < args.Length && !args[i + 1].StartsWith("--", StringComparison.Ordinal);
            result._values[name] = hasValue ? args[++i] : "true";
        }

        return result;
    }

    public bool Has(string name) => _values.ContainsKey(name);
    public string String(string name, string fallback) => _values.GetValueOrDefault(name, fallback);
    public int Int(string name, int fallback) => _values.TryGetValue(name, out var v) ? int.Parse(v, CultureInfo.InvariantCulture) : fallback;
    public long Long(string name, long fallback) => _values.TryGetValue(name, out var v) ? long.Parse(v, CultureInfo.InvariantCulture) : fallback;
    public double Double(string name, double fallback) => _values.TryGetValue(name, out var v) ? double.Parse(v, CultureInfo.InvariantCulture) : fallback;
}
