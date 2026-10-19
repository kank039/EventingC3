using System.Diagnostics;
using EventingC3.Etl.Core.Domain;
using EventingC3.Etl.Core.Serialization;
using EventingC3.Etl.Core.Transform;
using EventingC3.LoadGen;

// CPU cost of the Transform stage (parse + validate + normalise + enrich + RowBinary encode), isolated from I/O.
//
//   dotnet run -c Release --project bench/EventingC3.Etl.Benchmarks -- [records=1000000]

const long RecordsPerDayTarget = 1_000_000_000;
var count = args.Length > 0 ? int.Parse(args[0], System.Globalization.CultureInfo.InvariantCulture) : 1_000_000;

var generator = new TrackingEventGenerator(seed: 1, invalidRatio: 0.005, duplicateRatio: 0.01, missingIdRatio: 0.01);
var payloads = new byte[count][];
for (var i = 0; i < count; i++)
{
    payloads[i] = generator.Next(out _, out _);
}

var avgBytes = payloads.Average(p => (double)p.Length);
var parser = new TrackingEventParser(new TransformOptions(), HubDirectory.Default, TimeProvider.System);
Console.WriteLine($"{count:N0} payloads, avg {avgBytes:N0} bytes, {Environment.ProcessorCount} logical cores, .NET {Environment.Version}");

Run("parse+validate (1 thread)", 1, (from, to) =>
{
    var ok = 0;
    for (var i = from; i < to; i++)
    {
        ok += parser.Parse(payloads[i]).IsSuccess ? 1 : 0;
    }

    return ok;
});

Run("parse+validate+encode (1 thread)", 1, ParseAndEncode);
Run($"parse+validate+encode ({Environment.ProcessorCount} threads)", Environment.ProcessorCount, ParseAndEncode);

int ParseAndEncode(int from, int to)
{
    using var buffer = new PooledBuffer(to - from > 0 ? (to - from) * 200 : 1024);
    var writer = new RowBinaryWriter(buffer);
    var ok = 0;
    for (var i = from; i < to; i++)
    {
        if (parser.Parse(payloads[i]).Event is { } e)
        {
            TrackingEventEncoder.Write(writer, e, 0);
            ok++;
        }
    }

    return ok;
}

void Run(string name, int threads, Func<int, int, int> work)
{
    work(0, Math.Min(count, 50_000)); // warm-up (tiered JIT, PGO)
    GC.Collect();
    var gcBefore = GC.CollectionCount(0);
    var stopwatch = Stopwatch.StartNew();
    var chunk = (count + threads - 1) / threads;
    Parallel.For(0, threads, new ParallelOptions { MaxDegreeOfParallelism = threads },
        t => work(t * chunk, Math.Min(count, (t + 1) * chunk)));
    stopwatch.Stop();

    var rate = count / stopwatch.Elapsed.TotalSeconds;
    Console.WriteLine($"{name,-44} {rate,12:N0} rec/s  {stopwatch.Elapsed.TotalMicroseconds / count * threads,6:N2} µs/rec/thread  " +
                      $"{rate * 86_400 / RecordsPerDayTarget,6:N1}× 1B/day  gen0={GC.CollectionCount(0) - gcBefore}");
}
