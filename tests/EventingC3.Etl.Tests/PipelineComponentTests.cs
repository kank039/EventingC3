using System.Text;
using System.Threading.Channels;
using EventingC3.Etl.Core.Pipeline;

namespace EventingC3.Etl.Tests;

public class PartitionLeaseTests
{
    [Fact]
    public void Commit_point_only_advances_over_a_completed_prefix()
    {
        var commits = new List<long>();
        var lease = new PartitionLease("t", 0, (_, next) => commits.Add(next));

        var a = lease.Open(0);
        a.LastOffset = 9;
        var b = lease.Open(10);
        b.LastOffset = 19;
        var c = lease.Open(20);
        c.LastOffset = 29;

        c.Complete();
        b.Complete();
        Assert.Empty(commits); // segment a still in flight: committing 20 or 30 would skip it on a crash

        a.Complete();
        Assert.Equal([30L], commits);
        Assert.Equal(30, lease.CommittableOffset);
        Assert.Equal(0, lease.PendingSegments);
    }

    [Fact]
    public void Revoked_lease_stops_reporting_commits()
    {
        var commits = new List<long>();
        var lease = new PartitionLease("t", 0, (_, next) => commits.Add(next));
        var a = lease.Open(5);
        lease.Revoke();
        a.Complete();
        Assert.Empty(commits);
        Assert.True(lease.IsRevoked);
    }

    [Fact]
    public void Batch_tracks_segments_per_partition_and_derives_a_stable_token()
    {
        var l0 = new PartitionLease("t", 0, (_, _) => { });
        var l1 = new PartitionLease("t", 1, (_, _) => { });
        RawBatch Build()
        {
            var batch = new RawBatch(1, 4, DateTimeOffset.UtcNow);
            batch.Add(new RawRecord([1], l0, 100));
            batch.Add(new RawRecord([1], l1, 7));
            batch.Add(new RawRecord([1, 2], l0, 101));
            return batch;
        }

        var first = Build();
        Assert.Equal(2, first.Segments.Count());
        Assert.Equal(4, first.Bytes);
        Assert.Equal(Build().SourceFingerprint(), first.SourceFingerprint());

        var other = new RawBatch(2, 1, DateTimeOffset.UtcNow);
        other.Add(new RawRecord([1], l0, 102));
        Assert.NotEqual(first.SourceFingerprint(), other.SourceFingerprint());
    }
}

public class BatcherTests
{
    private static RawRecord Record(PartitionLease lease, long offset, int size = 10) => new(new byte[size], lease, offset);

    [Fact]
    public async Task Cuts_batches_by_record_count()
    {
        var batches = await RunBatcher(new PipelineOptions { MaxBatchRecords = 10, MaxBatchLatency = TimeSpan.FromMinutes(1) }, 25);
        Assert.Equal([10, 10, 5], batches.Select(b => b.Records.Count));
    }

    [Fact]
    public async Task Cuts_batches_by_bytes()
    {
        var batches = await RunBatcher(new PipelineOptions { MaxBatchRecords = 1000, MaxBatchBytes = 50, MaxBatchLatency = TimeSpan.FromMinutes(1) }, 12);
        Assert.Equal([5, 5, 2], batches.Select(b => b.Records.Count));
    }

    [Fact]
    public async Task Flushes_a_partial_batch_when_latency_budget_expires()
    {
        var options = new PipelineOptions { MaxBatchRecords = 1000, MaxBatchLatency = TimeSpan.FromMilliseconds(100) };
        var input = Channel.CreateUnbounded<RawRecord>();
        var output = Channel.CreateUnbounded<RawBatch>();
        var lease = new PartitionLease("t", 0, (_, _) => { });
        var run = new Batcher(options, TimeProvider.System).RunAsync(input.Reader, output.Writer, CancellationToken.None);

        input.Writer.TryWrite(Record(lease, 0));
        input.Writer.TryWrite(Record(lease, 1));

        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var batch = await output.Reader.ReadAsync(timeout.Token); // arrives without the input completing
        Assert.Equal(2, batch.Records.Count);

        input.Writer.Complete();
        await run;
    }

    [Fact]
    public async Task Preserves_per_partition_order_across_batches()
    {
        var batches = await RunBatcher(new PipelineOptions { MaxBatchRecords = 7 }, 100, partitions: 3);
        var all = batches.SelectMany(b => b.Records).GroupBy(r => r.Lease.Partition);
        foreach (var partition in all)
        {
            var offsets = partition.Select(r => r.Offset).ToList();
            Assert.Equal(offsets.Order(), offsets);
        }
    }

    private static async Task<List<RawBatch>> RunBatcher(PipelineOptions options, int records, int partitions = 1)
    {
        var input = Channel.CreateUnbounded<RawRecord>();
        var output = Channel.CreateUnbounded<RawBatch>();
        var leases = Enumerable.Range(0, partitions).Select(p => new PartitionLease("t", p, (_, _) => { })).ToArray();
        var offsets = new long[partitions];
        for (var i = 0; i < records; i++)
        {
            var p = i % partitions;
            input.Writer.TryWrite(Record(leases[p], offsets[p]++));
        }

        input.Writer.Complete();
        await new Batcher(options, TimeProvider.System).RunAsync(input.Reader, output.Writer, CancellationToken.None);
        var result = new List<RawBatch>();
        await foreach (var batch in output.Reader.ReadAllAsync())
        {
            result.Add(batch);
        }

        Assert.Equal(records, result.Sum(b => b.Records.Count));
        return result;
    }
}

public class FileLineReaderTests
{
    [Fact]
    public async Task Splits_lines_handling_crlf_blank_lines_and_missing_final_newline()
    {
        var lines = await Read("a\r\n\n  \nb\nc", maxLine: 1024);
        Assert.Equal([("a", 1L), ("b", 4L), ("c", 5L)], lines);
    }

    [Fact]
    public async Task Emits_the_head_of_an_oversized_line_once_and_resyncs()
    {
        var text = "first\n" + new string('x', 5000) + "\nlast\n";
        var lines = await Read(text, maxLine: 1000);
        Assert.Equal(3, lines.Count);
        Assert.Equal("first", lines[0].Line);
        Assert.True(lines[1].Line.Length > 1000);
        Assert.Equal(("last", 3L), lines[2]);
    }

    private static async Task<List<(string Line, long Number)>> Read(string text, int maxLine)
    {
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(text));
        var result = new List<(string, long)>();
        await foreach (var (line, number) in Infrastructure.Files.FileRecordSource.ReadLinesAsync(stream, maxLine, CancellationToken.None))
        {
            result.Add((Encoding.UTF8.GetString(line), number));
        }

        return result;
    }
}
