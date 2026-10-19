using System.Text;
using EventingC3.Etl.Core.Abstractions;
using EventingC3.Etl.Core.Domain;
using EventingC3.Etl.Core.Observability;
using EventingC3.Etl.Core.Pipeline;
using EventingC3.Etl.Core.Transform;
using EventingC3.Etl.Infrastructure.ClickHouse;
using EventingC3.Etl.TestSupport;
using Microsoft.Extensions.Logging.Abstractions;

namespace EventingC3.Etl.Tests;

public class BatchProcessorTests
{
    private static readonly DateTimeOffset Now = new(2026, 10, 19, 12, 0, 0, TimeSpan.Zero);

    private static readonly PipelineOptions FastRetries = new()
    {
        RetryInitialDelay = TimeSpan.FromMilliseconds(1),
        RetryMaxDelay = TimeSpan.FromMilliseconds(5),
    };

    private readonly RecordingSink _sink = new();
    private readonly RecordingDeadLetterSink _dlq = new();
    private readonly EtlMetrics _metrics = new();
    private readonly EtlHealth _health = new();
    private readonly List<long> _commits = new();
    private readonly PartitionLease _lease;

    public BatchProcessorTests()
    {
        _lease = new PartitionLease("topic", 0, (_, next) => _commits.Add(next));
    }

    private BatchProcessor Processor() => new(
        new TrackingEventParser(new TransformOptions(), HubDirectory.Default, new FixedTimeProvider(Now)),
        _sink, _dlq, FastRetries, _metrics, _health, new FixedTimeProvider(Now), NullLogger<BatchProcessor>.Instance);

    private static byte[] Valid(int i, Guid? id = null) => Encoding.UTF8.GetBytes(
        $$"""{"eventId":"{{id ?? Guid.NewGuid()}}","waybill":"RAM{{i:D9}}","eventType":"AT_HUB","occurredAt":"2026-10-19T09:00:00Z"}""");

    private RawBatch Batch(params byte[][] payloads)
    {
        var batch = new RawBatch(1, payloads.Length, Now);
        for (var i = 0; i < payloads.Length; i++)
        {
            batch.Add(new RawRecord(payloads[i], _lease, i));
        }

        return batch;
    }

    [Fact]
    public async Task Loads_valid_rows_dead_letters_rejects_and_commits()
    {
        var batch = Batch(Valid(1), "garbage"u8.ToArray(), Valid(2));
        await Processor().ProcessAsync(batch, CancellationToken.None);

        Assert.Equal(2, _sink.Rows.Count);
        var letter = Assert.Single(_dlq.Letters);
        Assert.Equal(RejectReason.MalformedJson, letter.Reason);
        Assert.Equal(1, letter.Offset);
        Assert.Equal("garbage", Encoding.UTF8.GetString(letter.Payload));
        Assert.Equal([3L], _commits);
        Assert.Equal(1, _metrics.Rejected(RejectReason.MalformedJson));
    }

    [Fact]
    public async Task Drops_in_batch_duplicates()
    {
        var id = Guid.NewGuid();
        await Processor().ProcessAsync(Batch(Valid(1, id), Valid(1, id), Valid(2)), CancellationToken.None);
        Assert.Equal(2, _sink.Rows.Count);
        Assert.Equal(1, _metrics.DuplicatesDropped);
        Assert.Equal([3L], _commits);
    }

    [Fact]
    public async Task Retries_transient_failures_with_the_same_token_until_success()
    {
        _sink.FailWhen = (_, attempt) => attempt <= 3 ? new SinkException(SinkErrorKind.Transient, "TOO_MANY_PARTS") : null;
        await Processor().ProcessAsync(Batch(Valid(1), Valid(2)), CancellationToken.None);

        Assert.Equal(4, _sink.Attempts);
        Assert.Equal(2, _sink.Rows.Count);
        Assert.Single(_sink.Tokens.Distinct());
        Assert.Equal(3, _metrics.SinkRetries);
        Assert.Equal(0, _health.ConsecutiveFailures);
    }

    [Fact]
    public async Task Unclassified_exceptions_are_retried_not_dropped()
    {
        _sink.FailWhen = (_, attempt) => attempt == 1 ? new HttpRequestException("connection reset") : null;
        await Processor().ProcessAsync(Batch(Valid(1)), CancellationToken.None);
        Assert.Single(_sink.Rows);
    }

    [Fact]
    public async Task Does_not_commit_while_the_sink_is_down()
    {
        _sink.FailWhen = (_, _) => new SinkException(SinkErrorKind.Fatal, "UNKNOWN_TABLE");
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(300));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => Processor().ProcessAsync(Batch(Valid(1)), cts.Token));

        Assert.Empty(_commits);
        Assert.Empty(_sink.Rows);
        Assert.False(_health.IsReady(FastRetries.UnhealthyAfterConsecutiveFailures));
    }

    [Fact]
    public async Task Bisects_to_isolate_rows_the_warehouse_refuses()
    {
        var poison = new[] { 13, 77 }.Select(i => $"RAM{i:D9}").ToHashSet();
        _sink.FailWhen = (rows, _) => rows.Any(r => poison.Contains(r.Waybill))
            ? new SinkException(SinkErrorKind.InvalidData, "Code: 27. CANNOT_PARSE_INPUT_ASSERTION_FAILED")
            : null;

        var payloads = Enumerable.Range(0, 100).Select(i => Valid(i)).ToArray();
        await Processor().ProcessAsync(Batch(payloads), CancellationToken.None);

        Assert.Equal(98, _sink.Rows.Count);
        Assert.Equal(98, _sink.Rows.Select(r => r.EventId).Distinct().Count());
        Assert.Equal([13L, 77L], _dlq.Letters.Select(l => l.Offset).Order());
        Assert.All(_dlq.Letters, l => Assert.Equal(RejectReason.SinkRejected, l.Reason));
        Assert.Equal([100L], _commits);
        Assert.True(_metrics.SinkBisections > 0);
    }

    [Fact]
    public async Task Retries_dead_letter_publishing_before_committing()
    {
        _dlq.FailuresRemaining = 2;
        await Processor().ProcessAsync(Batch("{}"u8.ToArray()), CancellationToken.None);
        Assert.Single(_dlq.Letters);
        Assert.Equal([1L], _commits);
    }

    [Fact]
    public void Insert_token_covers_the_exact_rows_not_just_the_source_ranges()
    {
        TrackingEvent E(Guid id) => new() { EventId = id, Waybill = "RAM123456", EventType = "AT_HUB", OccurredAtUnixMs = 1 };
        var a = E(Guid.NewGuid());
        var b = E(Guid.NewGuid());

        // Same ranges, same rows → same token (idempotent retry).
        Assert.Equal(BatchProcessor.InsertToken("t:0:0-1", [a, b], 0, 2), BatchProcessor.InsertToken("t:0:0-1", [a, b], 0, 2));
        // Same ranges, fewer rows (a worker skipped part of a revoked partition) → different token, so the complete
        // insert by the new owner is not discarded as a duplicate.
        Assert.NotEqual(BatchProcessor.InsertToken("t:0:0-1", [a, b], 0, 2), BatchProcessor.InsertToken("t:0:0-1", [a], 0, 1));
        // Bisected halves get distinct tokens.
        Assert.NotEqual(BatchProcessor.InsertToken("t:0:0-1", [a, b], 0, 1), BatchProcessor.InsertToken("t:0:0-1", [a, b], 1, 2));
    }

    [Fact]
    public async Task Skips_records_of_revoked_partitions()
    {
        var batch = Batch(Valid(1), Valid(2));
        _lease.Revoke();
        await Processor().ProcessAsync(batch, CancellationToken.None);
        Assert.Empty(_sink.Rows);
        Assert.Empty(_commits);
        Assert.Equal(2, _metrics.RevokedDropped);
    }
}

public class ClickHouseErrorClassifierTests
{
    [Theory]
    [InlineData("27", "", SinkErrorKind.InvalidData)]
    [InlineData(null, "Code: 33. DB::Exception: Cannot read all data", SinkErrorKind.InvalidData)]
    [InlineData("252", "", SinkErrorKind.Transient)]   // TOO_MANY_PARTS
    [InlineData("241", "", SinkErrorKind.Transient)]   // MEMORY_LIMIT_EXCEEDED
    [InlineData("319", "", SinkErrorKind.Transient)]   // UNKNOWN_STATUS_OF_INSERT
    [InlineData("60", "", SinkErrorKind.Fatal)]        // UNKNOWN_TABLE
    [InlineData("516", "", SinkErrorKind.Fatal)]       // AUTHENTICATION_FAILED
    public void Classifies_error_codes(string? header, string body, SinkErrorKind expected)
    {
        var code = ClickHouseErrorClassifier.ExtractCode(header, body);
        Assert.Equal(expected, ClickHouseErrorClassifier.Classify(System.Net.HttpStatusCode.InternalServerError, code));
    }

    [Theory]
    [InlineData(System.Net.HttpStatusCode.BadGateway, SinkErrorKind.Transient)]
    [InlineData(System.Net.HttpStatusCode.ServiceUnavailable, SinkErrorKind.Transient)]
    [InlineData(System.Net.HttpStatusCode.Forbidden, SinkErrorKind.Fatal)]
    public void Classifies_bare_http_statuses(System.Net.HttpStatusCode status, SinkErrorKind expected) =>
        Assert.Equal(expected, ClickHouseErrorClassifier.Classify(status, null));
}
