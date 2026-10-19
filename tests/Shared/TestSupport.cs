using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Text;
using System.Threading.Channels;
using EventingC3.Etl.Core.Abstractions;
using EventingC3.Etl.Core.Pipeline;

namespace EventingC3.Etl.TestSupport;

/// <summary>A clock frozen at a fixed instant (timers still run on real time).</summary>
public sealed class FixedTimeProvider(DateTimeOffset now) : TimeProvider
{
    public DateTimeOffset Now { get; set; } = now;

    public override DateTimeOffset GetUtcNow() => Now;
}

/// <summary>Decoded row, for assertions.</summary>
public sealed record DecodedRow(
    Guid EventId,
    string Waybill,
    string EventType,
    long OccurredAtMs,
    string HubCode,
    string Region,
    string CourierId,
    double? Latitude,
    double? Longitude,
    string SourceSystem,
    ushort SchemaVersion,
    List<KeyValuePair<string, string>> Attributes,
    long IngestedAtMs,
    int SourcePartition,
    long SourceOffset);

/// <summary>Reads back the RowBinary written by TrackingEventEncoder.</summary>
public static class RowBinaryDecoder
{
    public static List<DecodedRow> Decode(ReadOnlySpan<byte> data)
    {
        var rows = new List<DecodedRow>();
        var pos = 0;
        while (pos < data.Length)
        {
            var id = ReadUuid(data, ref pos);
            var waybill = ReadString(data, ref pos);
            var type = ReadString(data, ref pos);
            var occurred = ReadInt64(data, ref pos);
            var hub = ReadString(data, ref pos);
            var region = ReadString(data, ref pos);
            var courier = ReadString(data, ref pos);
            var lat = ReadNullableDouble(data, ref pos);
            var lon = ReadNullableDouble(data, ref pos);
            var source = ReadString(data, ref pos);
            var schema = BinaryPrimitives.ReadUInt16LittleEndian(data[pos..]);
            pos += 2;
            var mapCount = (int)ReadVarUInt(data, ref pos);
            var attributes = new List<KeyValuePair<string, string>>(mapCount);
            for (var i = 0; i < mapCount; i++)
            {
                attributes.Add(new(ReadString(data, ref pos), ReadString(data, ref pos)));
            }

            var ingested = ReadInt64(data, ref pos);
            var partition = BinaryPrimitives.ReadInt32LittleEndian(data[pos..]);
            pos += 4;
            var offset = ReadInt64(data, ref pos);
            rows.Add(new DecodedRow(id, waybill, type, occurred, hub, region, courier, lat, lon, source, schema, attributes, ingested, partition, offset));
        }

        return rows;
    }

    public static Guid ReadUuid(ReadOnlySpan<byte> data, ref int pos)
    {
        Span<byte> bytes = stackalloc byte[16];
        data.Slice(pos, 16).CopyTo(bytes);
        bytes[..8].Reverse();
        bytes[8..].Reverse();
        pos += 16;
        return new Guid(bytes, bigEndian: true);
    }

    public static ulong ReadVarUInt(ReadOnlySpan<byte> data, ref int pos)
    {
        ulong result = 0;
        var shift = 0;
        while (true)
        {
            var b = data[pos++];
            result |= (ulong)(b & 0x7F) << shift;
            if ((b & 0x80) == 0)
            {
                return result;
            }

            shift += 7;
        }
    }

    private static string ReadString(ReadOnlySpan<byte> data, ref int pos)
    {
        var length = (int)ReadVarUInt(data, ref pos);
        var value = Encoding.UTF8.GetString(data.Slice(pos, length));
        pos += length;
        return value;
    }

    private static long ReadInt64(ReadOnlySpan<byte> data, ref int pos)
    {
        var value = BinaryPrimitives.ReadInt64LittleEndian(data[pos..]);
        pos += 8;
        return value;
    }

    private static double? ReadNullableDouble(ReadOnlySpan<byte> data, ref int pos)
    {
        if (data[pos++] == 1)
        {
            return null;
        }

        var value = BinaryPrimitives.ReadDoubleLittleEndian(data[pos..]);
        pos += 8;
        return value;
    }
}

/// <summary>In-memory sink that records inserted rows and can inject failures.</summary>
public sealed class RecordingSink : IEventSink
{
    private readonly object _gate = new();
    private readonly HashSet<string> _tokens = new(StringComparer.Ordinal);

    public List<DecodedRow> Rows { get; } = new();

    public List<string> Tokens { get; } = new();

    public int Attempts;

    /// <summary>Return non-null to fail the attempt with that exception.</summary>
    public Func<IReadOnlyList<DecodedRow>, int, Exception?>? FailWhen { get; set; }

    /// <summary>Return non-null to store the rows and then fail anyway (a lost acknowledgement).</summary>
    public Func<int, Exception?>? FailAfterWriteWhen { get; set; }

    /// <summary>When true, an insert whose token was already seen is ignored (like ClickHouse insert deduplication).</summary>
    public bool DeduplicateTokens { get; set; } = true;

    public Task InsertAsync(ReadOnlyMemory<byte> rows, int rowCount, string deduplicationToken, CancellationToken cancellationToken)
    {
        var decoded = RowBinaryDecoder.Decode(rows.Span);
        if (decoded.Count != rowCount)
        {
            throw new InvalidOperationException($"Row count mismatch: {decoded.Count} decoded vs {rowCount} declared");
        }

        var attempt = Interlocked.Increment(ref Attempts);
        if (FailWhen?.Invoke(decoded, attempt) is { } failure)
        {
            throw failure;
        }

        lock (_gate)
        {
            Tokens.Add(deduplicationToken);
            if (!DeduplicateTokens || _tokens.Add(deduplicationToken))
            {
                Rows.AddRange(decoded);
            }
        }

        if (FailAfterWriteWhen?.Invoke(attempt) is { } lostAck)
        {
            throw lostAck;
        }

        return Task.CompletedTask;
    }
}

/// <summary>In-memory dead-letter sink.</summary>
public sealed class RecordingDeadLetterSink : IDeadLetterSink
{
    public ConcurrentQueue<DeadLetter> Letters { get; } = new();

    public int FailuresRemaining;

    public Task PublishAsync(IReadOnlyList<DeadLetter> letters, CancellationToken cancellationToken)
    {
        if (Interlocked.Decrement(ref FailuresRemaining) >= 0)
        {
            throw new IOException("simulated DLQ outage");
        }

        foreach (var letter in letters)
        {
            Letters.Enqueue(letter);
        }

        return Task.CompletedTask;
    }
}

/// <summary>A bounded in-memory source of pre-built payloads spread over partitions, tracking committed offsets.</summary>
public sealed class InMemorySource : IRecordSource
{
    private readonly IReadOnlyList<byte[]> _payloads;
    private readonly int _partitions;

    public InMemorySource(IReadOnlyList<byte[]> payloads, int partitions)
    {
        _payloads = payloads;
        _partitions = partitions;
        Leases = Enumerable.Range(0, partitions)
            .Select(p => new PartitionLease("mem", p, (lease, next) => Committed[lease.Partition] = next))
            .ToArray();
    }

    public PartitionLease[] Leases { get; }

    public ConcurrentDictionary<int, long> Committed { get; } = new();

    public int ExpectedNextOffset(int partition) => (_payloads.Count - partition + _partitions - 1) / _partitions;

    public async Task RunAsync(ChannelWriter<RawRecord> output, CancellationToken stopReading)
    {
        try
        {
            var offsets = new long[_partitions];
            for (var i = 0; i < _payloads.Count && !stopReading.IsCancellationRequested; i++)
            {
                var p = i % _partitions;
                await output.WriteAsync(new RawRecord(_payloads[i], Leases[p], offsets[p]++), stopReading);
            }
        }
        catch (OperationCanceledException)
        {
        }
        finally
        {
            output.TryComplete();
        }
    }

    public Task CompleteAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
