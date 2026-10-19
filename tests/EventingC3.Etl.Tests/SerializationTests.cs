using EventingC3.Etl.Core.Domain;
using EventingC3.Etl.Core.Serialization;
using EventingC3.Etl.TestSupport;

namespace EventingC3.Etl.Tests;

public class SerializationTests
{
    [Theory]
    [InlineData(0UL, new byte[] { 0x00 })]
    [InlineData(1UL, new byte[] { 0x01 })]
    [InlineData(127UL, new byte[] { 0x7F })]
    [InlineData(128UL, new byte[] { 0x80, 0x01 })]
    [InlineData(300UL, new byte[] { 0xAC, 0x02 })]
    [InlineData(16384UL, new byte[] { 0x80, 0x80, 0x01 })]
    public void Writes_leb128_varints(ulong value, byte[] expected)
    {
        using var buffer = new PooledBuffer(16);
        new RowBinaryWriter(buffer).WriteVarUInt(value);
        Assert.Equal(expected, buffer.WrittenSpan.ToArray());
    }

    [Fact]
    public void Writes_strings_as_length_prefixed_utf8()
    {
        using var buffer = new PooledBuffer(16);
        var writer = new RowBinaryWriter(buffer);
        writer.WriteString("Hé");
        writer.WriteString(string.Empty);
        Assert.Equal(new byte[] { 3, (byte)'H', 0xC3, 0xA9, 0 }, buffer.WrittenSpan.ToArray());
    }

    [Fact]
    public void Writes_uuid_as_two_little_endian_halves()
    {
        using var buffer = new PooledBuffer(16);
        new RowBinaryWriter(buffer).WriteUuid(Guid.Parse("00112233-4455-6677-8899-aabbccddeeff"));
        Assert.Equal(
            new byte[] { 0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11, 0x00, 0xFF, 0xEE, 0xDD, 0xCC, 0xBB, 0xAA, 0x99, 0x88 },
            buffer.WrittenSpan.ToArray());
    }

    [Fact]
    public void Writes_nullable_with_marker_byte()
    {
        using var buffer = new PooledBuffer(16);
        var writer = new RowBinaryWriter(buffer);
        writer.WriteNullableFloat64(null);
        writer.WriteNullableFloat64(1.0);
        Assert.Equal(new byte[] { 1, 0, 0, 0, 0, 0, 0, 0, 0xF0, 0x3F }, buffer.WrittenSpan.ToArray());
    }

    [Fact]
    public void Pooled_buffer_grows_and_preserves_content()
    {
        using var buffer = new PooledBuffer(256);
        var writer = new RowBinaryWriter(buffer);
        for (var i = 0; i < 100_000; i++)
        {
            writer.WriteInt32(i);
        }

        Assert.Equal(400_000, buffer.Length);
        Assert.Equal(99_999, BitConverter.ToInt32(buffer.WrittenSpan[^4..]));
    }

    [Fact]
    public void Encoded_rows_round_trip()
    {
        var events = new[]
        {
            new TrackingEvent
            {
                EventId = Guid.NewGuid(), Waybill = "RAM000000001", EventType = "DELIVERED", OccurredAtUnixMs = 1_760_868_000_123,
                HubCode = "JNB01", Region = "GAUTENG", CourierId = "C1", Latitude = -26.2, Longitude = 28.0, SourceSystem = "scanner-app",
                SchemaVersion = 7, Attributes = [new("k", "v"), new("ünïcödé", "✓")], SourcePartition = 11, SourceOffset = long.MaxValue,
            },
            new TrackingEvent { EventId = Guid.Empty, Waybill = "X", EventType = "AT_HUB", OccurredAtUnixMs = 0 },
        };

        using var buffer = new PooledBuffer(64);
        var writer = new RowBinaryWriter(buffer);
        foreach (var e in events)
        {
            TrackingEventEncoder.Write(writer, e, 99);
        }

        var rows = RowBinaryDecoder.Decode(buffer.WrittenSpan);
        Assert.Equal(2, rows.Count);
        Assert.Equal(events[0].EventId, rows[0].EventId);
        Assert.Equal("ünïcödé", rows[0].Attributes[1].Key);
        Assert.Equal(-26.2, rows[0].Latitude);
        Assert.Equal(long.MaxValue, rows[0].SourceOffset);
        Assert.Equal(99, rows[0].IngestedAtMs);
        Assert.Null(rows[1].Latitude);
        Assert.Empty(rows[1].Attributes);
        Assert.Equal(15, TrackingEventEncoder.Columns.Count);
    }
}
