using System.Buffers;
using System.Buffers.Binary;
using System.Text;

namespace EventingC3.Etl.Core.Serialization;

/// <summary>
/// Writes values in ClickHouse's <c>RowBinary</c> wire format — the cheapest format for the server to ingest
/// (no text parsing) and compact on the wire. All multi-byte integers are little-endian.
/// </summary>
/// <remarks>See https://clickhouse.com/docs/interfaces/formats/RowBinary.</remarks>
public readonly ref struct RowBinaryWriter
{
    private readonly IBufferWriter<byte> _output;

    public RowBinaryWriter(IBufferWriter<byte> output) => _output = output;

    public void WriteUInt8(byte value)
    {
        _output.GetSpan(1)[0] = value;
        _output.Advance(1);
    }

    public void WriteUInt16(ushort value)
    {
        BinaryPrimitives.WriteUInt16LittleEndian(_output.GetSpan(2), value);
        _output.Advance(2);
    }

    public void WriteInt32(int value)
    {
        BinaryPrimitives.WriteInt32LittleEndian(_output.GetSpan(4), value);
        _output.Advance(4);
    }

    public void WriteInt64(long value)
    {
        BinaryPrimitives.WriteInt64LittleEndian(_output.GetSpan(8), value);
        _output.Advance(8);
    }

    public void WriteFloat64(double value)
    {
        BinaryPrimitives.WriteDoubleLittleEndian(_output.GetSpan(8), value);
        _output.Advance(8);
    }

    /// <summary>Unsigned LEB128, used for string lengths and collection sizes.</summary>
    public void WriteVarUInt(ulong value)
    {
        var span = _output.GetSpan(10);
        var i = 0;
        while (value >= 0x80)
        {
            span[i++] = (byte)(value | 0x80);
            value >>= 7;
        }

        span[i++] = (byte)value;
        _output.Advance(i);
    }

    /// <summary><c>String</c> / <c>LowCardinality(String)</c>: varint byte length followed by UTF-8 bytes.</summary>
    public void WriteString(string value)
    {
        if (value.Length == 0)
        {
            WriteUInt8(0);
            return;
        }

        var byteCount = Encoding.UTF8.GetByteCount(value);
        WriteVarUInt((ulong)byteCount);
        var span = _output.GetSpan(byteCount);
        var written = Encoding.UTF8.GetBytes(value, span);
        _output.Advance(written);
    }

    /// <summary>
    /// <c>UUID</c>: ClickHouse stores a UUID as two little-endian UInt64 halves, so each 8-byte half of the
    /// RFC 4122 (big-endian) byte sequence is written reversed.
    /// </summary>
    public void WriteUuid(Guid value)
    {
        var span = _output.GetSpan(16);
        value.TryWriteBytes(span, bigEndian: true, out _);
        span[..8].Reverse();
        span[8..16].Reverse();
        _output.Advance(16);
    }

    /// <summary><c>Nullable(Float64)</c>: a null-marker byte (1 = NULL) then the value when present.</summary>
    public void WriteNullableFloat64(double? value)
    {
        if (value is { } v)
        {
            WriteUInt8(0);
            WriteFloat64(v);
        }
        else
        {
            WriteUInt8(1);
        }
    }

    /// <summary><c>Map(String, String)</c>: varint entry count followed by key/value pairs.</summary>
    public void WriteStringMap(IReadOnlyList<KeyValuePair<string, string>> map)
    {
        WriteVarUInt((ulong)map.Count);
        for (var i = 0; i < map.Count; i++)
        {
            WriteString(map[i].Key);
            WriteString(map[i].Value);
        }
    }
}
