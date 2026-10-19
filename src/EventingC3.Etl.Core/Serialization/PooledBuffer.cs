using System.Buffers;

namespace EventingC3.Etl.Core.Serialization;

/// <summary>
/// A growable byte buffer backed by <see cref="ArrayPool{T}"/>. Batches are tens of megabytes, so
/// renting avoids a large-object-heap allocation (and Gen2 GC pressure) for every insert.
/// </summary>
public sealed class PooledBuffer : IBufferWriter<byte>, IDisposable
{
    private byte[] _buffer;
    private int _written;

    public PooledBuffer(int initialCapacity = 1024 * 1024)
    {
        _buffer = ArrayPool<byte>.Shared.Rent(Math.Max(initialCapacity, 256));
    }

    /// <summary>Number of bytes written so far.</summary>
    public int Length => _written;

    /// <summary>The written bytes.</summary>
    public ReadOnlyMemory<byte> WrittenMemory => _buffer.AsMemory(0, _written);

    /// <summary>The written bytes.</summary>
    public ReadOnlySpan<byte> WrittenSpan => _buffer.AsSpan(0, _written);

    public void Advance(int count)
    {
        if ((uint)count > (uint)(_buffer.Length - _written))
        {
            throw new ArgumentOutOfRangeException(nameof(count));
        }

        _written += count;
    }

    public Memory<byte> GetMemory(int sizeHint = 0)
    {
        Ensure(sizeHint);
        return _buffer.AsMemory(_written);
    }

    public Span<byte> GetSpan(int sizeHint = 0)
    {
        Ensure(sizeHint);
        return _buffer.AsSpan(_written);
    }

    /// <summary>Discards written data but keeps the rented array for reuse.</summary>
    public void Reset() => _written = 0;

    public void Dispose()
    {
        var buffer = Interlocked.Exchange(ref _buffer, []);
        if (buffer.Length > 0)
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    private void Ensure(int sizeHint)
    {
        sizeHint = Math.Max(sizeHint, 1);
        if (_buffer.Length - _written >= sizeHint)
        {
            return;
        }

        var required = (long)_written + sizeHint;
        var newSize = Math.Max(required, (long)_buffer.Length * 2);
        if (newSize > Array.MaxLength)
        {
            newSize = required <= Array.MaxLength ? Array.MaxLength : throw new InvalidOperationException("Buffer exceeds maximum array length.");
        }

        var next = ArrayPool<byte>.Shared.Rent((int)newSize);
        _buffer.AsSpan(0, _written).CopyTo(next);
        ArrayPool<byte>.Shared.Return(_buffer);
        _buffer = next;
    }
}
