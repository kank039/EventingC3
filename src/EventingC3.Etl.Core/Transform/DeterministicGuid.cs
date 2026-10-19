using System.Buffers;
using System.Security.Cryptography;
using System.Text;

namespace EventingC3.Etl.Core.Transform;

/// <summary>
/// RFC 9562 UUIDv5 (SHA-1, name-based). Used to give id-less events a stable identity so that a
/// replayed or re-sent copy of the same event deduplicates in the warehouse.
/// </summary>
public static class DeterministicGuid
{
    /// <summary>Namespace for EventingC3 tracking events (fixed forever — changing it breaks deduplication).</summary>
    public static readonly Guid TrackingEventNamespace = new("4f1c2b9e-6a43-5d7e-9b1a-3c2d8e7f6a51");

    /// <summary>Creates the UUIDv5 of <paramref name="name"/> within <paramref name="namespaceId"/>.</summary>
    public static Guid Create(Guid namespaceId, string name)
    {
        var nameByteCount = Encoding.UTF8.GetByteCount(name);
        var length = 16 + nameByteCount;
        var rented = ArrayPool<byte>.Shared.Rent(length);
        try
        {
            namespaceId.TryWriteBytes(rented.AsSpan(0, 16), bigEndian: true, out _);
            Encoding.UTF8.GetBytes(name, rented.AsSpan(16, nameByteCount));

            Span<byte> hash = stackalloc byte[20];
            SHA1.HashData(rented.AsSpan(0, length), hash);

            hash[6] = (byte)((hash[6] & 0x0F) | 0x50); // version 5
            hash[8] = (byte)((hash[8] & 0x3F) | 0x80); // RFC 4122 variant
            return new Guid(hash[..16], bigEndian: true);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(rented);
        }
    }
}
