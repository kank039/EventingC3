using System.Security.Cryptography;
using System.Text;

namespace EventingC3.Etl.Core.Pipeline;

/// <summary>
/// A unit of loading: records from one or more partitions, plus the per-partition offset segments
/// that become committable once the batch is durably loaded.
/// </summary>
public sealed class RawBatch
{
    private readonly Dictionary<PartitionLease, PartitionLease.Segment> _segments = new(ReferenceEqualityComparer.Instance);

    public RawBatch(long id, int capacity, DateTimeOffset createdAt)
    {
        Id = id;
        Records = new List<RawRecord>(capacity);
        CreatedAt = createdAt;
    }

    public long Id { get; }

    public List<RawRecord> Records { get; }

    public DateTimeOffset CreatedAt { get; }

    public long Bytes { get; private set; }

    public IEnumerable<PartitionLease.Segment> Segments => _segments.Values;

    /// <summary>Appends a record. Must be called in source order per partition.</summary>
    public void Add(in RawRecord record)
    {
        if (_segments.TryGetValue(record.Lease, out var segment))
        {
            segment.LastOffset = record.Offset;
        }
        else
        {
            _segments.Add(record.Lease, record.Lease.Open(record.Offset));
        }

        Records.Add(record);
        Bytes += record.Payload.Length;
    }

    /// <summary>Marks every segment loaded so the source can commit past this batch.</summary>
    public void Complete()
    {
        foreach (var segment in _segments.Values)
        {
            segment.Complete();
        }
    }

    /// <summary>
    /// A stable fingerprint of this batch's exact source ranges (input to the insert deduplication token).
    /// </summary>
    /// <remarks>
    /// Not a token on its own: two batches can cover identical ranges yet carry different rows (e.g. a worker that
    /// lost the partition mid-batch inserted only part of it). See <see cref="BatchProcessor"/>.
    /// </remarks>
    public string SourceFingerprint()
    {
        var sb = new StringBuilder(64 * _segments.Count);
        foreach (var segment in _segments.Values.OrderBy(s => s.Lease.Source, StringComparer.Ordinal).ThenBy(s => s.Lease.Partition))
        {
            sb.Append(segment.Lease.Source).Append(':').Append(segment.Lease.Partition).Append(':')
              .Append(segment.FirstOffset).Append('-').Append(segment.LastOffset).Append(';');
        }

        return Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(sb.ToString())));
    }
}
