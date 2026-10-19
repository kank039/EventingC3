namespace EventingC3.Etl.Core.Pipeline;

/// <summary>
/// Tracks loading progress for one source partition for as long as this worker owns it, and decides
/// which offset may safely be committed.
/// </summary>
/// <remarks>
/// <para>
/// Batches are loaded concurrently and can finish out of order. A partition's offset may only advance past
/// a range once that range <em>and every earlier range</em> of the partition has been loaded, otherwise a
/// crash would skip unloaded records. Each batch therefore opens a <see cref="Segment"/> per partition it
/// touches; segments are queued in offset order and the commit point only advances over a completed prefix.
/// </para>
/// <para>
/// A lease is revoked when the partition is reassigned (Kafka rebalance). After that its completions are
/// ignored — the new owner restarts from the last committed offset, and any overlap is absorbed by
/// warehouse-side deduplication.
/// </para>
/// </remarks>
public sealed class PartitionLease
{
    private readonly object _gate = new();
    private readonly Queue<Segment> _segments = new();
    private readonly Action<PartitionLease, long> _onCommittable;
    private volatile bool _revoked;
    private long _committable = -1;

    /// <param name="source">Topic name or file path.</param>
    /// <param name="partition">Partition number (or file index).</param>
    /// <param name="onCommittable">Invoked with the next offset to consume each time it advances; runs under the lease lock, so it must be cheap and non-blocking.</param>
    public PartitionLease(string source, int partition, Action<PartitionLease, long> onCommittable)
    {
        Source = source;
        Partition = partition;
        _onCommittable = onCommittable;
    }

    public string Source { get; }

    public int Partition { get; }

    public bool IsRevoked => _revoked;

    /// <summary>The next offset to consume after everything loaded so far; -1 before the first completion.</summary>
    public long CommittableOffset => Volatile.Read(ref _committable);

    /// <summary>Number of opened but not yet committed segments (in-flight work for this partition).</summary>
    public int PendingSegments
    {
        get
        {
            lock (_gate)
            {
                return _segments.Count;
            }
        }
    }

    /// <summary>Opens a segment starting at <paramref name="firstOffset"/>. Called by the single batcher thread in offset order.</summary>
    public Segment Open(long firstOffset)
    {
        var segment = new Segment(this, firstOffset);
        lock (_gate)
        {
            _segments.Enqueue(segment);
        }

        return segment;
    }

    /// <summary>Stops this lease from producing further commits.</summary>
    public void Revoke() => _revoked = true;

    internal void Complete(Segment segment)
    {
        lock (_gate)
        {
            segment.Done = true;
            long advancedTo = -1;
            while (_segments.TryPeek(out var head) && head.Done)
            {
                _segments.Dequeue();
                advancedTo = head.LastOffset + 1;
            }

            if (advancedTo > _committable)
            {
                Volatile.Write(ref _committable, advancedTo);

                // Invoked under the lock so callers observe commit points in increasing order.
                if (!_revoked)
                {
                    _onCommittable(this, advancedTo);
                }
            }
        }
    }

    public override string ToString() => $"{Source}[{Partition}]";

    /// <summary>A contiguous run of offsets of one partition inside one batch.</summary>
    public sealed class Segment
    {
        internal Segment(PartitionLease lease, long firstOffset)
        {
            Lease = lease;
            FirstOffset = firstOffset;
            LastOffset = firstOffset;
        }

        public PartitionLease Lease { get; }

        public long FirstOffset { get; }

        public long LastOffset { get; internal set; }

        internal bool Done { get; set; }

        /// <summary>Marks the segment loaded; the lease advances its commit point if this closes a gap.</summary>
        public void Complete() => Lease.Complete(this);
    }
}
