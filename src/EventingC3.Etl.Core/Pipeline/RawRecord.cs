namespace EventingC3.Etl.Core.Pipeline;

/// <summary>An undecoded record as read from the source, with its position for offset tracking and lineage.</summary>
public readonly record struct RawRecord(byte[] Payload, PartitionLease Lease, long Offset);
