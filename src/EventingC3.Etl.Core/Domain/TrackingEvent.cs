namespace EventingC3.Etl.Core.Domain;

/// <summary>
/// A validated, normalised consignment tracking event — the unit the pipeline loads into the warehouse.
/// </summary>
/// <remarks>
/// Instances are only produced by <see cref="Transform.TrackingEventParser"/>, so every field is already
/// within the bounds the ClickHouse schema expects (UTC timestamps, upper-cased codes, bounded lengths).
/// </remarks>
public sealed record TrackingEvent
{
    /// <summary>Producer-supplied id, or a deterministic UUIDv5 when the producer omitted it.</summary>
    public required Guid EventId { get; init; }

    /// <summary>Upper-cased waybill / consignment number.</summary>
    public required string Waybill { get; init; }

    /// <summary>Canonical event type (e.g. <c>DELIVERED</c>, <c>AT_HUB</c>).</summary>
    public required string EventType { get; init; }

    /// <summary>When the event happened, in Unix milliseconds (UTC).</summary>
    public required long OccurredAtUnixMs { get; init; }

    /// <summary>Upper-cased hub code, empty when not supplied.</summary>
    public string HubCode { get; init; } = string.Empty;

    /// <summary>Region derived from <see cref="HubCode"/>; <c>UNKNOWN</c> when the hub is not in the directory.</summary>
    public string Region { get; init; } = HubDirectory.UnknownRegion;

    /// <summary>Courier identifier, empty when not supplied.</summary>
    public string CourierId { get; init; } = string.Empty;

    /// <summary>WGS84 latitude, <c>null</c> when absent or invalid.</summary>
    public double? Latitude { get; init; }

    /// <summary>WGS84 longitude, <c>null</c> when absent or invalid.</summary>
    public double? Longitude { get; init; }

    /// <summary>Producing system (scanner app, partner integration, ...).</summary>
    public string SourceSystem { get; init; } = string.Empty;

    /// <summary>Producer payload schema version.</summary>
    public ushort SchemaVersion { get; init; } = 1;

    /// <summary>Free-form attributes plus any unknown top-level scalar fields (schema drift is kept, not dropped).</summary>
    public IReadOnlyList<KeyValuePair<string, string>> Attributes { get; init; } = [];

    /// <summary>Lineage: source partition (Kafka partition or file index).</summary>
    public int SourcePartition { get; init; }

    /// <summary>Lineage: source offset (Kafka offset or line number).</summary>
    public long SourceOffset { get; init; }
}
