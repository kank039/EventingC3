using EventingC3.Etl.Core.Domain;

namespace EventingC3.Etl.Core.Serialization;

/// <summary>
/// Encodes <see cref="TrackingEvent"/> rows as RowBinary in the column order of <see cref="Columns"/>,
/// which must match <c>deploy/clickhouse/001_schema.sql</c>.
/// </summary>
public static class TrackingEventEncoder
{
    /// <summary>Insert column list; the order is the RowBinary field order.</summary>
    public static readonly IReadOnlyList<string> Columns =
    [
        "event_id",
        "waybill",
        "event_type",
        "occurred_at",
        "hub_code",
        "region",
        "courier_id",
        "latitude",
        "longitude",
        "source_system",
        "schema_version",
        "attributes",
        "ingested_at",
        "source_partition",
        "source_offset",
    ];

    /// <summary>Writes one row. <paramref name="ingestedAtUnixMs"/> is stamped per batch.</summary>
    public static void Write(RowBinaryWriter writer, TrackingEvent e, long ingestedAtUnixMs)
    {
        writer.WriteUuid(e.EventId);
        writer.WriteString(e.Waybill);
        writer.WriteString(e.EventType);
        writer.WriteInt64(e.OccurredAtUnixMs);   // DateTime64(3)
        writer.WriteString(e.HubCode);
        writer.WriteString(e.Region);
        writer.WriteString(e.CourierId);
        writer.WriteNullableFloat64(e.Latitude);
        writer.WriteNullableFloat64(e.Longitude);
        writer.WriteString(e.SourceSystem);
        writer.WriteUInt16(e.SchemaVersion);
        writer.WriteStringMap(e.Attributes);
        writer.WriteInt64(ingestedAtUnixMs);     // DateTime64(3)
        writer.WriteInt32(e.SourcePartition);
        writer.WriteInt64(e.SourceOffset);
    }
}
