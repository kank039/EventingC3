namespace EventingC3.Etl.Core.Transform;

/// <summary>Validation limits applied to every inbound record.</summary>
public sealed class TransformOptions
{
    /// <summary>Records larger than this are rejected before parsing (protects memory from runaway producers).</summary>
    public int MaxRecordBytes { get; set; } = 64 * 1024;

    /// <summary>Events older than this are rejected; raise it for historical backfills.</summary>
    public TimeSpan MaxEventAge { get; set; } = TimeSpan.FromDays(30);

    /// <summary>Tolerated device clock skew into the future.</summary>
    public TimeSpan MaxFutureSkew { get; set; } = TimeSpan.FromHours(1);

    /// <summary>Offset applied to timestamps that carry no zone designator. RAM operates on SAST (UTC+2, no DST).</summary>
    public TimeSpan DefaultUtcOffset { get; set; } = TimeSpan.FromHours(2);

    /// <summary>Maximum number of attributes kept per event; the rest are dropped.</summary>
    public int MaxAttributes { get; set; } = 32;

    /// <summary>Maximum attribute key length in characters.</summary>
    public int MaxAttributeKeyLength { get; set; } = 64;

    /// <summary>Attribute values are truncated to this many characters.</summary>
    public int MaxAttributeValueLength { get; set; } = 1024;
}
