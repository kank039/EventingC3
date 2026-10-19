namespace EventingC3.Etl.Core.Transform;

/// <summary>Why a record was routed to the dead-letter queue instead of the warehouse.</summary>
public enum RejectReason
{
    None = 0,
    TooLarge,
    MalformedJson,
    NotAnObject,
    MissingWaybill,
    InvalidWaybill,
    MissingEventType,
    InvalidEventType,
    MissingTimestamp,
    InvalidTimestamp,
    TimestampTooOld,
    TimestampInFuture,
    FieldTooLong,
    /// <summary>The warehouse refused the row even though it passed validation (isolated by batch bisection).</summary>
    SinkRejected,
}
