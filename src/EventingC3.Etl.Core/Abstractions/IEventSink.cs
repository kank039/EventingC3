namespace EventingC3.Etl.Core.Abstractions;

/// <summary>The Load side: writes an encoded RowBinary block to the warehouse atomically.</summary>
public interface IEventSink
{
    /// <summary>
    /// Inserts <paramref name="rowCount"/> pre-encoded rows. Implementations must be idempotent for a given
    /// <paramref name="deduplicationToken"/> and throw <see cref="SinkException"/> with a classified
    /// <see cref="SinkErrorKind"/> on failure.
    /// </summary>
    Task InsertAsync(ReadOnlyMemory<byte> rows, int rowCount, string deduplicationToken, CancellationToken cancellationToken);
}

/// <summary>How the pipeline should react to a sink failure.</summary>
public enum SinkErrorKind
{
    /// <summary>Retry the same insert after a back-off (network, overload, timeouts, too many parts...).</summary>
    Transient,

    /// <summary>The data itself was refused: split the batch to isolate and dead-letter the offending rows.</summary>
    InvalidData,

    /// <summary>Configuration / permission / schema problem. Retry with back-off and report unhealthy; never drop data.</summary>
    Fatal,
}

/// <summary>A classified sink failure.</summary>
public sealed class SinkException : Exception
{
    public SinkException(SinkErrorKind kind, string message, Exception? inner = null)
        : base(message, inner)
    {
        Kind = kind;
    }

    public SinkErrorKind Kind { get; }
}
