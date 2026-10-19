using EventingC3.Etl.Core.Transform;

namespace EventingC3.Etl.Core.Abstractions;

/// <summary>A rejected record, kept byte-for-byte so it can be inspected, fixed and replayed.</summary>
public sealed record DeadLetter(
    byte[] Payload,
    RejectReason Reason,
    string? Detail,
    string Source,
    int Partition,
    long Offset,
    DateTimeOffset RejectedAt);

/// <summary>Durable store for rejected records. A batch only commits after its dead letters are acknowledged.</summary>
public interface IDeadLetterSink
{
    Task PublishAsync(IReadOnlyList<DeadLetter> letters, CancellationToken cancellationToken);
}
