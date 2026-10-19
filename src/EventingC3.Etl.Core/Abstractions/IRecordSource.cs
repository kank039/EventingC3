using System.Threading.Channels;
using EventingC3.Etl.Core.Pipeline;

namespace EventingC3.Etl.Core.Abstractions;

/// <summary>The Extract side: reads raw records and commits progress handed back through <see cref="PartitionLease"/>.</summary>
public interface IRecordSource
{
    /// <summary>
    /// Reads records into <paramref name="output"/> until <paramref name="stopReading"/> fires or the source is
    /// exhausted, then completes the writer. Must honour channel back-pressure.
    /// </summary>
    Task RunAsync(ChannelWriter<RawRecord> output, CancellationToken stopReading);

    /// <summary>Called once all in-flight batches have finished: commit final progress and release resources.</summary>
    Task CompleteAsync(CancellationToken cancellationToken);
}
