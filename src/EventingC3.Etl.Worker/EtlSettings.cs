using EventingC3.Etl.Core.Pipeline;
using EventingC3.Etl.Core.Transform;
using EventingC3.Etl.Infrastructure.ClickHouse;
using EventingC3.Etl.Infrastructure.Files;
using EventingC3.Etl.Infrastructure.Kafka;

namespace EventingC3.Etl.Worker;

/// <summary>The <c>Etl</c> configuration section.</summary>
public sealed class EtlSettings
{
    public const string SectionName = "Etl";

    /// <summary><c>Kafka</c> (streaming, default) or <c>File</c> (bulk / backfill).</summary>
    public string Source { get; set; } = "Kafka";

    /// <summary><c>Kafka</c> or <c>File</c>.</summary>
    public string DeadLetter { get; set; } = "Kafka";

    public PipelineOptions Pipeline { get; set; } = new();

    public TransformOptions Transform { get; set; } = new();

    /// <summary>Hub code → region overrides for enrichment; empty uses the built-in directory.</summary>
    public Dictionary<string, string> HubRegions { get; set; } = new(StringComparer.OrdinalIgnoreCase);

    public KafkaSourceOptions Kafka { get; set; } = new();

    public KafkaDeadLetterOptions DeadLetterKafka { get; set; } = new();

    public FileSourceOptions Files { get; set; } = new();

    public FileDeadLetterOptions DeadLetterFiles { get; set; } = new();

    public ClickHouseOptions ClickHouse { get; set; } = new();

    /// <summary>Fails fast at startup on settings that would break correctness guarantees.</summary>
    public void Validate()
    {
        var errors = new List<string>();
        if (Pipeline.MaxBatchRecords is < 1 or > 1_000_000)
        {
            errors.Add("Pipeline.MaxBatchRecords must be 1..1,000,000 (one insert must stay one atomic ClickHouse block).");
        }

        if (Pipeline.MaxConcurrentBatches < 1)
        {
            errors.Add("Pipeline.MaxConcurrentBatches must be >= 1.");
        }

        if (Pipeline.RecordBufferCapacity < Pipeline.MaxBatchRecords / 4)
        {
            errors.Add("Pipeline.RecordBufferCapacity should be at least a quarter of MaxBatchRecords.");
        }

        if (Source is not ("Kafka" or "File"))
        {
            errors.Add("Source must be 'Kafka' or 'File'.");
        }

        if (DeadLetter is not ("Kafka" or "File"))
        {
            errors.Add("DeadLetter must be 'Kafka' or 'File'.");
        }

        if (errors.Count > 0)
        {
            throw new InvalidOperationException("Invalid Etl configuration: " + string.Join(" ", errors));
        }
    }
}
