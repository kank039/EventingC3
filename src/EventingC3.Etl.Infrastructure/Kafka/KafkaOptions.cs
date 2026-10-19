namespace EventingC3.Etl.Infrastructure.Kafka;

/// <summary>
/// Kafka-protocol source settings. Works unchanged against Azure Event Hubs (Kafka endpoint) by supplying
/// <c>security.protocol=SASL_SSL</c>, <c>sasl.mechanism=PLAIN</c>, <c>sasl.username=$ConnectionString</c>
/// and the connection string as <c>sasl.password</c> through <see cref="ConsumerConfig"/>.
/// </summary>
public sealed class KafkaSourceOptions
{
    public string BootstrapServers { get; set; } = "localhost:9092";

    public string Topic { get; set; } = "tracking-events";

    public string GroupId { get; set; } = "eventingc3-etl";

    /// <summary>How often loaded offsets are committed. Lower = less replay after a crash, more broker load.</summary>
    public TimeSpan CommitInterval { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>Raw librdkafka consumer properties; override the worker's defaults (applied last).</summary>
    public Dictionary<string, string> ConsumerConfig { get; set; } = new(StringComparer.Ordinal);
}

/// <summary>Kafka dead-letter topic settings.</summary>
public sealed class KafkaDeadLetterOptions
{
    /// <summary>Defaults to the source's bootstrap servers when empty.</summary>
    public string? BootstrapServers { get; set; }

    public string Topic { get; set; } = "tracking-events.dlq";

    /// <summary>Payloads above this are truncated (the broker would refuse them and stall the pipeline).</summary>
    public int MaxPayloadBytes { get; set; } = 512 * 1024;

    /// <summary>Raw librdkafka producer properties; override the defaults (applied last).</summary>
    public Dictionary<string, string> ProducerConfig { get; set; } = new(StringComparer.Ordinal);
}
