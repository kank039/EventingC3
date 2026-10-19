using System.Globalization;
using System.Text;
using Confluent.Kafka;
using EventingC3.Etl.Core.Abstractions;

namespace EventingC3.Etl.Infrastructure.Kafka;

/// <summary>
/// Publishes rejected records to a dead-letter topic with the original payload as the value and the
/// rejection context in headers. Idempotent, acks=all producer; the call returns only once every
/// message is acknowledged by the broker.
/// </summary>
public sealed class KafkaDeadLetterSink : IDeadLetterSink, IDisposable
{
    private readonly KafkaDeadLetterOptions _options;
    private readonly IProducer<Null, byte[]> _producer;
    private int _disposed;

    public KafkaDeadLetterSink(KafkaDeadLetterOptions options, KafkaSourceOptions source)
    {
        _options = options;
        var config = new ProducerConfig
        {
            BootstrapServers = string.IsNullOrWhiteSpace(options.BootstrapServers) ? source.BootstrapServers : options.BootstrapServers,
            ClientId = $"eventingc3-etl-dlq-{Environment.MachineName}",
            Acks = Acks.All,
            EnableIdempotence = true,
            LingerMs = 20,
            CompressionType = CompressionType.Lz4,
            MessageTimeoutMs = 120_000,
        };

        // Share security settings (SASL etc.) with the source unless explicitly overridden.
        foreach (var (key, value) in source.ConsumerConfig.Where(kv => kv.Key.StartsWith("security.", StringComparison.Ordinal)
                     || kv.Key.StartsWith("sasl.", StringComparison.Ordinal) || kv.Key.StartsWith("ssl.", StringComparison.Ordinal)))
        {
            config.Set(key, value);
        }

        foreach (var (key, value) in options.ProducerConfig)
        {
            config.Set(key, value);
        }

        _producer = new ProducerBuilder<Null, byte[]>(config).Build();
    }

    public async Task PublishAsync(IReadOnlyList<DeadLetter> letters, CancellationToken cancellationToken)
    {
        var deliveries = new Task[letters.Count];
        for (var i = 0; i < letters.Count; i++)
        {
            deliveries[i] = _producer.ProduceAsync(_options.Topic, ToMessage(letters[i]), cancellationToken);
        }

        await Task.WhenAll(deliveries).ConfigureAwait(false);
    }

    private Message<Null, byte[]> ToMessage(DeadLetter letter)
    {
        var payload = letter.Payload;
        var headers = new Headers
        {
            { "dlq.reason", Utf8(letter.Reason.ToString()) },
            { "dlq.source", Utf8(letter.Source) },
            { "dlq.partition", Utf8(letter.Partition.ToString(CultureInfo.InvariantCulture)) },
            { "dlq.offset", Utf8(letter.Offset.ToString(CultureInfo.InvariantCulture)) },
            { "dlq.rejected_at", Utf8(letter.RejectedAt.ToString("O", CultureInfo.InvariantCulture)) },
        };

        if (letter.Detail is { Length: > 0 } detail)
        {
            headers.Add("dlq.detail", Utf8(detail.Length > 1024 ? detail[..1024] : detail));
        }

        if (payload.Length > _options.MaxPayloadBytes)
        {
            headers.Add("dlq.original_size", Utf8(payload.Length.ToString(CultureInfo.InvariantCulture)));
            payload = payload[.._options.MaxPayloadBytes];
        }

        return new Message<Null, byte[]> { Value = payload, Headers = headers };
    }

    private static byte[] Utf8(string value) => Encoding.UTF8.GetBytes(value);

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        _producer.Flush(TimeSpan.FromSeconds(10));
        _producer.Dispose();
    }
}
