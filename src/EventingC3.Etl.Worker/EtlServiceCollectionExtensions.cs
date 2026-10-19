using EventingC3.Etl.Core.Abstractions;
using EventingC3.Etl.Core.Domain;
using EventingC3.Etl.Core.Observability;
using EventingC3.Etl.Core.Pipeline;
using EventingC3.Etl.Core.Transform;
using EventingC3.Etl.Infrastructure.ClickHouse;
using EventingC3.Etl.Infrastructure.Files;
using EventingC3.Etl.Infrastructure.Kafka;

namespace EventingC3.Etl.Worker;

public static class EtlServiceCollectionExtensions
{
    /// <summary>Registers the pipeline and the source / sink adapters selected by <paramref name="settings"/>.</summary>
    public static IServiceCollection AddEtlPipeline(this IServiceCollection services, EtlSettings settings)
    {
        services.AddSingleton(settings);
        services.AddSingleton(settings.Pipeline);
        services.AddSingleton(settings.Transform);
        services.AddSingleton(settings.ClickHouse);
        services.AddSingleton(TimeProvider.System);
        services.AddSingleton<EtlMetrics>();
        services.AddSingleton<EtlHealth>();
        services.AddSingleton(settings.HubRegions.Count > 0 ? new HubDirectory(settings.HubRegions) : HubDirectory.Default);
        services.AddSingleton<TrackingEventParser>();

        // Each disposable adapter is registered exactly once (forwarding registrations get disposed twice).
        services.AddSingleton<ClickHouseSink>();
        services.AddSingleton<IEventSink>(sp => sp.GetRequiredService<ClickHouseSink>());

        if (settings.Source == "File")
        {
            services.AddSingleton(settings.Files);
            services.AddSingleton<IRecordSource, FileRecordSource>();
        }
        else
        {
            services.AddSingleton(settings.Kafka);
            services.AddSingleton<IRecordSource, KafkaRecordSource>();
        }

        if (settings.DeadLetter == "File")
        {
            services.AddSingleton(settings.DeadLetterFiles);
            services.AddSingleton<IDeadLetterSink, FileDeadLetterSink>();
        }
        else
        {
            services.AddSingleton<IDeadLetterSink>(_ => new KafkaDeadLetterSink(settings.DeadLetterKafka, settings.Kafka));
        }

        services.AddSingleton<Batcher>();
        services.AddSingleton<BatchProcessor>();
        services.AddSingleton<EtlPipeline>();
        return services;
    }
}
