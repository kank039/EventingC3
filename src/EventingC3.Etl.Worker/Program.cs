using EventingC3.Etl.Core.Abstractions;
using EventingC3.Etl.Core.Domain;
using EventingC3.Etl.Core.Observability;
using EventingC3.Etl.Core.Pipeline;
using EventingC3.Etl.Core.Transform;
using EventingC3.Etl.Infrastructure.ClickHouse;
using EventingC3.Etl.Infrastructure.Files;
using EventingC3.Etl.Infrastructure.Kafka;
using EventingC3.Etl.Worker;

var builder = WebApplication.CreateSlimBuilder(args);

var settings = builder.Configuration.GetSection(EtlSettings.SectionName).Get<EtlSettings>() ?? new EtlSettings();
settings.Validate();

builder.Services.Configure<HostOptions>(o =>
{
    // Give the pipeline its drain window plus time to commit offsets before the host gives up.
    o.ShutdownTimeout = settings.Pipeline.ShutdownGracePeriod + TimeSpan.FromSeconds(15);
    o.BackgroundServiceExceptionBehavior = BackgroundServiceExceptionBehavior.StopHost;
});

var services = builder.Services;
services.AddSingleton(settings);
services.AddSingleton(settings.Pipeline);
services.AddSingleton(settings.Transform);
services.AddSingleton(settings.ClickHouse);
services.AddSingleton(TimeProvider.System);
services.AddSingleton<EtlMetrics>();
services.AddSingleton<EtlHealth>();
services.AddSingleton(settings.HubRegions.Count > 0 ? new HubDirectory(settings.HubRegions) : HubDirectory.Default);
services.AddSingleton<TrackingEventParser>();
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
    services.AddSingleton(settings.DeadLetterKafka);
    services.AddSingleton(sp => new KafkaDeadLetterSink(settings.DeadLetterKafka, settings.Kafka));
    services.AddSingleton<IDeadLetterSink>(sp => sp.GetRequiredService<KafkaDeadLetterSink>());
}

services.AddSingleton<Batcher>();
services.AddSingleton<BatchProcessor>();
services.AddSingleton<EtlPipeline>();
services.AddHostedService<EtlWorkerService>();

var app = builder.Build();

// Liveness: the process is up and the pipeline has not faulted.
app.MapGet("/healthz", (EtlHealth health) =>
    health.Fault is null ? Results.Text("ok") : Results.Text(health.Fault, statusCode: StatusCodes.Status503ServiceUnavailable));

// Readiness: the warehouse is accepting inserts (fails while inserts keep failing, so alerts fire and traffic-based autoscalers back off).
app.MapGet("/readyz", (EtlHealth health, PipelineOptions options) =>
    health.IsReady(options.UnhealthyAfterConsecutiveFailures)
        ? Results.Text("ready")
        : Results.Text($"not ready: {health.Fault ?? health.LastError}", statusCode: StatusCodes.Status503ServiceUnavailable));

app.MapGet("/metrics", (EtlMetrics metrics) => Results.Text(metrics.RenderPrometheus(), "text/plain; version=0.0.4"));

app.Run();
