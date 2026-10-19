using EventingC3.Etl.Core.Observability;
using EventingC3.Etl.Core.Pipeline;
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

builder.Services.AddEtlPipeline(settings);
builder.Services.AddHostedService<EtlWorkerService>();

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
