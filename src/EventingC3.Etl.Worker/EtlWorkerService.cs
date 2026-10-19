using EventingC3.Etl.Core.Pipeline;
using EventingC3.Etl.Infrastructure.ClickHouse;

namespace EventingC3.Etl.Worker;

/// <summary>Hosts the pipeline: optional schema bootstrap, then run until shutdown (or until a bulk source is exhausted).</summary>
public sealed class EtlWorkerService : BackgroundService
{
    private readonly EtlPipeline _pipeline;
    private readonly ClickHouseSink _sink;
    private readonly EtlSettings _settings;
    private readonly IHostApplicationLifetime _lifetime;
    private readonly ILogger<EtlWorkerService> _logger;

    public EtlWorkerService(
        EtlPipeline pipeline,
        ClickHouseSink sink,
        EtlSettings settings,
        IHostApplicationLifetime lifetime,
        ILogger<EtlWorkerService> logger)
    {
        _pipeline = pipeline;
        _sink = sink;
        _settings = settings;
        _lifetime = lifetime;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (_settings.ClickHouse.ApplySchemaOnStartup)
        {
            await ApplySchemaWithRetryAsync(stoppingToken);
        }

        _logger.LogInformation("Starting ETL pipeline: source={Source}, dead-letter={DeadLetter}, batch={Batch} rows, concurrency={Concurrency}",
            _settings.Source, _settings.DeadLetter, _settings.Pipeline.MaxBatchRecords, _settings.Pipeline.MaxConcurrentBatches);

        await _pipeline.RunAsync(stoppingToken);

        if (!stoppingToken.IsCancellationRequested)
        {
            // Bounded source (file backfill) finished: exit so the job completes.
            _logger.LogInformation("Source exhausted; stopping worker");
            _lifetime.StopApplication();
        }
    }

    private async Task ApplySchemaWithRetryAsync(CancellationToken cancellationToken)
    {
        for (var attempt = 1; ; attempt++)
        {
            try
            {
                await _sink.ApplySchemaAsync(cancellationToken);
                return;
            }
            catch (Exception ex) when (!cancellationToken.IsCancellationRequested && attempt < 30)
            {
                var delay = Backoff.Delay(attempt, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(15));
                _logger.LogWarning("Applying ClickHouse schema failed (attempt {Attempt}): {Error}; retrying in {Delay}", attempt, ex.Message, delay);
                await Task.Delay(delay, cancellationToken);
            }
        }
    }
}
