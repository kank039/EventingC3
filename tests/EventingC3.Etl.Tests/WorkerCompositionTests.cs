using EventingC3.Etl.Core.Abstractions;
using EventingC3.Etl.Core.Pipeline;
using EventingC3.Etl.Worker;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace EventingC3.Etl.Tests;

public class WorkerCompositionTests
{
    [Theory]
    [InlineData("Kafka", "Kafka")]
    [InlineData("File", "File")]
    [InlineData("Kafka", "File")]
    public async Task Resolves_the_pipeline_and_disposes_cleanly(string source, string deadLetter)
    {
        var settings = new EtlSettings { Source = source, DeadLetter = deadLetter };
        settings.Files.InputDirectory = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N"));
        settings.DeadLetterFiles.Directory = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N"));
        settings.Validate();

        var services = new ServiceCollection()
            .AddSingleton<ILoggerFactory>(NullLoggerFactory.Instance)
            .AddSingleton(typeof(ILogger<>), typeof(Logger<>))
            .AddEtlPipeline(settings);

        // Regression: a duplicate registration once made the host dispose the Kafka producer twice and crash on shutdown.
        await using (var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateOnBuild = true, ValidateScopes = true }))
        {
            Assert.NotNull(provider.GetRequiredService<EtlPipeline>());
            Assert.NotNull(provider.GetRequiredService<IDeadLetterSink>());
        }
    }

    [Fact]
    public void Rejects_settings_that_break_atomic_inserts()
    {
        var settings = new EtlSettings();
        settings.Pipeline.MaxBatchRecords = 2_000_000;
        Assert.Throws<InvalidOperationException>(settings.Validate);
    }
}
