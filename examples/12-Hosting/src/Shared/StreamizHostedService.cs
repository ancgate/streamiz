using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Streamiz.Kafka.Net;

namespace Streamiz.Examples.Hosting;

/// <summary>
/// Runs the KafkaStream for the lifetime of the host: started with the host, disposed (cleanly closed,
/// offsets committed) when the host stops (Ctrl+C, SIGTERM from Kubernetes...).
/// </summary>
public class StreamizHostedService : BackgroundService
{
    private readonly StreamizSettings settings;
    private readonly OrderTotalsTopology topology;
    private readonly StreamHealth health;
    private readonly ILoggerFactory loggerFactory;
    private readonly ILogger<StreamizHostedService> logger;

    public StreamizHostedService(StreamizSettings settings, OrderTotalsTopology topology, StreamHealth health,
        ILoggerFactory loggerFactory)
    {
        this.settings = settings;
        this.topology = topology;
        this.health = health;
        this.loggerFactory = loggerFactory;
        logger = loggerFactory.CreateLogger<StreamizHostedService>();
    }

    /// <summary>The running stream, e.g. to query its state stores. Null until the service has started.</summary>
    public KafkaStream Stream { get; private set; }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var config = settings.ToStreamConfig();
        // Streamiz logs through the host's logging (console, Application Insights, Serilog...)
        config.Logger = loggerFactory;

        Stream = new KafkaStream(topology.Build(), config);
        Stream.StateChanged += health.OnStateChanged;
        Stream.StateChanged += (oldState, newState) =>
            logger.LogInformation("Stream state {OldState} -> {NewState}", oldState, newState);

        await Stream.StartAsync(stoppingToken);
        try
        {
            await Task.Delay(Timeout.Infinite, stoppingToken);
        }
        catch (TaskCanceledException)
        {
            // the host is stopping
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        await base.StopAsync(cancellationToken);
        Stream?.Dispose();
    }
}
