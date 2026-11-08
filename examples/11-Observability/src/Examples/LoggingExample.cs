using Microsoft.Extensions.Logging;
using Streamiz.Examples.Common;
using Streamiz.Kafka.Net;

namespace Streamiz.Examples.Observability.Examples;

/// <summary>
/// Logging: Streamiz logs through Microsoft.Extensions.Logging. Give it your logger factory with <c>config.Logger</c>,
/// and use the same factory for your own logs in the topology.
/// </summary>
public class LoggingExample : IExample
{
    private readonly ILoggerFactory loggerFactory;

    public LoggingExample() : this(LoggerFactory.Create(builder => builder
        .AddConsole()
        .SetMinimumLevel(LogLevel.Information)
        // Streamiz is verbose at debug level: raise its level, keep yours
        .AddFilter("Streamiz", LogLevel.Warning)))
    {
    }

    public LoggingExample(ILoggerFactory loggerFactory) => this.loggerFactory = loggerFactory;

    public string Name => "logging";

    public string Description => "Logs suspicious logins from the topology with the same logger factory as Streamiz.";

    public IEnumerable<string> InputTopics => new[] { "logins" };

    public void BuildTopology(StreamBuilder builder)
    {
        var logger = loggerFactory.CreateLogger("Examples.Logins");

        // key: user, value: country of the login
        builder.Stream<string, string>("logins")
            .Filter((_, country, _) => country != "FR")
            // Peek: a side effect (here a structured log) without changing the stream
            .Peek((user, country, context) => logger.LogWarning(
                "Login of {User} from {Country} (partition {Partition}, offset {Offset})",
                user, country, context.Partition, context.Offset))
            .To("suspicious-logins");
    }

    public void Configure(StreamConfig config)
    {
        // Streamiz's own logs (threads, tasks, rebalances, errors) go through this factory too
        config.Logger = loggerFactory;
    }
}
