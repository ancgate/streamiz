using Streamiz.Examples.Common;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Examples.Windowing;

/// <summary>
/// Suppress UntilWallClockTimeLimit: like UntilTimeLimit, but the limit is measured in real (wall-clock) time,
/// so the latest value is emitted even if no new record arrives (KIP-424).
/// </summary>
public class RateLimitWallClock : IExample
{
    public const string InputTopic = "stock-prices-wall-clock";
    public const string OutputTopic = "stock-prices-every-minute";
    public static readonly TimeSpan Limit = TimeSpan.FromMinutes(1);

    public string Name => "rate-limit-wall-clock";
    public string Description => "Emit the latest price of each stock one minute (wall clock) after its first change.";
    public IEnumerable<string> InputTopics => new[] { InputTopic };

    public void BuildTopology(StreamBuilder builder)
    {
        builder.Table(InputTopic, new StringSerDes(), new DoubleSerDes(), InMemory.As<string, double>("stock-prices-wall-clock-store"))
            // A processing-time punctuation emits the buffered value when its minute is over, with no new input.
            .Suppress(SuppressedBuilder.UntilWallClockTimeLimit<string, double>(Limit, StrictBufferConfig.Unbounded())
                .WithKeySerdes(new StringSerDes())
                .WithValueSerdes(new DoubleSerDes()))
            .ToStream()
            .To(OutputTopic, new StringSerDes(), new DoubleSerDes());
    }

    public void Configure(StreamConfig config)
    {
    }
}
