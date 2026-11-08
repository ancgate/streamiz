using Streamiz.Examples.Common;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Examples.Windowing;

/// <summary>
/// Suppress UntilTimeLimit: rate-limit the updates of a table. The latest value of a key is emitted once
/// the time limit has passed since its first buffered update, measured in stream time (record timestamps).
/// </summary>
public class RateLimitStreamTime : IExample
{
    public const string InputTopic = "stock-prices";
    public const string OutputTopic = "stock-prices-per-minute";
    public static readonly TimeSpan Limit = TimeSpan.FromMinutes(1);

    public string Name => "rate-limit-stream-time";
    public string Description => "Emit at most one price per stock per minute of stream time (Suppress UntilTimeLimit).";
    public IEnumerable<string> InputTopics => new[] { InputTopic };

    public void BuildTopology(StreamBuilder builder)
    {
        // Prices: key = stock symbol, value = price. The latest price of each symbol is the table.
        builder.Table(InputTopic, new StringSerDes(), new DoubleSerDes(), InMemory.As<string, double>("stock-prices-store"))
            // The first update of a key starts its timer; the following ones replace the buffered value without
            // restarting it. Stream time only moves forward with new records: without new prices, nothing is emitted.
            .Suppress(SuppressedBuilder.UntilTimeLimit<string, double>(Limit, StrictBufferConfig.Unbounded())
                .WithKeySerdes(new StringSerDes())
                .WithValueSerdes(new DoubleSerDes()))
            .ToStream()
            .To(OutputTopic, new StringSerDes(), new DoubleSerDes());
    }

    public void Configure(StreamConfig config)
    {
    }
}
