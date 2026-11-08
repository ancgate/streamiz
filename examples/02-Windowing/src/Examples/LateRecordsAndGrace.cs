using Streamiz.Examples.Common;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Examples.Windowing;

/// <summary>
/// Grace period: how long a window still accepts out-of-order (late) records after it ends. Stream time is the
/// highest timestamp seen so far; a window closes when stream time passes its end + grace, and later records
/// for it are dropped (and counted in the dropped-records metric).
/// </summary>
public class LateRecordsAndGrace : IExample
{
    public const string InputTopic = "orders-per-minute";
    public const string OutputTopic = "orders-per-minute-counts";
    public static readonly TimeSpan WindowSize = TimeSpan.FromMinutes(1);
    public static readonly TimeSpan Grace = TimeSpan.FromSeconds(30);

    public string Name => "late-records-grace";
    public string Description => "A late record is counted within the grace period, and dropped after it.";
    public IEnumerable<string> InputTopics => new[] { InputTopic };

    public void BuildTopology(StreamBuilder builder)
    {
        // Orders: key = shop, value = order id.
        builder.Stream<string, string>(InputTopic)
            .GroupByKey()
            // Without a grace argument, a tumbling window accepts late records for one day (retention - size).
            .WindowedBy(TumblingWindowOptions.Of(WindowSize, Grace))
            .Count(InMemoryWindows.As<string, long>("orders-per-minute-store"))
            .ToStream()
            .To(OutputTopic,
                new TimeWindowedSerDes<string>(new StringSerDes(), (long)WindowSize.TotalMilliseconds),
                new Int64SerDes());
    }

    public void Configure(StreamConfig config)
    {
    }
}
