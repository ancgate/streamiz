using Streamiz.Examples.Common;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Examples.Windowing;

/// <summary>
/// Suppress UntilWindowClose: emit only the final result of each window, once the window is closed
/// (window end + grace period), instead of one update per record.
/// </summary>
public class TumblingFinalResults : IExample
{
    public const string InputTopic = "page-clicks-final";
    public const string OutputTopic = "clicks-per-minute-final";
    public static readonly TimeSpan WindowSize = TimeSpan.FromMinutes(1);
    public static readonly TimeSpan Grace = TimeSpan.FromSeconds(30);

    public string Name => "tumbling-final-results";
    public string Description => "Emit only the final click count of each minute, once the window closes (Suppress UntilWindowClose).";
    public IEnumerable<string> InputTopics => new[] { InputTopic };

    public void BuildTopology(StreamBuilder builder)
    {
        var windowedKeySerdes = new TimeWindowedSerDes<string>(new StringSerDes(), (long)WindowSize.TotalMilliseconds);

        builder.Stream<string, string>(InputTopic)
            .GroupByKey()
            // The grace period accepts records up to 30 seconds late: a window [08:00, 08:01) closes when the
            // stream time reaches 08:01:30.
            .WindowedBy(TumblingWindowOptions.Of(WindowSize, Grace))
            .Count(InMemoryWindows.As<string, long>("clicks-final-store"))
            // Buffer the updates of each window and emit the last one when the window closes.
            // Give the same grace period as the windows. Unbounded: the buffer never emits early.
            .Suppress(SuppressedBuilder.UntilWindowClose<Windowed<string>, long>(Grace, StrictBufferConfig.Unbounded())
                .WithKeySerdes(windowedKeySerdes)
                .WithValueSerdes(new Int64SerDes()))
            .ToStream()
            .To(OutputTopic, windowedKeySerdes, new Int64SerDes());
    }

    public void Configure(StreamConfig config)
    {
    }
}
