using Streamiz.Examples.Common;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Examples.Windowing;

/// <summary>
/// Tumbling windows: fixed-size, non-overlapping windows. Each click falls in exactly one one-minute window.
/// </summary>
public class TumblingClicksPerMinute : IExample
{
    public const string InputTopic = "page-clicks";
    public const string OutputTopic = "clicks-per-minute";
    public const string StoreName = "clicks-per-minute-store";
    public static readonly TimeSpan WindowSize = TimeSpan.FromMinutes(1);

    public string Name => "tumbling-clicks-per-minute";
    public string Description => "Count the clicks of each page per minute with tumbling windows.";
    public IEnumerable<string> InputTopics => new[] { InputTopic };

    public void BuildTopology(StreamBuilder builder)
    {
        // Clicks: key = page, value = user. Windows use the record timestamp (event time).
        builder.Stream<string, string>(InputTopic)
            .GroupByKey()
            // [08:00, 08:01), [08:01, 08:02)... A window is identified by its start.
            .WindowedBy(TumblingWindowOptions.Of(WindowSize))
            // The counts are kept in a window store: one entry per (page, window).
            .Count(InMemoryWindows.As<string, long>(StoreName))
            // The result is a table keyed by Windowed<string> (the page + its window); each click updates it.
            .ToStream()
            // A windowed key needs a windowed serdes: it writes the page and the window start.
            // Read the topic back with the same serdes (see read-windowed-results).
            .To(OutputTopic,
                new TimeWindowedSerDes<string>(new StringSerDes(), (long)WindowSize.TotalMilliseconds),
                new Int64SerDes());
    }

    public void Configure(StreamConfig config)
    {
    }
}
