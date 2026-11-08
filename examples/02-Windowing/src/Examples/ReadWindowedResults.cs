using Streamiz.Examples.Common;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;

namespace Streamiz.Examples.Windowing;

/// <summary>
/// Reading a topic written with windowed keys: use the same windowed serdes (with the same window size),
/// then use the key and the window like any other value. Here the output of tumbling-clicks-per-minute
/// becomes readable text.
/// </summary>
public class ReadWindowedResults : IExample
{
    public const string InputTopic = TumblingClicksPerMinute.OutputTopic;
    public const string OutputTopic = "clicks-per-minute-readable";

    public string Name => "read-windowed-results";
    public string Description => "Read the windowed output of tumbling-clicks-per-minute and turn it into text.";
    public IEnumerable<string> InputTopics => new[] { InputTopic };

    public void BuildTopology(StreamBuilder builder)
    {
        // TimeWindowedSerDes stores the window start only: the window size rebuilds the end.
        // (SessionWindowedSerDes stores both, for sessions.)
        var windowedKeySerdes = new TimeWindowedSerDes<string>(new StringSerDes(),
            (long)TumblingClicksPerMinute.WindowSize.TotalMilliseconds);

        builder.Stream(InputTopic, windowedKeySerdes, new Int64SerDes())
            // windowedKey.Key is the page, windowedKey.Window its window (StartMs / EndMs, unix milliseconds).
            .Map((windowedKey, count, _) => KeyValuePair.Create(
                windowedKey.Key,
                $"{count} clicks from {Format(windowedKey.Window.StartMs)} to {Format(windowedKey.Window.EndMs)}"))
            .To(OutputTopic, new StringSerDes(), new StringSerDes());
    }

    private static string Format(long unixMs) => DateTimeOffset.FromUnixTimeMilliseconds(unixMs).UtcDateTime.ToString("HH:mm");

    public void Configure(StreamConfig config)
    {
    }
}
