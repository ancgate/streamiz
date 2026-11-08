using Streamiz.Examples.Common;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Examples.Windowing;

/// <summary>
/// Sliding windows: windows defined by the records themselves, covering every set of records that are at
/// most a time difference apart. Unlike tumbling or hopping windows, no burst is cut in two by a window boundary.
/// Here: alert when a user has 3 failed logins within 10 seconds.
/// </summary>
public class SlidingFailedLogins : IExample
{
    public const string InputTopic = "failed-logins";
    public const string OutputTopic = "login-alerts";
    public const string StoreName = "failed-logins-store";
    public static readonly TimeSpan TimeDifference = TimeSpan.FromSeconds(10);
    public const long Threshold = 3;

    public string Name => "sliding-failed-logins";
    public string Description => "Alert on 3 failed logins of a user within 10 seconds (sliding windows).";
    public IEnumerable<string> InputTopics => new[] { InputTopic };

    public void BuildTopology(StreamBuilder builder)
    {
        // Failed logins: key = user, value = IP address.
        builder.Stream<string, string>(InputTopic)
            .GroupByKey()
            // A window [start, start + 10s] (both ends included) for every group of records within 10 seconds.
            .WindowedBy(SlidingWindowOptions.Of(TimeDifference))
            .Count(InMemoryWindows.As<string, long>(StoreName))
            .ToStream()
            // Keep only the windows that reach the threshold.
            .Filter((window, count, _) => count >= Threshold)
            // Sliding windows have the size of the time difference.
            .To(OutputTopic,
                new TimeWindowedSerDes<string>(new StringSerDes(), (long)TimeDifference.TotalMilliseconds),
                new Int64SerDes());
    }

    public void Configure(StreamConfig config)
    {
    }
}
