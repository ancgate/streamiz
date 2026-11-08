using Streamiz.Examples.Common;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Examples.Windowing;

/// <summary>
/// Session windows: a session of a key lasts as long as its records are less than an inactivity gap apart.
/// When a record falls between two sessions, they are merged into one. Here: page views per visit.
/// </summary>
public class SessionCount : IExample
{
    public const string InputTopic = "page-views";
    public const string OutputTopic = "page-views-per-session";
    public const string StoreName = "user-sessions-store";
    public static readonly TimeSpan InactivityGap = TimeSpan.FromMinutes(5);

    public string Name => "session-count";
    public string Description => "Count the page views of each user visit (session windows with a 5-minute inactivity gap).";
    public IEnumerable<string> InputTopics => new[] { InputTopic };

    public void BuildTopology(StreamBuilder builder)
    {
        // Page views: key = user, value = page.
        builder.Stream<string, string>(InputTopic)
            .GroupByKey()
            .WindowedBy(SessionWindowOptions.Of(InactivityGap))
            // The session Count returns long?: when sessions are merged, the old sessions are deleted
            // downstream with a null count (a tombstone), before the merged session is emitted.
            .Count(InMemorySessions.As<string, long?>(StoreName))
            .ToStream()
            // A session window has a start and an end: SessionWindowedSerDes writes both.
            // NullableInt64SerDes writes a null count as a real Kafka tombstone (null value).
            .To(OutputTopic, new SessionWindowedSerDes<string>(new StringSerDes()), new NullableInt64SerDes());
    }

    public void Configure(StreamConfig config)
    {
    }
}
