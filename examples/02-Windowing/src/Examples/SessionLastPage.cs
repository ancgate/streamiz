using Streamiz.Examples.Common;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Examples.Windowing;

/// <summary>
/// Session Reduce: combine the values of a session two by two. Here: the last page of each visit (the exit page).
/// </summary>
public class SessionLastPage : IExample
{
    public const string InputTopic = "page-views-for-exit";
    public const string OutputTopic = "exit-page-per-session";
    public const string StoreName = "exit-page-store";
    public static readonly TimeSpan InactivityGap = TimeSpan.FromMinutes(5);

    public string Name => "session-last-page";
    public string Description => "The exit page of each user visit (session Reduce).";
    public IEnumerable<string> InputTopics => new[] { InputTopic };

    public void BuildTopology(StreamBuilder builder)
    {
        // Page views: key = user, value = page.
        builder.Stream<string, string>(InputTopic)
            .GroupByKey()
            .WindowedBy(SessionWindowOptions.Of(InactivityGap))
            // The reducer keeps the latest page. When sessions merge, it is also used to combine them
            // (the older session's value first).
            .Reduce((previousPage, page) => page, InMemorySessions.As<string, string>(StoreName))
            .ToStream()
            .To(OutputTopic, new SessionWindowedSerDes<string>(new StringSerDes()), new StringSerDes());
    }

    public void Configure(StreamConfig config)
    {
    }
}
