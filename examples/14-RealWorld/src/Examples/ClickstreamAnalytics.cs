using Streamiz.Examples.Common;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Examples.RealWorld.Examples;

/// <summary>
/// Clickstream analytics (dataset: clickstream): drop declared crawlers, flag scrapers by their rate,
/// rebuild visits with session windows (30 minutes of inactivity ends a session) and count page views per minute.
/// </summary>
public class ClickstreamAnalytics : IExample
{
    public const string PageViewsTopic = "clickstream.pageviews";
    public const string SessionsTopic = "clickstream.sessions";
    public const string PageViewsPerMinuteTopic = "clickstream.pageviews-per-minute";
    public const string BotSuspectsTopic = "clickstream.bot-suspects";
    public const int BotThresholdPerMinute = 60;
    public static readonly TimeSpan SessionGap = TimeSpan.FromMinutes(30);

    public string Name => "clickstream-analytics";

    public string Description => "Sessions (session windows) with funnel steps, page views per minute, crawlers filtered and scrapers flagged.";

    public IEnumerable<string> InputTopics => new[] { PageViewsTopic };

    public void BuildTopology(StreamBuilder builder)
    {
        // page views are keyed by user id; declared crawlers say so in their user agent: drop them
        var views = builder.Stream(PageViewsTopic, new StringSerDes(), new CamelJsonSerDes<PageView>())
            .FilterNot((_, view, _) => view.UserAgent.Contains("bot", StringComparison.OrdinalIgnoreCase)
                                       || view.UserAgent.Contains("crawler", StringComparison.OrdinalIgnoreCase));

        // scrapers hide behind a browser user agent, but no human opens 60 pages a minute
        views.GroupByKey()
            .WindowedBy(TumblingWindowOptions.Of(TimeSpan.FromMinutes(1)))
            .Count(InMemoryWindows.As<string, long>("views-per-user-minute"))
            .ToStream()
            .Filter((_, count, _) => count == BotThresholdPerMinute + 1) // once per minute, when the threshold is crossed
            .Map((window, count, _) => KeyValuePair.Create(window.Key,
                $"more than {BotThresholdPerMinute} page views in the minute starting {DateTimeOffset.FromUnixTimeMilliseconds(window.Window.StartMs):yyyy-MM-dd HH:mm}"))
            .To(BotSuspectsTopic, new StringSerDes(), new StringSerDes());

        // sessions: a user's views separated by less than 30 minutes form one session.
        // When a view bridges two sessions, they're merged: the old ones are deleted (null value) and the merged one emitted.
        views.GroupByKey()
            .WindowedBy(SessionWindowOptions.Of(SessionGap))
            .Aggregate(() => SessionSummary.Empty, (_, view, session) => session.Add(view.Page),
                (_, left, right) => left.Merge(right),
                InMemorySessions.As<string, SessionSummary>("sessions").WithValueSerdes(new CamelJsonSerDes<SessionSummary>()))
            .ToStream()
            .To(SessionsTopic, new SessionWindowedSerDes<string>(new StringSerDes()), new CamelJsonSerDes<SessionSummary>());

        // page views per minute and per page type (home, category, product, cart, checkout, confirmation)
        views.GroupBy<string, StringSerDes, CamelJsonSerDes<PageView>>((_, view, _) => PageType(view.Page))
            .WindowedBy(TumblingWindowOptions.Of(TimeSpan.FromMinutes(1)))
            .Count(InMemoryWindows.As<string, long>("views-per-page-type-minute"))
            .ToStream()
            .To(PageViewsPerMinuteTopic, new TimeWindowedSerDes<string>(new StringSerDes(), 60_000), new Int64SerDes());
    }

    public static string PageType(string page)
        => page == "/" ? "home" : page.Split('/', StringSplitOptions.RemoveEmptyEntries)[0];

    public void Configure(StreamConfig config)
    {
    }
}

/// <summary>A visit: number of page views and the furthest funnel step reached.</summary>
public record SessionSummary(int Views, int FunnelStep)
{
    public static readonly string[] Funnel = { "browse", "product", "cart", "checkout", "confirmation" };
    public static readonly SessionSummary Empty = new(0, 0);

    public bool Converted => FunnelStep == Funnel.Length - 1;

    public SessionSummary Add(string page) => new(Views + 1, Math.Max(FunnelStep, StepOf(page)));

    public SessionSummary Merge(SessionSummary other) => new(Views + other.Views, Math.Max(FunnelStep, other.FunnelStep));

    public static int StepOf(string page) => page switch
    {
        "/confirmation" => 4,
        "/checkout" => 3,
        "/cart" => 2,
        _ when page.StartsWith("/product/", StringComparison.Ordinal) => 1,
        _ => 0
    };
}
