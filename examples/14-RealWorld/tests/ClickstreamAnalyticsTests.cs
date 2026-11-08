using NUnit.Framework;
using Streamiz.Examples.RealWorld.Examples;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;

namespace Streamiz.Examples.RealWorld.Tests;

/// <summary>2 days of page views: 1000 users, a declared crawler and 2 scrapers hiding behind a browser user agent.</summary>
public class ClickstreamAnalyticsTests
{
    private const string Crawler = "BOT-CRAWLER";
    private TopologyTestDriver driver;
    private List<PageView> views;

    [OneTimeSetUp]
    public void Run()
    {
        var records = Scenario.Load("clickstream");
        views = records.Select(r => r.As<PageView>()).ToList();

        driver = Scenario.Create(new ClickstreamAnalytics());
        Scenario.Pipe(driver, records);
    }

    [OneTimeTearDown]
    public void Dispose() => driver.Dispose();

    private List<PageView> NotCrawler => views.Where(v => v.UserId != Crawler).ToList();

    private static bool IsScraper(string userId) => userId.StartsWith("BOT-SCRAPER", StringComparison.Ordinal);

    /// <summary>The sessions at the end: the last value of each session window, without the ones deleted by a merge.</summary>
    private Dictionary<(string User, DateTime Start, DateTime End), SessionSummary> FinalSessions()
    {
        var sessions = new Dictionary<(string, DateTime, DateTime), SessionSummary>();
        foreach (var r in Scenario.ReadAll(driver, ClickstreamAnalytics.SessionsTopic,
                     new SessionWindowedSerDes<string>(new StringSerDes()), new CamelJsonSerDes<SessionSummary>()))
        {
            var key = (r.Message.Key.Key, Scenario.Utc(r.Message.Key.Window.StartMs), Scenario.Utc(r.Message.Key.Window.EndMs));
            if (r.Message.Value == null)
                sessions.Remove(key);
            else
                sessions[key] = r.Message.Value;
        }

        return sessions;
    }

    [Test]
    public void SessionsMatchTheVisits()
    {
        var sessions = FinalSessions();

        // independently: sort each user's views, a gap of more than 30 minutes starts a new session
        var expected = NotCrawler.GroupBy(v => v.UserId).SelectMany(g =>
        {
            var result = new List<((string, DateTime, DateTime), SessionSummary)>();
            var ordered = g.OrderBy(v => v.ViewedAt).ToList();
            var start = 0;
            for (var i = 1; i <= ordered.Count; i++)
            {
                if (i < ordered.Count && ordered[i].ViewedAt - ordered[i - 1].ViewedAt <= ClickstreamAnalytics.SessionGap)
                    continue;
                var session = ordered.Skip(start).Take(i - start).ToList();
                result.Add(((g.Key, session[0].ViewedAt, session[^1].ViewedAt),
                    new SessionSummary(session.Count, session.Max(v => SessionSummary.StepOf(v.Page)))));
                start = i;
            }

            return result;
        }).ToDictionary(x => x.Item1, x => x.Item2);

        Scenario.AssertSame(expected, sessions);

        // documented facts about the human sessions (the scrapers are not humans, but they look like ones)
        var facts = Scenario.Facts("clickstream");
        var human = sessions.Where(s => !IsScraper(s.Key.User)).Select(s => s.Value).ToList();
        Assert.AreEqual(facts.GetProperty("humanSessions").GetInt32(), human.Count); // 2118
        Assert.AreEqual(facts.GetProperty("humanPageViews").GetInt32(), human.Sum(s => s.Views)); // 10134
        Assert.AreEqual(facts.GetProperty("sessionsReachingCart").GetInt32(), human.Count(s => s.FunnelStep >= 2)); // 778
        Assert.AreEqual(facts.GetProperty("sessionsReachingCheckout").GetInt32(), human.Count(s => s.FunnelStep >= 3)); // 446
        Assert.AreEqual(facts.GetProperty("sessionsConverted").GetInt32(), human.Count(s => s.Converted)); // 331
    }

    [Test]
    public void TheCrawlerIsFilteredOut()
        => Assert.IsFalse(FinalSessions().Keys.Any(k => k.User == Crawler));

    [Test]
    public void PageViewsPerMinuteAndPageType()
    {
        // the first 12 hours only, in a driver of their own (the TopologyTestDriver reads big output topics slowly)
        var slice = Scenario.Load("clickstream").Where(r => r.Timestamp < new DateTime(2026, 1, 5, 12, 0, 0, DateTimeKind.Utc)).ToList();
        using var sliceDriver = Scenario.Create(new ClickstreamAnalytics());
        Scenario.Pipe(sliceDriver, slice);

        var expected = slice.Select(r => r.As<PageView>()).Where(v => v.UserId != Crawler)
            .GroupBy(v => (ClickstreamAnalytics.PageType(v.Page), Scenario.Floor(v.ViewedAt, TimeSpan.FromMinutes(1))))
            .ToDictionary(g => g.Key, g => (long)g.Count());

        var actual = Scenario.FinalPerWindow(Scenario.ReadAll(sliceDriver, ClickstreamAnalytics.PageViewsPerMinuteTopic,
            new TimeWindowedSerDes<string>(new StringSerDes(), 60_000), new Int64SerDes()));

        Scenario.AssertSame(expected, actual); // the crawler's views aren't counted
        CollectionAssert.AreEquivalent(new[] { "home", "category", "product", "cart", "checkout", "confirmation" },
            actual.Keys.Select(k => k.Key).Distinct());
    }

    [Test]
    public void ScrapersAreFlaggedByTheirRate()
    {
        var suspects = Scenario.ReadAll(driver, ClickstreamAnalytics.BotSuspectsTopic, new StringSerDes(), new StringSerDes())
            .Select(r => r.Message.Key).ToList();

        // independently: users with more than 60 views in a minute
        var expected = NotCrawler.GroupBy(v => (v.UserId, Scenario.Floor(v.ViewedAt, TimeSpan.FromMinutes(1))))
            .Where(g => g.Count() > ClickstreamAnalytics.BotThresholdPerMinute)
            .Select(g => g.Key.UserId).ToList();
        CollectionAssert.AreEquivalent(expected, suspects); // one alert per user and minute

        // documented: only the 2 scrapers; no human gets close (at most 4 views in a minute)
        var scrapers = Scenario.Facts("clickstream").GetProperty("scrapers").EnumerateArray().Select(s => s.GetProperty("userId").GetString());
        CollectionAssert.AreEquivalent(scrapers, suspects.Distinct());
    }
}
