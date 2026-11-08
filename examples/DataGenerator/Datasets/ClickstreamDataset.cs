namespace Streamiz.Examples.DataGenerator.Datasets;

/// <summary>
/// Page views of a shop's website over 2 days (2026-01-05 and 2026-01-06, UTC): 1000 visitors browsing in
/// sessions (at least 45 minutes apart, at most 3 minutes between two views of a session) that follow the funnel
/// home -> category -> product -> cart -> checkout -> confirmation, plus 3 bots.
/// </summary>
public static class ClickstreamDataset
{
    public static readonly DateTime Start = Json.Utc(2026, 1, 5);
    public const int Days = 2;
    public const int Users = 1000;
    public static readonly TimeSpan SessionGap = TimeSpan.FromMinutes(30);

    public const string CrawlerId = "BOT-CRAWLER";
    public const string CrawlerUserAgent = "ExampleCrawler/1.0 (+https://example.com/bot)";
    public static readonly string[] Scrapers = { "BOT-SCRAPER-1", "BOT-SCRAPER-2" };

    /// <summary>Bursts of the scrapers: 400 page views in about 2 minutes, starting at these times.</summary>
    public static readonly (string UserId, DateTime Start)[] Bursts =
    {
        (Scrapers[0], Start.AddHours(3).AddMinutes(12)),
        (Scrapers[0], Start.AddDays(1).AddHours(2).AddMinutes(40)),
        (Scrapers[1], Start.AddHours(15).AddMinutes(5)),
    };

    private static readonly string[] UserAgents =
    {
        "Mozilla/5.0 (Windows NT 10.0; Win64) Chrome/130.0",
        "Mozilla/5.0 (Macintosh; Mac OS X 14_6) Safari/605.1",
        "Mozilla/5.0 (iPhone; iOS 18_0) Mobile Safari/605.1",
        "Mozilla/5.0 (Linux; Android 15) Chrome/130.0 Mobile",
        "Mozilla/5.0 (X11; Linux x86_64) Firefox/132.0"
    };

    private static readonly string[] CategoryPages =
        { "electronics", "books", "home", "clothing", "sports", "toys", "beauty", "grocery" };

    private static readonly double[] HourWeights =
        { 1, 1, 1, 1, 1, 2, 3, 5, 7, 8, 8, 9, 10, 9, 8, 8, 9, 10, 11, 12, 11, 8, 5, 2 };

    public static List<PageView> Generate(int seed)
    {
        var rng = new Rng(seed);
        var views = new List<PageView>();

        for (var u = 1; u <= Users; ++u)
        {
            var userId = $"U{u:D4}";
            var userAgent = rng.Pick(UserAgents);
            var sessionCount = rng.Weighted(new double[] { 35, 30, 20, 15 }) + 1;
            var starts = Enumerable.Range(0, sessionCount)
                .Select(_ => Start.AddDays(rng.Int(0, Days)).AddHours(rng.Weighted(HourWeights)).AddSeconds(rng.Int(0, 3600)))
                .OrderBy(t => t)
                .ToList();

            var previousEnd = DateTime.MinValue;
            foreach (var plannedStart in starts)
            {
                // sessions are at least 45 minutes apart: with a 30-minute session gap, each one is a separate session
                var start = plannedStart < previousEnd.AddMinutes(45) ? previousEnd.AddMinutes(45).AddSeconds(rng.Int(0, 600)) : plannedStart;
                if (start >= Start.AddDays(Days).AddHours(-1))
                    break;

                var at = start;
                foreach (var page in Session(rng))
                {
                    views.Add(new PageView(userId, page, userAgent, at));
                    previousEnd = at;
                    at += rng.Seconds(5, 180); // at most 3 minutes between 2 views of a session
                }
            }
        }

        // a declared crawler: steady, 2 pages a minute for the whole period, identified by its user agent
        for (var t = Start.AddSeconds(17); t < Start.AddDays(Days); t = t.AddSeconds(rng.Int(20, 40)))
            views.Add(new PageView(CrawlerId, $"/product/P{rng.Int(1, 201):D4}", CrawlerUserAgent, t));

        // two scrapers hidden behind a browser user agent: bursts of ~200 pages a minute
        foreach (var (scraper, burstStart) in Bursts)
        {
            var t = burstStart;
            for (var i = 0; i < 400; ++i)
            {
                views.Add(new PageView(scraper, $"/product/P{rng.Int(1, 201):D4}", UserAgents[0], t));
                t = t.AddMilliseconds(rng.Int(150, 450));
            }
        }

        return views.OrderBy(v => v.ViewedAt).ThenBy(v => v.UserId, StringComparer.Ordinal).ToList();
    }

    // One visit: entry page, some browsing, then each funnel step with a drop-off.
    private static IEnumerable<string> Session(Rng rng)
    {
        yield return rng.Chance(0.7) ? "/" : $"/category/{rng.Pick(CategoryPages)}";
        if (rng.Chance(0.6))
            yield return $"/category/{rng.Pick(CategoryPages)}";
        var products = rng.Int(0, 6);
        for (var i = 0; i < products; ++i)
            yield return $"/product/P{rng.Skewed(200) + 1:D4}";
        if (products == 0 || !rng.Chance(0.45))
            yield break;
        yield return "/cart";
        if (!rng.Chance(0.55))
            yield break;
        yield return "/checkout";
        if (rng.Chance(0.75))
            yield return "/confirmation";
    }

    public static void Write(string dataDir, int seed)
    {
        var views = Generate(seed);
        var dir = Path.Combine(dataDir, "clickstream");
        // one file per 12 hours, to keep the files small
        for (var part = Start; part < Start.AddDays(Days); part = part.AddHours(12))
        {
            var end = part.AddHours(12);
            Json.WriteLines(Path.Combine(dir, $"pageviews-{part:yyyy-MM-dd-HH}h.jsonl"),
                views.Where(v => v.ViewedAt >= part && v.ViewedAt < end));
        }

        var bots = new HashSet<string>(Scrapers.Append(CrawlerId));
        var sessions = Sessionize(views.Where(v => !bots.Contains(v.UserId)), SessionGap);
        Json.WriteFacts(Path.Combine(dir, "facts.json"), new
        {
            seed,
            pageViews = views.Count,
            humanPageViews = views.Count(v => !bots.Contains(v.UserId)),
            users = Users,
            sessionGapMinutes = SessionGap.TotalMinutes,
            humanSessions = sessions.Count,
            sessionsReachingCart = sessions.Count(s => s.Contains("/cart")),
            sessionsReachingCheckout = sessions.Count(s => s.Contains("/checkout")),
            sessionsConverted = sessions.Count(s => s.Contains("/confirmation")),
            crawler = new { userId = CrawlerId, userAgent = CrawlerUserAgent, pageViews = views.Count(v => v.UserId == CrawlerId) },
            scrapers = Scrapers.Select(s => new
            {
                userId = s,
                pageViews = views.Count(v => v.UserId == s),
                bursts = Bursts.Where(b => b.UserId == s).Select(b => b.Start)
            }),
            maxHumanViewsPerMinute = views.Where(v => !bots.Contains(v.UserId))
                .GroupBy(v => (v.UserId, v.ViewedAt.Ticks / TimeSpan.TicksPerMinute))
                .Max(g => g.Count())
        });
    }

    /// <summary>Splits each user's views into sessions: a new session starts after more than <paramref name="gap"/> of inactivity.</summary>
    public static List<List<string>> Sessionize(IEnumerable<PageView> views, TimeSpan gap)
    {
        var sessions = new List<List<string>>();
        foreach (var user in views.GroupBy(v => v.UserId))
        {
            List<string> current = null;
            var last = DateTime.MinValue;
            foreach (var view in user.OrderBy(v => v.ViewedAt))
            {
                if (current == null || view.ViewedAt - last > gap)
                {
                    current = new List<string>();
                    sessions.Add(current);
                }

                current.Add(view.Page);
                last = view.ViewedAt;
            }
        }

        return sessions;
    }
}
