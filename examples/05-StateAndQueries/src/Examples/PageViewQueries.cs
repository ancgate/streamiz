using Streamiz.Kafka.Net.State;

namespace Streamiz.Examples.StateAndQueries.Examples;

/// <summary>
/// Read-only queries on the stores of <see cref="LocalQueriesExample"/>.
/// They only use the read-only store interfaces, so they work on the stores of a running application
/// (<c>KafkaStream.Store(...)</c>) and on the stores of the <c>TopologyTestDriver</c> in the tests.
/// </summary>
public static class PageViewQueries
{
    /// <summary>Total views of a page (a missing key returns the default value of long: 0).</summary>
    public static long ViewsOf(IReadOnlyKeyValueStore<string, long> store, string page)
        => store.Get(page);

    /// <summary>The 3 most viewed pages. <c>All()</c> scans the whole local store: fine for small stores.</summary>
    public static List<(string Page, long Views)> TopPages(IReadOnlyKeyValueStore<string, long> store)
        => store.All()
            .ToList()
            .OrderByDescending(kv => kv.Value)
            .Take(3)
            .Select(kv => (kv.Key, kv.Value))
            .ToList();

    /// <summary>Views of a page per minute, for the windows starting between <paramref name="from"/> and <paramref name="to"/>.</summary>
    public static List<(DateTime WindowStart, long Views)> ViewsPerMinute(
        IReadOnlyWindowStore<string, long> store, string page, DateTime from, DateTime to)
    {
        // Fetch(key, from, to) returns the windows of the key by start time
        using var windows = store.Fetch(page, from, to);
        var result = new List<(DateTime, long)>();
        while (windows.MoveNext())
        {
            var window = windows.Current.Value;
            result.Add((DateTimeOffset.FromUnixTimeMilliseconds(window.Key).UtcDateTime, window.Value));
        }

        return result;
    }

    /// <summary>The sessions of a user (start, end, number of views), oldest first.</summary>
    public static List<(DateTime Start, DateTime End, long Views)> SessionsOf(
        IReadOnlySessionStore<string, long?> store, string user)
    {
        // Fetch(key) returns every session of the key
        using var sessions = store.Fetch(user);
        var result = new List<(DateTime, DateTime, long)>();
        while (sessions.MoveNext())
        {
            var session = sessions.Current.Value;
            result.Add((DateTimeOffset.FromUnixTimeMilliseconds(session.Key.Window.StartMs).UtcDateTime,
                DateTimeOffset.FromUnixTimeMilliseconds(session.Key.Window.EndMs).UtcDateTime,
                session.Value ?? 0));
        }

        return result;
    }

    /// <summary>The profile a user had at a given time (a versioned store keeps the history).</summary>
    public static string ProfileAt(IReadOnlyVersionedKeyValueStore<string, string> store, string user, DateTime time)
        => store.Get(user, new DateTimeOffset(time).ToUnixTimeMilliseconds())?.Value;

    /// <summary>The current profile of a user.</summary>
    public static string CurrentProfile(IReadOnlyVersionedKeyValueStore<string, string> store, string user)
        => store.Get(user)?.Value;
}
