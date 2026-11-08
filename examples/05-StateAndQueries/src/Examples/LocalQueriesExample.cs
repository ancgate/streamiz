using Streamiz.Examples.Common;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Examples.StateAndQueries.Examples;

/// <summary>
/// Materializes four kinds of stores and queries them from the running application with
/// <c>KafkaStream.Store(...)</c>: a key-value store, a window store, a session store and a versioned store.
/// Each instance only queries the partitions it hosts (see the interactive-queries-http example for the others).
/// </summary>
public class LocalQueriesExample : IHostedExample
{
    public const string ViewsPerPage = "views-per-page";
    public const string ViewsPerPagePerMinute = "views-per-page-per-minute";
    public const string UserSessions = "user-sessions";
    public const string UserProfiles = "user-profiles";

    public string Name => "local-queries";
    public string Description => "Queries key-value, window, session and versioned stores of the running application.";
    public IEnumerable<string> InputTopics => new[] { "page-views", "user-profiles" };

    public void BuildTopology(StreamBuilder builder)
    {
        // key: user id, value: page
        var views = builder.Stream<string, string>("page-views");
        var viewsByPage = views.GroupBy((user, page, _) => page);

        // 1. key-value store: total views per page
        viewsByPage.Count(InMemory.As<string, long>(ViewsPerPage).WithValueSerdes(new Int64SerDes()));

        // 2. window store: views per page and per minute (tumbling windows of 1 minute)
        viewsByPage
            .WindowedBy(TumblingWindowOptions.Of(TimeSpan.FromMinutes(1)))
            .Count(InMemoryWindows.As<string, long>(ViewsPerPagePerMinute).WithValueSerdes(new Int64SerDes()));

        // 3. session store: browsing sessions of each user, closed after 30 minutes without a view
        views
            .GroupByKey()
            .WindowedBy(SessionWindowOptions.Of(TimeSpan.FromMinutes(30)))
            .Count(InMemorySessions.As<string, long?>(UserSessions, TimeSpan.FromDays(1)));

        // 4. versioned store: the profile of each user, with one day of history (query "as of" a time)
        builder.Table("user-profiles", InMemoryVersioned.As<string, string>(UserProfiles, TimeSpan.FromDays(1)));
    }

    public void Configure(StreamConfig config)
    {
        config.DefaultStateStoreCacheMaxBytes = 0;
    }

    public async Task<int> RunAsync(string[] args, CancellationToken token)
    {
        var builder = new StreamBuilder();
        BuildTopology(builder);
        var config = ExampleRunner.CreateConfig(this);

        using var stream = new KafkaStream(builder.Build(), config);

        // the stores can be queried once the stream is RUNNING (or with EnableStaleStores, while restoring)
        var running = new TaskCompletionSource();
        stream.StateChanged += (_, state) =>
        {
            if (state.Equals(KafkaStream.State.RUNNING))
                running.TrySetResult();
        };

        await stream.StartAsync(token);
        await running.Task.WaitAsync(token);

        while (!token.IsCancellationRequested)
        {
            // StoreQueryParameters.FromNameAndType: the name of the store and its type
            var counts = stream.Store(StoreQueryParameters.FromNameAndType(ViewsPerPage,
                QueryableStoreTypes.KeyValueStore<string, long>()));
            var perMinute = stream.Store(StoreQueryParameters.FromNameAndType(ViewsPerPagePerMinute,
                QueryableStoreTypes.WindowStore<string, long>()));
            var sessions = stream.Store(StoreQueryParameters.FromNameAndType(UserSessions,
                QueryableStoreTypes.SessionStore<string, long?>()));
            var profiles = stream.Store(StoreQueryParameters.FromNameAndType(UserProfiles,
                QueryableStoreTypes.VersionedKeyValueStore<string, string>()));

            Console.WriteLine($"Top pages: {string.Join(", ", PageViewQueries.TopPages(counts))}");
            Console.WriteLine($"'home' per minute (last 10 min): " +
                string.Join(", ", PageViewQueries.ViewsPerMinute(perMinute, "home",
                    DateTime.UtcNow.AddMinutes(-10), DateTime.UtcNow)));
            Console.WriteLine($"Sessions of 'alice': {string.Join(", ", PageViewQueries.SessionsOf(sessions, "alice"))}");
            Console.WriteLine($"Profile of 'alice' now: {PageViewQueries.CurrentProfile(profiles, "alice")}, " +
                $"one hour ago: {PageViewQueries.ProfileAt(profiles, "alice", DateTime.UtcNow.AddHours(-1))}");

            try
            {
                await Task.Delay(TimeSpan.FromSeconds(10), token);
            }
            catch (TaskCanceledException)
            {
            }
        }

        return 0;
    }
}
