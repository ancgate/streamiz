using Streamiz.Examples.Common;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Examples.StateAndQueries.Examples;

/// <summary>
/// Standby replicas: each instance keeps an up-to-date copy of the stores of other instances
/// (fed from the changelog topics). When an instance dies, the instance that takes over its partitions
/// only replays the end of the changelog, and the copies can be queried (possibly stale) meanwhile.
/// </summary>
public class StandbyReplicasExample : IHostedExample
{
    public const string StoreName = "views-per-page";

    public string Name => "standby-replicas";
    public string Description => "Keeps a standby copy of each store on another instance and queries it with EnableStaleStores.";
    public IEnumerable<string> InputTopics => new[] { "page-views" };

    public static string ApplicationServer =>
        Environment.GetEnvironmentVariable("APPLICATION_SERVER") ?? "localhost:7070";

    public void BuildTopology(StreamBuilder builder)
    {
        builder
            .Stream<string, string>("page-views")
            .GroupBy((user, page, _) => page)
            // standbys are fed from the changelog: the store must keep its changelog (the default).
            // RocksDB: a promoted standby is reused as is (an in-memory standby is rebuilt from the changelog)
            .Count(RocksDb.As<string, long>(StoreName).WithValueSerdes(new Int64SerDes()));
    }

    public void Configure(StreamConfig config)
    {
        // one standby copy of each stateful task, on another instance
        config.NumStandbyReplicas = 1;
        // optional for standbys, needed to know the hosts in StandbyHosts
        config.ApplicationServer = ApplicationServer;
        config.StateDir = Path.Combine(Path.GetTempPath(), "streamiz-examples", Name, ApplicationServer.Replace(':', '-'));
    }

    public async Task<int> RunAsync(string[] args, CancellationToken token)
    {
        var builder = new StreamBuilder();
        BuildTopology(builder);
        var config = ExampleRunner.CreateConfig(this);

        using var stream = new KafkaStream(builder.Build(), config);
        await stream.StartAsync(token);

        while (!token.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(TimeSpan.FromSeconds(10), token);

                // where "home" is hosted: the active instance and the standby instances
                var metadata = stream.QueryMetadataForKey(StoreName, "home", new StringSerDes());
                Console.WriteLine($"'home': active on {metadata.ActiveHost}, standby on {string.Join(", ", metadata.StandbyHosts)}");

                // EnableStaleStores: also query the local standby copies (and the stores being restored)
                var store = stream.Store(StoreQueryParameters
                    .FromNameAndType(StoreName, QueryableStoreTypes.KeyValueStore<string, long>())
                    .EnableStaleStores());
                Console.WriteLine($"Local count of 'home' (active or standby copy): {store.Get("home")}");
            }
            catch (TaskCanceledException)
            {
            }
            catch (InvalidStateStoreException e)
            {
                // the stream is not started yet, or the store is not hosted here (neither active nor standby)
                Console.WriteLine($"Store not queryable yet: {e.Message}");
            }
        }

        return 0;
    }
}
