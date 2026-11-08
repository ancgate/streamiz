using Streamiz.Examples.Common;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Examples.StateAndQueries.Examples;

/// <summary>
/// Each store is backed by a compacted changelog topic, <c>&lt;application.id&gt;-&lt;store&gt;-changelog</c>,
/// which is used to restore the store. You can pass topic configs for it, or disable it for a store
/// you don't need to restore (it then starts empty after a restart or a rebalance).
/// </summary>
public class ChangelogExample : IExample
{
    public const string StoreName = "views-per-page";

    public string Name => "changelog-config";
    public string Description => "Configures the changelog topic of a store, or disables it.";
    public IEnumerable<string> InputTopics => new[] { "page-views" };

    /// <summary>Name of the changelog topic of the store, for an application id.</summary>
    public static string ChangelogTopic(string applicationId) => $"{applicationId}-{StoreName}-changelog";

    public void BuildTopology(StreamBuilder builder) => BuildTopology(builder, logging: true);

    public static void BuildTopology(StreamBuilder builder, bool logging)
    {
        var store = InMemory.As<string, long>(StoreName).WithValueSerdes(new Int64SerDes());
        store = logging
            // these configs are used when Streamiz creates the changelog topic
            ? store.WithLoggingEnabled(new Dictionary<string, string>
            {
                { "min.insync.replicas", "1" },
                { "segment.bytes", "104857600" }
            })
            // no changelog topic: lower cost, but the store is lost when its task moves or restarts
            : store.WithLoggingDisabled();

        builder
            .Stream<string, string>("page-views")
            .GroupBy((user, page, _) => page)
            .Count(store)
            .ToStream()
            .To("page-view-counts", new StringSerDes(), new Int64SerDes());
    }

    public void Configure(StreamConfig config)
    {
        config.DefaultStateStoreCacheMaxBytes = 0;
        // replication factor of the internal topics (changelogs, repartition topics); -1 = the broker default
        config.ReplicationFactor = -1;
    }
}
