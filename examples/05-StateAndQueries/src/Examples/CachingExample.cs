using Streamiz.Examples.Common;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Examples.StateAndQueries.Examples;

/// <summary>
/// A cache in front of the store keeps only the latest value of each key between two commits:
/// fewer writes to the store and its changelog, and fewer (deduplicated) records downstream.
/// Without cache, every update of a count is sent downstream.
/// </summary>
public class CachingExample : IExample
{
    public const string StoreName = "views-per-page";

    public string Name => "caching";
    public string Description => "Deduplicates the count updates with a store cache, flushed on each commit.";
    public IEnumerable<string> InputTopics => new[] { "page-views" };

    public void BuildTopology(StreamBuilder builder) => BuildTopology(builder, caching: true);

    /// <summary>The same topology with or without the cache, to compare their outputs.</summary>
    public static void BuildTopology(StreamBuilder builder, bool caching)
    {
        var store = InMemory.As<string, long>(StoreName).WithValueSerdes(new Int64SerDes());
        store = caching
            // up to 1 MB of cached entries for this store; the cache is flushed (and forwarded) on each commit
            ? store.WithCachingEnabled(CacheSize.OfMb(1))
            // every update goes to the store and downstream right away
            : store.WithCachingDisabled();

        builder
            .Stream<string, string>("page-views")
            .GroupBy((user, page, _) => page)
            .Count(store)
            .ToStream()
            .To("page-view-counts", new StringSerDes(), new Int64SerDes());
    }

    public void Configure(StreamConfig config)
    {
        // the cache is flushed on each commit: the commit interval is the maximum delay of the output
        config.CommitIntervalMs = 1000;
    }
}
