using Streamiz.Examples.Common;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Examples.StateAndQueries.Examples;

/// <summary>
/// Counts the views of each page in an in-memory key-value store.
/// The store lives in the process memory and is rebuilt from its changelog topic when the application
/// (or a task) restarts: fast, but the restore reads the whole changelog.
/// </summary>
public class InMemoryStoreExample : IExample
{
    public const string StoreName = "views-per-page";

    public string Name => "in-memory-store";
    public string Description => "Counts page views in an in-memory key-value store.";
    public IEnumerable<string> InputTopics => new[] { "page-views" };

    public void BuildTopology(StreamBuilder builder)
    {
        builder
            // key: user id, value: the page the user viewed
            .Stream<string, string>("page-views")
            // re-key by page: all the views of a page go to the same partition (through a repartition topic)
            .GroupBy((user, page, _) => page)
            // InMemory.As materializes the count in an in-memory store named "views-per-page".
            // The counts are longs, so the store needs a long serdes for its values (the default serdes is string).
            .Count(InMemory.As<string, long>(StoreName).WithValueSerdes(new Int64SerDes()))
            // every update of a count is sent downstream
            .ToStream()
            .To("page-view-counts", new StringSerDes(), new Int64SerDes());
    }

    public void Configure(StreamConfig config)
    {
        // send each update right away (no store cache), so every new count is visible in the output
        config.DefaultStateStoreCacheMaxBytes = 0;
    }
}
