using Streamiz.Examples.Common;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Examples.StateAndQueries.Examples;

/// <summary>
/// The same count, in a RocksDB store: the data is on disk (under <c>StateDir</c>), so the store can be
/// bigger than the memory, and after a clean restart only the end of the changelog is replayed
/// (from the offset saved in the checkpoint file).
/// </summary>
public class RocksDbStoreExample : IExample
{
    public const string StoreName = "views-per-page";

    public string Name => "rocksdb-store";
    public string Description => "Counts page views in a persistent RocksDB key-value store.";
    public IEnumerable<string> InputTopics => new[] { "page-views" };

    public void BuildTopology(StreamBuilder builder)
    {
        builder
            .Stream<string, string>("page-views")
            .GroupBy((user, page, _) => page)
            // RocksDb.As: the only change compared to the in-memory example
            .Count(RocksDb.As<string, long>(StoreName).WithValueSerdes(new Int64SerDes()))
            .ToStream()
            .To("page-view-counts", new StringSerDes(), new Int64SerDes());
    }

    public void Configure(StreamConfig config)
    {
        // where RocksDB writes its files: keep it on a persistent volume to restart quickly
        config.StateDir = Path.Combine(Path.GetTempPath(), "streamiz-examples", Name);
        config.DefaultStateStoreCacheMaxBytes = 0;

        // tune RocksDB if needed (block cache, compaction, ...), per store
        config.RocksDbConfigHandler = (storeName, options) => { };
    }
}
