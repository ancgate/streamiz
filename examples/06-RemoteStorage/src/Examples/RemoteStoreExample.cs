using Streamiz.Examples.Common;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Examples.RemoteStorage.Examples;

/// <summary>
/// The topology shared by the remote storage examples: the number of orders of each customer,
/// kept in a remote database instead of a local store.
/// A remote store has no changelog topic (the database keeps the data), so a task can move to another
/// instance without restoring anything. Every read and write is a network call: it suits small,
/// low-throughput stores, or stores shared with other applications.
/// </summary>
public abstract class RemoteStoreExample : IExample
{
    public const string StoreName = "orders-per-customer";

    public abstract string Name { get; }
    public abstract string Description { get; }
    public IEnumerable<string> InputTopics => new[] { "orders" };

    /// <summary>The materialization of the count in the remote database.</summary>
    public abstract Materialized<string, long, IKeyValueStore<Bytes, byte[]>> CreateStore();

    public void BuildTopology(StreamBuilder builder)
    {
        builder
            // key: customer id, value: the order
            .Stream<string, string>("orders")
            .GroupByKey()
            // the only difference with a local store: the Materialized comes from the remote storage package
            .Count(CreateStore().WithValueSerdes(new Int64SerDes()))
            .ToStream()
            .To("orders-per-customer", new StringSerDes(), new Int64SerDes());
    }

    public abstract void Configure(StreamConfig config);

    /// <summary>Copies an environment variable into a configuration key, when it's set.</summary>
    protected static void FromEnvironment(StreamConfig config, string key, string variable)
    {
        var value = Environment.GetEnvironmentVariable(variable);
        if (!string.IsNullOrEmpty(value))
            config[key] = value;
    }
}
