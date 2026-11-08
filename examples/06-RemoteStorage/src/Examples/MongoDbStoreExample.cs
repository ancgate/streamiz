using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.MongoDb.RemoteStorage;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Examples.RemoteStorage.Examples;

/// <summary>
/// Store in MongoDB (package Streamiz.Kafka.Net.MongoDb.RemoteStorage): one collection per store, one document
/// per key, with a unique index on (task, key) created at startup.
/// </summary>
public class MongoDbStoreExample : RemoteStoreExample
{
    public override string Name => "mongodb-store";
    public override string Description => "Keeps the store in a MongoDB collection.";

    public override Materialized<string, long, IKeyValueStore<Bytes, byte[]>> CreateStore()
        => MongoDbRemoteStorage.As<string, long>(new MongoDbRemoteStorageOptions
        {
            DatabaseName = "streamiz-examples",
            // the collection is named <prefix><store name>
            CollectionPrefix = "kv-"
        }, StoreName);

    public override void Configure(StreamConfig config)
    {
        // read and write concerns come from the connection string (e.g. ?w=majority)
        config[MongoDbRemoteStorageOptions.connectionStringCst] =
            Environment.GetEnvironmentVariable("MONGODB_CONNECTION_STRING") ?? "mongodb://localhost:27017";
    }
}
