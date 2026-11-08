using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.Azure.Cosmos.RemoteStorage;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Examples.RemoteStorage.Examples;

/// <summary>
/// Store in Azure Cosmos DB (package Streamiz.Kafka.Net.Azure.Cosmos.RemoteStorage): one container per store,
/// partitioned by task. The database and the container are created if they don't exist.
/// </summary>
public class AzureCosmosStoreExample : RemoteStoreExample
{
    public override string Name => "azure-cosmos-store";
    public override string Description => "Keeps the store in an Azure Cosmos DB container.";

    public override Materialized<string, long, IKeyValueStore<Bytes, byte[]>> CreateStore()
        => CosmosRemoteStorage.As<string, long>(new CosmosRemoteStorageOptions
        {
            // set in code: these win over the configuration keys
            DatabaseName = "streamiz-examples",
            ContainerPrefix = "orders-",
            // a Cosmos transactional batch holds at most 100 operations
            BatchSize = 100
        }, StoreName);

    public override void Configure(StreamConfig config)
    {
        // a connection string, or an endpoint + a key, or a TokenCredential set in code (options.Credential)
        FromEnvironment(config, CosmosRemoteStorageOptions.connectionStringCst, "COSMOS_CONNECTION_STRING");
        FromEnvironment(config, CosmosRemoteStorageOptions.accountEndpointCst, "COSMOS_ENDPOINT");
        FromEnvironment(config, CosmosRemoteStorageOptions.accountKeyCst, "COSMOS_KEY");
    }
}
