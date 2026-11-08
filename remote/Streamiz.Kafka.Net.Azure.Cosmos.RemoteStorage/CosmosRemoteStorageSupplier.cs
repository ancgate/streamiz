using Streamiz.Kafka.Net.Azure.Cosmos.RemoteStorage.Internal;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.State.Supplier;

namespace Streamiz.Kafka.Net.Azure.Cosmos.RemoteStorage
{
    /// <summary>
    /// Supplier of key-value stores kept in Azure Cosmos DB
    /// </summary>
    public class CosmosRemoteStorageSupplier : IKeyValueBytesStoreSupplier
    {
        private CosmosRemoteStorageOptions CosmosRemoteStorageOptions { get; }

        /// <summary>
        /// Name of the store
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="cosmosRemoteStorageOptions">options, completed by the stream configuration (can be null)</param>
        /// <param name="name">name of the store</param>
        public CosmosRemoteStorageSupplier(CosmosRemoteStorageOptions cosmosRemoteStorageOptions, string name)
        {
            CosmosRemoteStorageOptions = cosmosRemoteStorageOptions;
            Name = name;
        }

        /// <summary>
        /// Build the store
        /// </summary>
        public IKeyValueStore<Bytes, byte[]> Get()
            => new CosmosStore(CosmosRemoteStorageOptions, Name);

        /// <summary>
        /// Metrics scope of the store
        /// </summary>
        public string MetricsScope => "azure-cosmos-remote-storage";
    }
}
