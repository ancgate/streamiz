using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.MongoDb.RemoteStorage.Internal;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.State.Supplier;

namespace Streamiz.Kafka.Net.MongoDb.RemoteStorage
{
    /// <summary>
    /// Supplier of key-value stores kept in MongoDB
    /// </summary>
    public class MongoDbRemoteStorageSupplier : IKeyValueBytesStoreSupplier
    {
        private MongoDbRemoteStorageOptions MongoDbRemoteStorageOptions { get; }

        /// <summary>
        /// Name of the store
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="mongoDbRemoteStorageOptions">options, completed by the stream configuration (can be null)</param>
        /// <param name="name">name of the store</param>
        public MongoDbRemoteStorageSupplier(MongoDbRemoteStorageOptions mongoDbRemoteStorageOptions, string name)
        {
            MongoDbRemoteStorageOptions = mongoDbRemoteStorageOptions;
            Name = name;
        }

        /// <summary>
        /// Build the store
        /// </summary>
        public IKeyValueStore<Bytes, byte[]> Get()
            => new MongoDbStore(MongoDbRemoteStorageOptions, Name);

        /// <summary>
        /// Metrics scope of the store
        /// </summary>
        public string MetricsScope => "mongodb-remote-storage";
    }
}
