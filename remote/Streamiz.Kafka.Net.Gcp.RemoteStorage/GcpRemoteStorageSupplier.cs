using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Gcp.RemoteStorage.Internal;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.State.Supplier;

namespace Streamiz.Kafka.Net.Gcp.RemoteStorage
{
    /// <summary>
    /// Supplier of key-value stores kept in GCP Bigtable
    /// </summary>
    public class GcpRemoteStorageSupplier : IKeyValueBytesStoreSupplier
    {
        private GcpRemoteStorageOptions GcpRemoteStorageOptions { get; }

        /// <summary>
        /// Name of the store
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="gcpRemoteStorageOptions">options, completed by the stream configuration (can be null)</param>
        /// <param name="name">name of the store</param>
        public GcpRemoteStorageSupplier(GcpRemoteStorageOptions gcpRemoteStorageOptions, string name)
        {
            GcpRemoteStorageOptions = gcpRemoteStorageOptions;
            Name = name;
        }

        /// <summary>
        /// Build the store
        /// </summary>
        public IKeyValueStore<Bytes, byte[]> Get()
            => new BigtableStore(GcpRemoteStorageOptions, Name);

        /// <summary>
        /// Metrics scope of the store
        /// </summary>
        public string MetricsScope => "gcp-remote-storage";
    }
}
