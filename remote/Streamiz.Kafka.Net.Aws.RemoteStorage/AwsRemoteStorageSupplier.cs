using Streamiz.Kafka.Net.Aws.RemoteStorage.Internal;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.State.Supplier;

namespace Streamiz.Kafka.Net.Aws.RemoteStorage
{
    /// <summary>
    /// Supplier of key-value stores kept in AWS DynamoDB
    /// </summary>
    public class AwsRemoteStorageSupplier : IKeyValueBytesStoreSupplier
    {
        private AwsRemoteStorageOptions AwsRemoteStorageOptions { get; set; }

        /// <summary>
        /// Name of the store
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Create a supplier of DynamoDB stores
        /// </summary>
        /// <param name="awsRemoteStorageOptions">options, completed by the stream configuration</param>
        /// <param name="name">name of the store</param>
        public AwsRemoteStorageSupplier(AwsRemoteStorageOptions awsRemoteStorageOptions, string name)
        {
            AwsRemoteStorageOptions = awsRemoteStorageOptions;
            Name = name;
        }

        /// <summary>
        /// Build the store
        /// </summary>
        public IKeyValueStore<Bytes, byte[]> Get()
            => new DynamoDbStore(AwsRemoteStorageOptions, Name);

        /// <summary>
        /// Metrics scope of the store
        /// </summary>
        public string MetricsScope => "aws-remote-storage";
    }
}
