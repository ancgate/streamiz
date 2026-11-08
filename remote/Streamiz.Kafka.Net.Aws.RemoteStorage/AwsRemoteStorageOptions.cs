namespace Streamiz.Kafka.Net.Aws.RemoteStorage
{
    /// <summary>
    /// Options of the AWS DynamoDB remote store. Each option can be set in code, or in the stream configuration with
    /// its key (the value set in code wins).
    /// </summary>
    public class AwsRemoteStorageOptions
    {
        public const string tableNamePrefixCst = "aws.remote.storage.table.prefix";
        public const string regionCst = "aws.remote.storage.region";
        public const string serviceUrlCst = "aws.remote.storage.service.url";
        public const string accessKeyCst = "aws.remote.storage.access.key";
        public const string secretKeyCst = "aws.remote.storage.secret.key";
        public const string sessionTokenCst = "aws.remote.storage.session.token";
        public const string createTableIfNotExistsCst = "aws.remote.storage.create.table";

        /// <summary>
        /// Prefix of the DynamoDB table names (the table name is the prefix followed by the store name). Default : none
        /// </summary>
        [StreamConfigProperty(tableNamePrefixCst)]
        public string TableNamePrefix { get; set; }

        /// <summary>
        /// AWS region of the DynamoDB tables, e.g. "eu-west-1". Default : the region of the AWS SDK configuration
        /// (environment, profile, ...)
        /// </summary>
        [StreamConfigProperty(regionCst)]
        public string Region { get; set; }

        /// <summary>
        /// DynamoDB endpoint, to use DynamoDB local or LocalStack, e.g. "http://localhost:4566". Default : the AWS endpoint
        /// of the region
        /// </summary>
        [StreamConfigProperty(serviceUrlCst)]
        public string ServiceUrl { get; set; }

        /// <summary>
        /// Access key id. When it is not set, the default AWS credentials chain is used (environment, profile, instance role, ...)
        /// </summary>
        [StreamConfigProperty(accessKeyCst)]
        public string AccessKey { get; set; }

        /// <summary>
        /// Secret access key, used with <see cref="AccessKey"/>
        /// </summary>
        [StreamConfigProperty(secretKeyCst)]
        public string SecretKey { get; set; }

        /// <summary>
        /// Session token of temporary credentials, used with <see cref="AccessKey"/> and <see cref="SecretKey"/>
        /// </summary>
        [StreamConfigProperty(sessionTokenCst)]
        public string SessionToken { get; set; }

        /// <summary>
        /// Create the DynamoDB table (on-demand capacity) if it doesn't exist. Default : true.
        /// In the stream configuration, set a bool value.
        /// </summary>
        [StreamConfigProperty(createTableIfNotExistsCst)]
        public bool? CreateTableIfNotExists { get; set; }
    }
}
