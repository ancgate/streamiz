using System;
using Azure.Core;
using Microsoft.Azure.Cosmos;

namespace Streamiz.Kafka.Net.Azure.Cosmos.RemoteStorage
{
    /// <summary>
    /// Options of the Azure Cosmos DB remote storage. Each option with a configuration key can be set here or in the stream
    /// configuration (<c>config.AddConfig("azure.cosmos.remote.storage.connection.string", "...")</c>); a value set here wins.
    /// Authentication: <see cref="ConnectionString"/>, or <see cref="AccountEndpoint"/> with <see cref="AccountKey"/>
    /// or with <see cref="Credential"/> (an Azure AD token credential, e.g. <c>DefaultAzureCredential</c> from Azure.Identity).
    /// </summary>
    public class CosmosRemoteStorageOptions
    {
        /// <summary>
        /// Default database
        /// </summary>
        public const string DEFAULT_DATABASE_NAME = "streamiz";

        /// <summary>
        /// Default (and maximum) number of operations of one transactional batch in <c>PutAll</c>
        /// </summary>
        public const int DEFAULT_BATCH_SIZE = 100;

        /// <summary>Configuration key of <see cref="ConnectionString"/></summary>
        public const string connectionStringCst = "azure.cosmos.remote.storage.connection.string";
        /// <summary>Configuration key of <see cref="AccountEndpoint"/></summary>
        public const string accountEndpointCst = "azure.cosmos.remote.storage.account.endpoint";
        /// <summary>Configuration key of <see cref="AccountKey"/></summary>
        public const string accountKeyCst = "azure.cosmos.remote.storage.account.key";
        /// <summary>Configuration key of <see cref="DatabaseName"/></summary>
        public const string databaseNameCst = "azure.cosmos.remote.storage.database";
        /// <summary>Configuration key of <see cref="ContainerPrefix"/></summary>
        public const string containerPrefixCst = "azure.cosmos.remote.storage.container.prefix";
        /// <summary>Configuration key of <see cref="CreateIfNotExists"/></summary>
        public const string createIfNotExistsCst = "azure.cosmos.remote.storage.create.if.not.exists";
        /// <summary>Configuration key of <see cref="Throughput"/></summary>
        public const string throughputCst = "azure.cosmos.remote.storage.throughput";
        /// <summary>Configuration key of <see cref="ConsistencyLevel"/></summary>
        public const string consistencyLevelCst = "azure.cosmos.remote.storage.consistency.level";
        /// <summary>Configuration key of <see cref="BatchSize"/></summary>
        public const string batchSizeCst = "azure.cosmos.remote.storage.batch.size";
        /// <summary>Configuration key of <see cref="MaxRetryAttemptsOnRateLimitedRequests"/></summary>
        public const string maxRetryAttemptsCst = "azure.cosmos.remote.storage.max.retry.attempts.on.rate.limited.requests";

        /// <summary>
        /// Connection string of the Cosmos DB account (<c>AccountEndpoint=...;AccountKey=...;</c>).
        /// Used instead of <see cref="AccountEndpoint"/> and <see cref="AccountKey"/> when it's set.
        /// </summary>
        [StreamConfigProperty(connectionStringCst)]
        public string ConnectionString { get; set; }

        /// <summary>
        /// Endpoint of the Cosmos DB account (<c>https://my-account.documents.azure.com:443/</c>)
        /// </summary>
        [StreamConfigProperty(accountEndpointCst)]
        public string AccountEndpoint { get; set; }

        /// <summary>
        /// Key of the Cosmos DB account, used with <see cref="AccountEndpoint"/>
        /// </summary>
        [StreamConfigProperty(accountKeyCst)]
        public string AccountKey { get; set; }

        /// <summary>
        /// Azure AD credential, used with <see cref="AccountEndpoint"/> when there is no key (code only, not in the configuration)
        /// </summary>
        public TokenCredential Credential { get; set; }

        /// <summary>
        /// Database of the containers. Default : <see cref="DEFAULT_DATABASE_NAME"/>.
        /// </summary>
        [StreamConfigProperty(databaseNameCst)]
        public string DatabaseName { get; set; }

        /// <summary>
        /// Prefix of the container names: each store has a container named prefix + store name. Default : none.
        /// Use a different prefix (or database) per application if two applications have stores with the same name.
        /// </summary>
        [StreamConfigProperty(containerPrefixCst)]
        public string ContainerPrefix { get; set; }

        /// <summary>
        /// Create the database and the container if they don't exist. Default : true.
        /// </summary>
        [StreamConfigProperty(createIfNotExistsCst)]
        public bool? CreateIfNotExists { get; set; }

        /// <summary>
        /// Provisioned throughput (RU/s) of the containers created by the store. Default : none (the account's default,
        /// e.g. serverless or the database's shared throughput).
        /// </summary>
        [StreamConfigProperty(throughputCst)]
        public int? Throughput { get; set; }

        /// <summary>
        /// Consistency level of the client (a <see cref="Microsoft.Azure.Cosmos.ConsistencyLevel"/> name, e.g. <c>Session</c>),
        /// it can't be stronger than the account's level. Default : the account's level.
        /// </summary>
        [StreamConfigProperty(consistencyLevelCst)]
        public string ConsistencyLevel { get; set; }

        /// <summary>
        /// Number of operations of one transactional batch in <c>PutAll</c>, between 1 and 100 (the Cosmos DB limit).
        /// Default : <see cref="DEFAULT_BATCH_SIZE"/>.
        /// </summary>
        [StreamConfigProperty(batchSizeCst)]
        public int? BatchSize { get; set; }

        /// <summary>
        /// Number of retries of a request throttled by Cosmos DB (HTTP 429). Default : the SDK's default (9).
        /// </summary>
        [StreamConfigProperty(maxRetryAttemptsCst)]
        public int? MaxRetryAttemptsOnRateLimitedRequests { get; set; }

        /// <summary>
        /// Changes the client options before the client is created (code only), e.g. to accept the certificate of the
        /// local emulator: <c>o => o.HttpClientFactory = ...</c>. Disabling the certificate validation is your responsibility.
        /// </summary>
        public Action<CosmosClientOptions> ConfigureClientOptions { get; set; }
    }
}
