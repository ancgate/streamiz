namespace Streamiz.Kafka.Net.MongoDb.RemoteStorage
{
    /// <summary>
    /// Options of the MongoDB remote storage. Each option can be set here or in the stream configuration
    /// (<c>config.AddConfig("mongodb.remote.storage.connection.string", "mongodb://localhost:27017")</c>); a value set here wins.
    /// </summary>
    public class MongoDbRemoteStorageOptions
    {
        /// <summary>
        /// Default database
        /// </summary>
        public const string DEFAULT_DATABASE_NAME = "streamiz";

        /// <summary>
        /// Default number of documents written by one bulk write in <c>PutAll</c>
        /// </summary>
        public const int DEFAULT_BATCH_SIZE = 1000;

        /// <summary>Configuration key of <see cref="ConnectionString"/></summary>
        public const string connectionStringCst = "mongodb.remote.storage.connection.string";
        /// <summary>Configuration key of <see cref="DatabaseName"/></summary>
        public const string databaseNameCst = "mongodb.remote.storage.database";
        /// <summary>Configuration key of <see cref="CollectionPrefix"/></summary>
        public const string collectionPrefixCst = "mongodb.remote.storage.collection.prefix";
        /// <summary>Configuration key of <see cref="CreateIndex"/></summary>
        public const string createIndexCst = "mongodb.remote.storage.create.index";
        /// <summary>Configuration key of <see cref="BatchSize"/></summary>
        public const string batchSizeCst = "mongodb.remote.storage.batch.size";

        /// <summary>
        /// MongoDB connection string (required), e.g. <c>mongodb://localhost:27017</c>. The read and write concerns
        /// come from the connection string (e.g. <c>?w=majority&amp;readConcernLevel=majority</c>) or the server defaults.
        /// </summary>
        [StreamConfigProperty(connectionStringCst)]
        public string ConnectionString { get; set; }

        /// <summary>
        /// Database holding the collections. Default : <see cref="DEFAULT_DATABASE_NAME"/>.
        /// </summary>
        [StreamConfigProperty(databaseNameCst)]
        public string DatabaseName { get; set; }

        /// <summary>
        /// Prefix of the collection names (one collection per store : prefix + store name). Default : no prefix.
        /// Use it (e.g. with the application id) when several applications have stores with the same name in one database.
        /// </summary>
        [StreamConfigProperty(collectionPrefixCst)]
        public string CollectionPrefix { get; set; }

        /// <summary>
        /// Create the unique index { t: 1, k: 1 } of the collection if it doesn't exist (needs the createIndex permission).
        /// Default : true. When it's false, create it before starting the application: ranges need it.
        /// </summary>
        [StreamConfigProperty(createIndexCst)]
        public bool? CreateIndex { get; set; }

        /// <summary>
        /// Number of documents written by one bulk write in <c>PutAll</c>. Default : <see cref="DEFAULT_BATCH_SIZE"/>.
        /// </summary>
        [StreamConfigProperty(batchSizeCst)]
        public int? BatchSize { get; set; }
    }
}
