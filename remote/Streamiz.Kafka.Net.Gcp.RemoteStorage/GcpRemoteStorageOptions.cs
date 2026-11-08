namespace Streamiz.Kafka.Net.Gcp.RemoteStorage
{
    /// <summary>
    /// Options of the GCP Bigtable remote storage. Each option can be set here or in the stream configuration
    /// (<c>config.AddConfig("gcp.remote.storage.project.id", "my-project")</c>); a value set here wins.
    /// </summary>
    public class GcpRemoteStorageOptions
    {
        /// <summary>
        /// Default Bigtable table, shared by all the stores (rows are prefixed by application, store and task).
        /// </summary>
        public const string DEFAULT_TABLE_ID = "streamiz-state";

        /// <summary>
        /// Default column family
        /// </summary>
        public const string DEFAULT_COLUMN_FAMILY = "v";

        /// <summary>
        /// Default number of rows written by one MutateRows request in <c>PutAll</c>
        /// </summary>
        public const int DEFAULT_BATCH_SIZE = 500;

        /// <summary>Configuration key of <see cref="ProjectId"/></summary>
        public const string projectIdCst = "gcp.remote.storage.project.id";
        /// <summary>Configuration key of <see cref="InstanceId"/></summary>
        public const string instanceIdCst = "gcp.remote.storage.instance.id";
        /// <summary>Configuration key of <see cref="TableId"/></summary>
        public const string tableIdCst = "gcp.remote.storage.table.id";
        /// <summary>Configuration key of <see cref="ColumnFamily"/></summary>
        public const string columnFamilyCst = "gcp.remote.storage.column.family";
        /// <summary>Configuration key of <see cref="AppProfileId"/></summary>
        public const string appProfileIdCst = "gcp.remote.storage.app.profile.id";
        /// <summary>Configuration key of <see cref="EmulatorHost"/></summary>
        public const string emulatorHostCst = "gcp.remote.storage.emulator.host";
        /// <summary>Configuration key of <see cref="CredentialsPath"/></summary>
        public const string credentialsPathCst = "gcp.remote.storage.credentials.path";
        /// <summary>Configuration key of <see cref="CreateTableIfNotExists"/></summary>
        public const string createTableCst = "gcp.remote.storage.create.table";
        /// <summary>Configuration key of <see cref="BatchSize"/></summary>
        public const string batchSizeCst = "gcp.remote.storage.batch.size";

        /// <summary>
        /// Google Cloud project id (required)
        /// </summary>
        [StreamConfigProperty(projectIdCst)]
        public string ProjectId { get; set; }

        /// <summary>
        /// Bigtable instance id (required)
        /// </summary>
        [StreamConfigProperty(instanceIdCst)]
        public string InstanceId { get; set; }

        /// <summary>
        /// Bigtable table id. Default : <see cref="DEFAULT_TABLE_ID"/>.
        /// All the stores can share one table: each row key starts with the application id, the store name and the task id.
        /// </summary>
        [StreamConfigProperty(tableIdCst)]
        public string TableId { get; set; }

        /// <summary>
        /// Column family holding the values. Default : <see cref="DEFAULT_COLUMN_FAMILY"/>.
        /// </summary>
        [StreamConfigProperty(columnFamilyCst)]
        public string ColumnFamily { get; set; }

        /// <summary>
        /// Bigtable app profile used for reads and writes. Default : the instance's default app profile.
        /// </summary>
        [StreamConfigProperty(appProfileIdCst)]
        public string AppProfileId { get; set; }

        /// <summary>
        /// Host and port of a Bigtable emulator (e.g. <c>localhost:8086</c>). When it's not set,
        /// the <c>BIGTABLE_EMULATOR_HOST</c> environment variable is used if it exists.
        /// </summary>
        [StreamConfigProperty(emulatorHostCst)]
        public string EmulatorHost { get; set; }

        /// <summary>
        /// Path of a service account JSON key. Default : Application Default Credentials.
        /// </summary>
        [StreamConfigProperty(credentialsPathCst)]
        public string CredentialsPath { get; set; }

        /// <summary>
        /// Create the table and its column family if they don't exist (needs the Bigtable admin permissions). Default : true.
        /// </summary>
        [StreamConfigProperty(createTableCst)]
        public bool? CreateTableIfNotExists { get; set; }

        /// <summary>
        /// Number of rows written by one MutateRows request in <c>PutAll</c>. Default : <see cref="DEFAULT_BATCH_SIZE"/>.
        /// </summary>
        [StreamConfigProperty(batchSizeCst)]
        public int? BatchSize { get; set; }
    }
}
