using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Gcp.RemoteStorage;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Examples.RemoteStorage.Examples;

/// <summary>
/// Store in Google Cloud Bigtable (package Streamiz.Kafka.Net.Gcp.RemoteStorage): all the stores share one table,
/// each row key starts with the application id, the store and the task. Works with the Bigtable emulator.
/// </summary>
public class GcpBigtableStoreExample : RemoteStoreExample
{
    public override string Name => "gcp-bigtable-store";
    public override string Description => "Keeps the store in a Google Cloud Bigtable table.";

    public override Materialized<string, long, IKeyValueStore<Bytes, byte[]>> CreateStore()
        => GcpRemoteStorage.As<string, long>(new GcpRemoteStorageOptions
        {
            // the table shared by the stores (default "streamiz-state")
            TableId = "streamiz-examples",
            // rows written per MutateRows request in PutAll
            BatchSize = 500
        }, StoreName);

    public override void Configure(StreamConfig config)
    {
        FromEnvironment(config, GcpRemoteStorageOptions.projectIdCst, "GCP_PROJECT_ID");
        FromEnvironment(config, GcpRemoteStorageOptions.instanceIdCst, "GCP_BIGTABLE_INSTANCE");
        // credentials: Application Default Credentials, or a service account key file
        FromEnvironment(config, GcpRemoteStorageOptions.credentialsPathCst, "GCP_CREDENTIALS_PATH");
        // the emulator (BIGTABLE_EMULATOR_HOST is also read directly)
        FromEnvironment(config, GcpRemoteStorageOptions.emulatorHostCst, "GCP_BIGTABLE_EMULATOR_HOST");
    }
}
