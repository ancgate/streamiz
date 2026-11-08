using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.Azure.RemoteStorage;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Examples.RemoteStorage.Examples;

/// <summary>
/// Store in Azure Table Storage (package Streamiz.Kafka.Net.Azure.RemoteStorage): one table per store,
/// one partition per task. Works with Azurite for local development.
/// </summary>
public class AzureTableStoreExample : RemoteStoreExample
{
    public override string Name => "azure-table-store";
    public override string Description => "Keeps the store in Azure Table Storage.";

    public override Materialized<string, long, IKeyValueStore<Bytes, byte[]>> CreateStore()
        // no options in code: everything comes from the configuration (see Configure)
        => AzureRemoteStorage.As<string, long>(new AzureRemoteStorageOptions(), StoreName);

    public override void Configure(StreamConfig config)
    {
        // azure.remote.storage.uri / .account.name / .account.key
        // e.g. Azurite: http://127.0.0.1:10002/devstoreaccount1 and the account devstoreaccount1
        FromEnvironment(config, AzureRemoteStorageOptions.storageUriCst, "AZURE_TABLE_URI");
        FromEnvironment(config, AzureRemoteStorageOptions.accountNameCst, "AZURE_TABLE_ACCOUNT_NAME");
        FromEnvironment(config, AzureRemoteStorageOptions.storageAccountKeyCst, "AZURE_TABLE_ACCOUNT_KEY");
    }
}
