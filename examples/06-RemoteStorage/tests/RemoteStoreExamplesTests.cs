using NUnit.Framework;
using Streamiz.Examples.Common;
using Streamiz.Examples.RemoteStorage.Examples;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.Aws.RemoteStorage;
using Streamiz.Kafka.Net.Azure.Cosmos.RemoteStorage;
using Streamiz.Kafka.Net.Azure.RemoteStorage;
using Streamiz.Kafka.Net.Gcp.RemoteStorage;
using Streamiz.Kafka.Net.MongoDb.RemoteStorage;

namespace Streamiz.Examples.RemoteStorage.Tests;

/// <summary>
/// The remote stores need their cloud service (or its emulator) to run, so these tests check how each example
/// wires its store: the supplier, no changelog, the store in the topology, and the configuration keys.
/// </summary>
public class RemoteStoreExamplesTests
{
    private static IEnumerable<TestCaseData> Examples()
    {
        yield return new TestCaseData(new AzureTableStoreExample(), typeof(AzureRemoteStorageSupplier), "azure-remote-storage");
        yield return new TestCaseData(new AzureCosmosStoreExample(), typeof(CosmosRemoteStorageSupplier), "azure-cosmos-remote-storage");
        yield return new TestCaseData(new AwsDynamoDbStoreExample(), typeof(AwsRemoteStorageSupplier), "aws-remote-storage");
        yield return new TestCaseData(new GcpBigtableStoreExample(), typeof(GcpRemoteStorageSupplier), "gcp-remote-storage");
        yield return new TestCaseData(new MongoDbStoreExample(), typeof(MongoDbRemoteStorageSupplier), "mongodb-remote-storage");
    }

    [TestCaseSource(nameof(Examples))]
    public void TheStoreComesFromTheRemoteStoragePackage(RemoteStoreExample example, Type supplierType, string metricsScope)
    {
        var store = example.CreateStore();

        Assert.IsInstanceOf(supplierType, store.StoreSupplier);
        Assert.AreEqual(metricsScope, store.StoreSupplier.MetricsScope);
        Assert.AreEqual(RemoteStoreExample.StoreName, store.StoreName);
        // the database keeps the data: no changelog topic
        Assert.IsFalse(store.LoggingEnabled);
    }

    [TestCaseSource(nameof(Examples))]
    public void TheTopologyCountsTheOrdersInTheRemoteStore(RemoteStoreExample example, Type supplierType, string metricsScope)
    {
        var builder = new StreamBuilder();
        example.BuildTopology(builder);

        var description = builder.Build().Describe().ToString();

        StringAssert.Contains("orders", description);
        StringAssert.Contains($"stores: [{RemoteStoreExample.StoreName}]", description);
    }

    [Test]
    public void AwsSettingsComeFromTheConfiguration()
    {
        var config = WithEnvironment("AWS_SERVICE_URL", "http://localhost:4566",
            () => ExampleRunner.CreateConfig(new AwsDynamoDbStoreExample()));

        Assert.AreEqual("http://localhost:4566", (string)config[AwsRemoteStorageOptions.serviceUrlCst]);
        Assert.IsNotNull((string)config[AwsRemoteStorageOptions.regionCst]);
    }

    [Test]
    public void MongoDbConnectionStringHasALocalDefault()
    {
        var config = WithEnvironment("MONGODB_CONNECTION_STRING", null,
            () => ExampleRunner.CreateConfig(new MongoDbStoreExample()));

        Assert.AreEqual("mongodb://localhost:27017", (string)config[MongoDbRemoteStorageOptions.connectionStringCst]);
    }

    [Test]
    public void AzureTableSettingsComeFromTheConfiguration()
    {
        var config = WithEnvironment("AZURE_TABLE_ACCOUNT_NAME", "devstoreaccount1",
            () => ExampleRunner.CreateConfig(new AzureTableStoreExample()));

        Assert.AreEqual("devstoreaccount1", (string)config[AzureRemoteStorageOptions.accountNameCst]);
    }

    private static StreamConfig WithEnvironment(string variable, string value, Func<StreamConfig> create)
    {
        var previous = Environment.GetEnvironmentVariable(variable);
        Environment.SetEnvironmentVariable(variable, value);
        try
        {
            return create();
        }
        finally
        {
            Environment.SetEnvironmentVariable(variable, previous);
        }
    }
}
