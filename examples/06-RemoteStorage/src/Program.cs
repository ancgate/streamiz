using Streamiz.Examples.Common;
using Streamiz.Examples.RemoteStorage.Examples;

return await ExampleRunner.RunAsync(new IExample[]
{
    new AzureTableStoreExample(),
    new AzureCosmosStoreExample(),
    new AwsDynamoDbStoreExample(),
    new GcpBigtableStoreExample(),
    new MongoDbStoreExample()
}, args);
