using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.Aws.RemoteStorage;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Examples.RemoteStorage.Examples;

/// <summary>
/// Store in AWS DynamoDB (package Streamiz.Kafka.Net.Aws.RemoteStorage): one table per store,
/// partition key = task, sort key = the key bytes. Works with LocalStack for local development.
/// </summary>
public class AwsDynamoDbStoreExample : RemoteStoreExample
{
    public override string Name => "aws-dynamodb-store";
    public override string Description => "Keeps the store in an AWS DynamoDB table.";

    public override Materialized<string, long, IKeyValueStore<Bytes, byte[]>> CreateStore()
        => AwsRemoteStorage.As<string, long>(new AwsRemoteStorageOptions
        {
            // the table is named <prefix><store name>: "streamiz-examples-orders-per-customer"
            TableNamePrefix = "streamiz-examples-",
            // creates the table (on-demand capacity) if it doesn't exist
            CreateTableIfNotExists = true
        }, StoreName);

    public override void Configure(StreamConfig config)
    {
        // credentials: the default AWS chain (environment, profile, instance role), or explicit keys
        config[AwsRemoteStorageOptions.regionCst] = Environment.GetEnvironmentVariable("AWS_REGION") ?? "us-east-1";
        // LocalStack: http://localhost:4566
        FromEnvironment(config, AwsRemoteStorageOptions.serviceUrlCst, "AWS_SERVICE_URL");
    }
}
