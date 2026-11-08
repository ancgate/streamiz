# 06 - Remote storage

Keeping a store in a database instead of a local store and its changelog topic. One example per supported database; all of them count the orders of each customer:

```
orders (key: customer id, value: order)  ->  GroupByKey().Count(<remote store>)  ->  orders-per-customer
```

Each store comes from its own NuGet package and replaces `InMemory.As` / `RocksDb.As`:

| Example | Package | Materialized |
| --- | --- | --- |
| `azure-table-store` | `Streamiz.Kafka.Net.Azure.RemoteStorage` | `AzureRemoteStorage.As<K, V>(options, storeName)` |
| `azure-cosmos-store` | `Streamiz.Kafka.Net.Azure.Cosmos.RemoteStorage` | `CosmosRemoteStorage.As<K, V>(options, storeName)` |
| `aws-dynamodb-store` | `Streamiz.Kafka.Net.Aws.RemoteStorage` | `AwsRemoteStorage.As<K, V>(options, storeName)` |
| `gcp-bigtable-store` | `Streamiz.Kafka.Net.Gcp.RemoteStorage` | `GcpRemoteStorage.As<K, V>(options, storeName)` |
| `mongodb-store` | `Streamiz.Kafka.Net.MongoDb.RemoteStorage` | `MongoDbRemoteStorage.As<K, V>(options, storeName)` |

```bash
cd examples/06-RemoteStorage/src
dotnet run -- <example>
```

Docs: [stores](../../docs/stores.md) (options, data layout and behaviour of each store).

## When to use a remote store

- No changelog topic and no restoration: a task can move to another instance and use the data right away.
- The data can be read by other applications, and outlives the Kafka topics.
- But every `Get`/`Put` is a network call made by the stream thread: it suits small or low-throughput stores. Prefer a local store (and its cache) for hot paths.
- The range queries of Azure Table read the whole partition of the task (Azure Tables can't compare byte keys); the other stores run a real range query.

## Configuration: code or config keys

Each example sets some options in code (`new XxxRemoteStorageOptions { ... }`) and the connection settings through configuration keys, read from environment variables in `Configure`. An option set in code wins over the configuration key. The keys are constants of each options class:

| Store | Main configuration keys |
| --- | --- |
| Azure Table | `azure.remote.storage.uri`, `azure.remote.storage.account.name`, `azure.remote.storage.account.key` |
| Azure Cosmos DB | `azure.cosmos.remote.storage.connection.string` (or `.account.endpoint` + `.account.key`), `.database`, `.container.prefix`, `.batch.size` |
| AWS DynamoDB | `aws.remote.storage.region`, `aws.remote.storage.service.url`, `aws.remote.storage.table.prefix`, `aws.remote.storage.create.table` |
| GCP Bigtable | `gcp.remote.storage.project.id`, `gcp.remote.storage.instance.id`, `gcp.remote.storage.table.id`, `gcp.remote.storage.emulator.host`, `gcp.remote.storage.credentials.path` |
| MongoDB | `mongodb.remote.storage.connection.string`, `mongodb.remote.storage.database`, `mongodb.remote.storage.collection.prefix` |

```csharp
config[AwsRemoteStorageOptions.regionCst] = "eu-west-1";   // or the string "aws.remote.storage.region"
```

## Running locally with emulators

| Example | Emulator | Environment variables |
| --- | --- | --- |
| `azure-table-store` | [Azurite](https://learn.microsoft.com/azure/storage/common/storage-use-azurite): `docker run -p 10002:10002 mcr.microsoft.com/azure-storage/azurite azurite-table --tableHost 0.0.0.0` | `AZURE_TABLE_URI=http://127.0.0.1:10002/devstoreaccount1`, `AZURE_TABLE_ACCOUNT_NAME=devstoreaccount1`, `AZURE_TABLE_ACCOUNT_KEY=<the well-known Azurite key>` |
| `azure-cosmos-store` | [Cosmos DB emulator](https://learn.microsoft.com/azure/cosmos-db/emulator) (accept its certificate, or configure `ConfigureClientOptions`) | `COSMOS_CONNECTION_STRING=AccountEndpoint=https://localhost:8081/;AccountKey=<emulator key>` |
| `aws-dynamodb-store` | [LocalStack](https://docs.localstack.cloud): `docker run -p 4566:4566 localstack/localstack` | `AWS_SERVICE_URL=http://localhost:4566`, `AWS_REGION=us-east-1`, `AWS_ACCESS_KEY_ID=test`, `AWS_SECRET_ACCESS_KEY=test` |
| `gcp-bigtable-store` | Bigtable emulator: `gcloud beta emulators bigtable start --host-port=localhost:8086` | `GCP_BIGTABLE_EMULATOR_HOST=localhost:8086`, `GCP_PROJECT_ID=local`, `GCP_BIGTABLE_INSTANCE=local` |
| `mongodb-store` | `docker run -p 27017:27017 mongo:7` | `MONGODB_CONNECTION_STRING=mongodb://localhost:27017` (default) |

In the cloud, use the platform's identities rather than keys: the default AWS credentials chain, Application Default Credentials on GCP, a `TokenCredential` (`DefaultAzureCredential`) for Cosmos DB.

## Tests

The stores need their database to run, so the tests check how each example wires its store: the supplier type and metrics scope, no changelog topic, the store in the topology description, and the configuration keys. The stores themselves are tested in the library against in-memory fakes of each API.
