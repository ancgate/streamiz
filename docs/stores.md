# State stores

⚠️ <span style="color:red">**Some things you need to know**</span> ⚠️

- From 1.4.0 release, the default state store is a RocksDb state store. (Before 1.4.0, the default state store was a in memory state store.)
- RocksDb state store is available from 1.2.0 release.
- By default, a state store is tracked by a changelog topic from 1.2.0 release. (If you don't need, you have to make it explicit).

## Basics

A stateful processor may use one or more state stores. Each task that contains a stateful processor has exclusive access to the state stores in the processor. That means a topology with two state stores and five input partitions will lead to five tasks, and each task will own two state stores resulting in 10 state stores in total for your Kafka Streams application.

State stores in Streamiz are layered in four ways :
- The outermost layer collects metrics about the operations on the state store and serializes and deserializes the records that are written to and read from the state store.
- The next layer caches the records. If a record exists in the cache with the same key as a new record, the cache overwrites the existing record with the new record; otherwise, the cache adds a new entry for the new record. The cache is the primary serving area for lookups. If a lookup can’t find a record with a given key in the cache, it is forwarded to the next layer. If this lookup returns an entry, the entry is added to the cache. If the cache exceeds its configured size during a write, the cache evicts the records that have been least recently used and sends new and overwritten records downstream. The caching layer decreases downstream traffic because no updates are sent downstream unless the cache evicts records or is flushed. The cache’s size is configurable. If it is set to zero, the cache is disabled. (`EARLY ACCESS ONLY`)
- The changelogging layer sends each record updated in the state store to a topic in Kafka—the state’s changelog topic. The changelog topic is a compacted topic that replicates the data in the local state. Changelogging is needed for fault tolerance, as we will explain below.
- The innermost layer updates and reads the local state store.

![state-store](./assets/state-store.png)

## In Memory key/value store

As his name, this is an inmemory key value state store which is supplied by InMemoryKeyValueBytesStoreSupplier.
You have an child materialized class to help you to use it.

Usefull with statefull operation like Count, Aggregate, Reduce but also to materialized IKTable<K, V> or IGlobalKTable<K, V>

Example :
``` csharp

builder.Table("test-ktable", InMemory.As<string, string>("test-store"));

builder
        .Stream<string, string>("topic")
        .GroupBy((k, v) => k.ToUpper())
        .Aggregate(
            () => 0L,
            (k, v, agg) => agg + 1,
            InMemory.As<string, long>("agg-store").WithValueSerdes<Int64SerDes>()
        );
```

**Be carefull, this state store is not persistent ! So after each application restart, you loose the state of your state store.**

## In Memory window store

As his name, this is an inmemory windows state store which is supplied by InMemoryWindowStoreSupplier.
You have an child materialized class to help you to use it.

Usefull with windowing statefull operation like Count, Aggregate, Reduce in ITimeWindowedKStream<K, V>

Example :
``` csharp
builder
        .Stream<string, string>("topic")
        .GroupByKey()
        .WindowedBy(TumblingWindowOptions.Of(2000))
        .Aggregate(
            () => 0,
            (k, v, agg) => Math.Max(v.Length, agg),
            InMemoryWindows.As<string, int>("store").WithValueSerdes<Int32SerDes>()
        )
        .ToStream()
        .To<StringTimeWindowedSerDes, Int32SerDes>("output");
```

**Be carefull, this state store is not persistent ! So after each application restart, you loose the state of your state store.**

## RocksDb key/value store

As his name, this is a rocksdb key value state store which is supplied by RocksDbKeyValueBytesStoreSupplier. 

The state store is persisted on disk at `{config.StateDir}/{config.ApplicationId}/{taskId}/rocksdb/{store.Name}`.

You have an child materialized class to help you to use it.

Usefull with statefull operation like Count, Aggregate, Reduce but also to materialized IKTable<K, V> or IGlobalKTable<K, V>

Example :
``` csharp

builder.Table("test-ktable", RocksDb.As<string, string>("test-store"));

 builder
    .Stream<string, string>("topic")
    .GroupBy((k, v) => k.ToUpper())
    .Aggregate(
        () => 0L,
        (k, v, agg) => agg + 1,
        RocksDb.As<string, long>("agg-store").WithValueSerdes<Int64SerDes>()
    );
```

## RocksDb window store

As his name, this is a rocksdb windows state store which is supplied by RocksDbWindowBytesStoreSupplier.

This state store save data in 3 segments, each segment is a rocksdb properly and contains certain time range. This for optimization purposes and for retention periods (window size + grace + window-additional-retention).

You have an child materialized class to help you to use it.

Usefull with windowing statefull operation like Count, Aggregate, Reduce in ITimeWindowedKStream<K, V>

Example :
``` csharp
builder
        .Stream<string, string>("topic")
        .GroupByKey()
        .WindowedBy(TumblingWindowOptions.Of(2000))
        .Aggregate(
            () => 0,
            (k, v, agg) => Math.Max(v.Length, agg),
            RocksDbWindows.As<string, int>("store").WithValueSerdes<Int32SerDes>()
        )
        .ToStream()
        .To<StringTimeWindowedSerDes, Int32SerDes>("output");
```

## Versioned key/value store

A versioned key/value store (KIP-889) keeps the history of each key: every record is a version of its key, valid from its timestamp until the timestamp of the next record. The history is kept for the **history retention**, which is also the store's grace period: a record older than stream time minus the history retention is not put.

``` csharp
IVersionedKeyValueStore<string, string> store = ...;
store.Put("key", "v1", 1000);
store.Put("key", "v3", 3000);
store.Put("key", "v2", 2000);  // out-of-order: inserted in the history, returns its validTo (3000)

store.Get("key");              // v3 (timestamp 3000), the latest record
store.Get("key", 2500);        // v2 (timestamp 2000, validTo 3000)
store.Delete("key", 4000);     // a tombstone: returns the record valid at 4000 (v3)
store.Get("key", 3500);        // v3 (validTo 4000)
store.Get("key");              // null
```

`Put` returns `VersionedKeyValueStoreReturnCodes.PUT_RETURN_CODE_VALID_TO_UNDEFINED` when the record is the latest one, `PUT_RETURN_CODE_NOT_PUT` when it is older than the history retention, otherwise the timestamp of the next newer record. `Get(key, asOfTimestamp)` returns null when `asOfTimestamp` is older than the history retention.

Two implementations are available:
- `Stores.InMemoryVersionedKeyValueStore(name, historyRetention)` (or `InMemoryVersioned.As<K, V>(name, historyRetention)`),
- `Stores.PersistentVersionedKeyValueStore(name, historyRetention, segmentInterval)` (or `RocksDbVersioned.As<K, V>(...)`): the latest values are in one RocksDB database, and the older versions in time segments (by default `max(historyRetention / 3, 2s)` long), dropped once they are older than the history retention.

Versioned stores are supported to materialize **source KTables**:

``` csharp
builder.Table("topic", RocksDbVersioned.As<string, string>("table-store", TimeSpan.FromMinutes(5)));
```

With a versioned source table, an out-of-order record (older than the latest value of its key) is written in the history, and forwarded downstream marked as "not latest" (KIP-914), with the latest value of the key as old value. A record older than the history retention is dropped.

- `ToStream()`, `MapValues` and `Filter` forward it like any other record (so it appears in `ToStream()` output, with its own timestamp).
- Table-table joins (both sides, including foreign-key joins) and `GroupBy` (so `Count`, `Reduce`, `Aggregate`) ignore it: the table's latest value didn't change.
- If a `MapValues` or `Filter` result is materialized, it is materialized as a (non-versioned) timestamped store: the out-of-order record is written in it, and is then forwarded as a latest record (as in Kafka Streams with a non-versioned downstream store).
- Value getters read the latest value, and stream-table joins read the value as of the stream record's timestamp.

Use `QueryableStoreTypes.VersionedKeyValueStore<K, V>()` for interactive queries and `TopologyTestDriver.GetVersionedKeyValueStore<K, V>(name)` in tests.

Limitations:
- versioned stores are not supported for global tables, aggregations or other materialized operations (a `TopologyException` is thrown),
- versioned stores are never cached: the caching settings are ignored,
- the changelog topic is compacted, with `min.compaction.lag.ms` = history retention + `WindowStoreChangelogAdditionalRetentionMs`,
- after a restart without restoration, the stream time of a RocksDB versioned store starts again from the next record.

## Caching

**This feature is available in `EARLY ACCESS` only.**

Streamiz offers robust capabilities for stream processing applications, including efficient data caching mechanisms. Caching optimizes performance by minimizing redundant computations, reducing latency, and enhancing overall throughput within stream processing pipelines.

**Purpose of Caching**

Caching in Streamiz serves several key purposes:
- **Reduction of Redundant Computations**: Stores intermediate results to avoid recomputing data unnecessarily.
- **Performance Optimization**: Minimizes latency by reducing the need for repeated expensive computations or data fetches.
- **Fault Tolerance**: Ensures resilience by maintaining readily accessible intermediate results for recovery during failures.

**Types of Caching**

Streamiz supports one primary type of caching:
1. **State Store Cache**: Enhances processing efficiency by caching state store entries, reducing the overhead of state fetches and updates.

### Configuring Caching
To optimize caching behavior, Streamiz offers configurable parameters:
- **Cache Size**: Defines the maximum size of the cache to balance performance and memory usage. To be fair, this cache size is the maximum cache size per state store. 
- **Enable caching** : By default, caching is disabled for all stateful processors in Streamiz. **This feature will be enabled by default soon.** You can enable the caching feature with `InMemory.WithCachingEnabled()`, `InMemoryWindows.WithCachingEnabled()`, `RocksDb.WithCachingEnabled()` or `RocksDbWindows.WithCachingEnabled()`

``` csharp
// Enable caching for a rocksdb window store
RocksDbWindows.As<string, long>("count-window-store")
        .WithKeySerdes(new StringSerDes())
        .WithValueSerdes(new Int64SerDes())
        .WithCachingEnabled();

// Enable caching for an in-memory window store
InMemoryWindows.As<string, long>("count-window-store")
        .WithKeySerdes(new StringSerDes())
        .WithValueSerdes(new Int64SerDes())
        .WithCachingEnabled();

// Enable caching for an rocksdb key/value store
RocksDb.As<string, long>("count-store")
        .WithKeySerdes(new StringSerDes())
        .WithValueSerdes(new Int64SerDes())
        .WithCachingEnabled();

// Enable caching for an in-memory key/value store
InMemory.As<string, long>("count-store")
        .WithKeySerdes(new StringSerDes())
        .WithValueSerdes(new Int64SerDes())
        .WithCachingEnabled();
```

## Remote storage

Steamiz provides powerful capabilities for real-time stream processing applications, leveraging Kafka as a robust event streaming platform. One of its key features is the ability to manage and store stateful data, crucial for maintaining application state across stream processing operations. While Steamiz offers built-in state stores like RocksDB, In memory store, developers may sometimes need to create custom state stores tailored to specific use cases or integration requirements.

Custom state stores can be beneficial for scenarios such as integrating with existing systems, enhancing performance for specific data access patterns, or ensuring compatibility with proprietary data formats.

### Custom state store 

**Steps to Create Custom State Stores**

1- Implement the State Store

Create a class that extends `IKeyValueStore<Bytes, byte[]>` depending on the nature of your state. This involves defining how the state is stored and accessed. Consider using technologies like in-memory data structures or external databases depending on your use case.

For instance:

``` csharp
public class CustomKeyValueStore : IKeyValueStore<Bytes, byte[]>
{
        /// <summary>
        /// Name of this store
        /// </summary>
        public string Name { get; }

        /// <summary>
        /// Return if the storage is persistent or not.
        /// </summary>
        public bool Persistent { get; }

        /// <summary>
        /// Return if the storage is present locally or not.
        /// If the store is local, an internal kafka topic will be created to offload the data
        /// </summary>
        public bool IsLocally { get; }

        /// <summary>
        /// Is this store open for reading and writing
        /// </summary>
        public bool IsOpen { get; }

        /// <summary>
        /// Constructor with client parameters ..
        /// </summary>
        public CustomKeyValueStore(string uri, string token, string name)
        {
                Name = name;
                // doing something with uri, token etc ..
        }

        /// <summary>
        /// Initializes this state store.
        /// The implementation of this function must register the root store in the context via the
        /// <see cref="ProcessorContext.Register(IStateStore, Internal.StateRestoreCallback)"/> function, where the
        /// first <see cref="IStateStore"/> parameter should always be the passed-in <code>root</code> object, and
        /// the second parameter should be an object of user's implementation
        /// of the <see cref="Internal.StateRestoreCallback"/> interface used for restoring the state store from the changelog.
        /// </summary>
        /// <param name="context">Processor context</param>
        /// <param name="root">Root state (always itself)</param>
        public void Init(ProcessorContext context, IStateStore root)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Flush any cached data
        /// </summary>
        public void Flush()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Close the storage engine.
        /// Note that this function needs to be idempotent since it may be called
        /// several times on the same state store
        /// Users only need to implement this function but should NEVER need to call this api explicitly
        /// as it will be called by the library automatically when necessary
        /// </summary>
        public void Close()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Get the value corresponding to this key.
        /// </summary>
        /// <param name="key">the key to fetch</param>
        /// <returns>The value or null if no value is found.</returns>
        public byte[] Get(Bytes key)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Get an enumerator over a given range of keys. This enumerator must be closed after use.
        /// Order is not guaranteed as bytes lexicographical ordering might not represent key order.
        /// </summary>
        /// <param name="from">The first key that could be in the range, where iteration starts from.</param>
        /// <param name="to">The last key that could be in the range, where iteration ends.</param>
        /// <returns>The enumerator for this range, from smallest to largest bytes.</returns>
        public IKeyValueEnumerator<Bytes, byte[]> Range(Bytes from, Bytes to)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Get a reverser enumerator over a given range of keys. This enumerator must be closed after use.
        /// Order is not guaranteed as bytes lexicographical ordering might not represent key order.
        /// </summary>
        /// <param name="from">The first key that could be in the range, where iteration starts from.</param>
        /// <param name="to">The last key that could be in the range, where iteration ends.</param>
        /// <returns>The reverse enumerator for this range, from smallest to largest bytes.</returns>
        /// <exception cref="InvalidStateStoreException">if the store is not initialized</exception>
        public IKeyValueEnumerator<Bytes, byte[]> ReverseRange(Bytes from, Bytes to)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Return an enumerator over all keys in this store. No ordering guarantees are provided.
        /// </summary>
        /// <returns>An enumerator of all key/value pairs in the store.</returns>
        /// <exception cref="InvalidStateStoreException">if the store is not initialized</exception>
        public IEnumerable<KeyValuePair<Bytes, byte[]>> All()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Return a reverse enumerator over all keys in this store. No ordering guarantees are provided.
        /// </summary>
        /// <returns>A reverse enumerator of all key/value pairs in the store.</returns>
        /// <exception cref="InvalidStateStoreException">if the store is not initialized</exception>
        public IEnumerable<KeyValuePair<Bytes, byte[]>> ReverseAll()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Return an approximate count of key-value mappings in this store.
        /// The count is not guaranteed to be exact in order to accommodate stores
        /// where an exact count is expensive to calculate.
        /// </summary>
        /// <returns>an approximate count of key-value mappings in the store.</returns>
        public long ApproximateNumEntries()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Update the value associated with this key.
        /// </summary>
        /// <param name="key">The key to associate the value to</param>
        /// <param name="value">The value to update, it can be null if the serialized bytes are also null it is interpreted as deletes</param>
        public void Put(Bytes key, byte[] value)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Update the value associated with this key, unless a value is already associated with the key.
        /// </summary>
        /// <param name="key">The key to associate the value to</param>
        /// <param name="value">The value to update, it can be null; if the serialized bytes are also null it is interpreted as deletes</param>
        /// <returns>The old value or null if there is no such key.</returns>
        public byte[] PutIfAbsent(Bytes key, byte[] value)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Update all the given key/value pairs.
        /// </summary>
        /// <param name="entries">A list of entries to put into the store. if the serialized bytes are also null it is interpreted as deletes</param>
        public void PutAll(IEnumerable<KeyValuePair<Bytes, byte[]>> entries)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Delete the value from the store (if there is one).
        /// </summary>
        /// <param name="key">the key</param>
        /// <returns>The old value or null if there is no such key</returns>
        public byte[] Delete(Bytes key)
        {
            throw new NotImplementedException();
        }
}
```

2 - Integrate with Streamiz Application

For using the custom state store within your Streamiz application, you need to write a custom `IKeyValueBytesStoreSupplier`. This supplier will be used to create one or multiple instances of your custom store. Of course, you can add some parameters like URI, API Key/Secret, etc ..

For instance :

``` csharp
    public class CustomStorageSupplier : IKeyValueBytesStoreSupplier
    {
        public String Uri {get;set;}
        public String Token {get;set;}
        public string Name { get; set; }

        public AzureRemoteStorageSupplier(string uri, string token, string name)
        {
            Uri = uri;
            Token = token;
            Name = name;
        }

        public IKeyValueStore<Bytes, byte[]> Get()
            => new CustomKeyValueStore(Uri, Token, Name);

        public string MetricsScope => "custom-remote-storage";
    }
```

3 - Use the supplier

Let's suppose, you would like to materialize a KTable with your custom state store. 

``` csharp
var table = builder.Table("table-input", 
                        Materialized<string, string, IKeyValueStore<Bytes, byte[]>>
                                .Create(new CustomStorageSupplier("uri://", "APIZ?ZLAKZ", "custom-store")));
```

### Azure Table Key/Value store

**This feature is available in `EARLY ACCESS` only.**
 
`1.7.0` introduce a new remote storage. The `Azure Table Store` use under the hood, the Azure Table Client which can be used with a [Table Blob Storage](https://learn.microsoft.com/en-us/azure/storage/tables/table-storage-overview) or a [CosmosDb endpoint](https://learn.microsoft.com/en-us/azure/cosmos-db/table/tutorial-query)

#### How to ?

1- Install this nuget package 

``` 
dotnet add package Streamiz.Kafka.Net.Azure.RemoteStorage
```

2- Use the helper class to materialize the `Azure Remote Store`

``` csharp
var table = builder.Table("table-input", AzureRemoteStorage.As<string, string>());
```

3- Configure it

You can configure the azure endpoint and credentials via two way. 

1- Programmaticaly 
```csharp
var table = builder
                .Table("table-input",
                    AzureRemoteStorage.As<string, string>( 
                            new AzureRemoteStorageOptions
                                {
                                    AccountName = "",
                                    StorageUri = "",
                                    StorageAccountKey = ""
                                }));
```

2- Via Configuration

``` csharp
var config = new StreamConfig<StringSerDes, StringSerDes>{
                ApplicationId = $"test-app",
                BootstrapServers = "localhost:9092"
        };
           
config["azure.remote.storage.uri"] = "URI";
config["azure.remote.storage.account.name"] = "ACCOUNT_NAME";
config["azure.remote.storage.account.key"] = "MASTER_KEY";

/// ....
var table = builder.Table("table-input", AzureRemoteStorage.As<string, string>());
```

#### Behaviour

- Each store is an Azure table. The table name is the store name with only its letters and digits kept (it must start with a letter and have 3 to 63 characters).
- The partition key of an entity is the task id, so each task only reads and writes its own entities.
- Putting a `null` value deletes the entity, and `Delete` returns the previous value.
- `PutAll` writes the entities in transactions of at most 100 entities (an Azure limit). Deletes are sent one by one.
- `Range`, `ReverseRange`, `All` and `ReverseAll` read all the entities of the task and filter and sort them by key bytes in the application, because Azure Tables can't compare byte arrays. They are fine for small stores, but each call reads the task's whole partition.
- `ApproximateNumEntries` returns 0: Azure Tables can't count entities without reading all of them.

### Azure Cosmos DB Key/Value store

**This feature is available in `EARLY ACCESS` only.**

The `Azure Cosmos DB Store` keeps a key/value store in an [Azure Cosmos DB for NoSQL](https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/) container instead of a local store and its changelog topic. It's a separate package from the Azure Table store, so the Cosmos DB SDK is only added when you use it.

#### How to ?

1- Install this nuget package

```
dotnet add package Streamiz.Kafka.Net.Azure.Cosmos.RemoteStorage
```

2- Use the helper class to materialize the `Azure Cosmos DB Store`

``` csharp
var table = builder.Table("table-input", CosmosRemoteStorage.As<string, string>());
```

3- Configure it, programmatically with `CosmosRemoteStorageOptions` or via the configuration :

``` csharp
config.AddConfig("azure.cosmos.remote.storage.connection.string", "AccountEndpoint=https://my-account.documents.azure.com:443/;AccountKey=...;");
config.AddConfig("azure.cosmos.remote.storage.database", "my-database");
```

To use Azure AD instead of a key, set `AccountEndpoint` and `Credential` in code (for example `new DefaultAzureCredential()` from the `Azure.Identity` package, which you add to your application):

``` csharp
var table = builder.Table("table-input", CosmosRemoteStorage.As<string, string>(
    new CosmosRemoteStorageOptions
    {
        AccountEndpoint = "https://my-account.documents.azure.com:443/",
        Credential = new DefaultAzureCredential()
    }));
```

| Option | Configuration key | Default |
| --- | --- | --- |
| `ConnectionString` | `azure.cosmos.remote.storage.connection.string` | a connection string, or `AccountEndpoint` with `AccountKey` or `Credential`, is required |
| `AccountEndpoint` | `azure.cosmos.remote.storage.account.endpoint` | |
| `AccountKey` | `azure.cosmos.remote.storage.account.key` | |
| `Credential` | code only | |
| `DatabaseName` | `azure.cosmos.remote.storage.database` | `streamiz` |
| `ContainerPrefix` | `azure.cosmos.remote.storage.container.prefix` | none |
| `CreateIfNotExists` (bool) | `azure.cosmos.remote.storage.create.if.not.exists` | `true` |
| `Throughput` (int, RU/s of the created containers) | `azure.cosmos.remote.storage.throughput` | the account's default (serverless or shared throughput) |
| `ConsistencyLevel` (`Session`, `Eventual`, ...) | `azure.cosmos.remote.storage.consistency.level` | the account's level |
| `BatchSize` (int, 1 to 100) | `azure.cosmos.remote.storage.batch.size` | `100` operations per transactional batch in `PutAll` |
| `MaxRetryAttemptsOnRateLimitedRequests` (int) | `azure.cosmos.remote.storage.max.retry.attempts.on.rate.limited.requests` | the SDK's default (9) |
| `ConfigureClientOptions` | code only | changes the `CosmosClientOptions` before the client is created |

A value set in code wins over the configuration.

#### Behaviour

- Each store has a container named `ContainerPrefix` + store name (`/`, `\`, `#` and `?` are replaced by `_`), in `DatabaseName`. The database and the container (partition key path `/t`) are created if they don't exist, unless `CreateIfNotExists` is false. If two applications have stores with the same name, give them different prefixes or databases.
- Items are partitioned by task: `{ "id": ..., "t": "<task id>", "k": "<key bytes in lowercase hexadecimal>", "v": "<value bytes in base64>" }`. The hexadecimal strings compare like the key bytes, so `Range`, `ReverseRange`, `All` and `ReverseAll` are queries on the task's partition sorted by `k` (`ORDER BY c.k`), read page by page, and they return the keys in the same order as the local stores. The default indexing policy (a range index on every path) is enough.
- The item id is the hexadecimal key. An id is limited to 1023 bytes, so a key longer than 500 bytes gets the id `h-` + the SHA-256 of the key (the empty key gets `e-`); the full key is still in `k`. Range queries compare `k`, so very long keys can be slower to query: keep keys short when you can.
- Putting a `null` value deletes the item, and `Delete` reads and deletes the item in one transactional batch, returning the previous value.
- `PutAll` keeps the last value of each key, sends the upserts in transactional batches of at most 100 operations (a Cosmos DB limit, all in the task's partition) and deletes the `null` values one by one (a transactional delete fails when the item doesn't exist).
- `ApproximateNumEntries` is a `COUNT` query on the task's partition.
- Throttled requests (HTTP 429) are retried by the SDK, see `MaxRetryAttemptsOnRateLimitedRequests`.
- To use the [Cosmos DB emulator](https://learn.microsoft.com/en-us/azure/cosmos-db/emulator), use its connection string. Accepting its self-signed certificate (for example with `ConfigureClientOptions` and an `HttpClientFactory`) is your responsibility, and must not be done in production.

### GCP Bigtable Key/Value store

**This feature is available in `EARLY ACCESS` only.**

The `GCP Bigtable Store` keeps a key/value store in a [Cloud Bigtable](https://cloud.google.com/bigtable) table instead of a local store and its changelog topic.

#### How to ?

1- Install this nuget package

```
dotnet add package Streamiz.Kafka.Net.Gcp.RemoteStorage
```

2- Use the helper class to materialize the `GCP Bigtable Store`

``` csharp
var table = builder.Table("table-input", GcpRemoteStorage.As<string, string>());
```

3- Configure it, programmatically with `GcpRemoteStorageOptions` (`GcpRemoteStorage.As<string, string>(new GcpRemoteStorageOptions { ProjectId = "...", InstanceId = "..." })`) or via the configuration :

``` csharp
config["gcp.remote.storage.project.id"] = "my-project";
config["gcp.remote.storage.instance.id"] = "my-instance";
```

| Option | Configuration key | Default |
| --- | --- | --- |
| `ProjectId` | `gcp.remote.storage.project.id` | required |
| `InstanceId` | `gcp.remote.storage.instance.id` | required |
| `TableId` | `gcp.remote.storage.table.id` | `streamiz-state` |
| `ColumnFamily` | `gcp.remote.storage.column.family` | `v` |
| `AppProfileId` | `gcp.remote.storage.app.profile.id` | the instance's default app profile |
| `EmulatorHost` | `gcp.remote.storage.emulator.host` | the `BIGTABLE_EMULATOR_HOST` environment variable, if it's set |
| `CredentialsPath` | `gcp.remote.storage.credentials.path` | Application Default Credentials |
| `CreateTableIfNotExists` | `gcp.remote.storage.create.table` | `true` (needs the Bigtable admin permissions) |
| `BatchSize` | `gcp.remote.storage.batch.size` | `500` rows per `MutateRows` request in `PutAll` |

How the data is stored :
- All the stores share one table by default: each row key starts with the application id, the store name and the task id (each prefixed by its length), followed by the key bytes. So the rows of a task are contiguous and sorted like the keys of the local stores, and ranges are Bigtable range scans (reverse ranges use reversed scans).
- Each row holds the value in one cell (column `value` of the column family). Writing a value replaces the previous cell, and a null value deletes the row.
- `Delete` reads the old value and then deletes the row (it's not atomic, the task is the only writer of its rows).
- `ApproximateNumEntries` returns 0: Bigtable has no cheap row count.
- When `CreateTableIfNotExists` is true, the table is created with the column family (garbage collection: 1 version) if it doesn't exist. Otherwise, create the table and the column family before starting the application.

### AWS DynamoDB Key/Value store

**This feature is available in `EARLY ACCESS` only.**

The `AWS DynamoDB` remote store keeps a key/value state store in [Amazon DynamoDB](https://aws.amazon.com/dynamodb/). As with the Azure store, the changelog topic is disabled: DynamoDB keeps the data.

#### How to ?

1- Install this nuget package

```
dotnet add package Streamiz.Kafka.Net.Aws.RemoteStorage
```

2- Use the helper class to materialize the `AWS Remote Store`

``` csharp
var table = builder.Table("table-input", AwsRemoteStorage.As<string, string>("my-store"));
```

3- Configure it, in code or via the configuration (the values set in code win)

``` csharp
var table = builder
                .Table("table-input",
                    AwsRemoteStorage.As<string, string>(
                            new AwsRemoteStorageOptions
                                {
                                    Region = "eu-west-1",
                                    TableNamePrefix = "my-app-"
                                },
                            "my-store"));
```

``` csharp
config["aws.remote.storage.region"] = "eu-west-1";
config["aws.remote.storage.table.prefix"] = "my-app-";
// optional: DynamoDB local or LocalStack endpoint
config["aws.remote.storage.service.url"] = "http://localhost:4566";
// optional: explicit credentials (otherwise the default AWS credentials chain is used: environment, profile, instance role, ...)
config["aws.remote.storage.access.key"] = "ACCESS_KEY";
config["aws.remote.storage.secret.key"] = "SECRET_KEY";
config["aws.remote.storage.session.token"] = "SESSION_TOKEN";
// optional: don't create the table if it doesn't exist (a bool, default true)
config["aws.remote.storage.create.table"] = false;
```

#### Behaviour

- Each store is a DynamoDB table named `<prefix><store name>`. Characters which are not allowed in a table name are replaced by `_`.
- Partition key `TaskId` (string): the task id, so each task only reads and writes its own items. Sort key `Key` (binary): the key bytes, with a `0x00` byte in front because a key attribute can't be empty. DynamoDB orders binary sort keys by unsigned bytes, so `Range` and `All` return the keys in the same order as the other stores. Attribute `Value` (binary).
- By default, the table is created with on-demand capacity (`PAY_PER_REQUEST`) if it doesn't exist, and the store waits until it is active. Set `CreateTableIfNotExists = false` to manage the table yourself, with the same key schema.
- Permissions: `dynamodb:GetItem`, `PutItem`, `DeleteItem`, `BatchWriteItem` and `Query` on the table, plus `DescribeTable` and `CreateTable` when the table is created by Streamiz.
- Reads are strongly consistent. Putting a `null` value deletes the item, and `Delete` returns the previous value.
- `PutAll` writes batches of 25 items (a DynamoDB limit) and retries the items DynamoDB didn't process.
- `Range`, `ReverseRange`, `All` and `ReverseAll` are `Query` calls on the task's partition, page by page.
- `ApproximateNumEntries` counts the task's items with a `COUNT` query: it is exact, but it reads all the items of the task.
- A DynamoDB item is at most 400 KB, so a key and a value must fit in it.

### MongoDB Key/Value store

**This feature is available in `EARLY ACCESS` only.**

The `MongoDB Store` keeps a key/value store in a [MongoDB](https://www.mongodb.com) collection instead of a local store and its changelog topic.

#### How to ?

1- Install this nuget package (it targets `netstandard2.1`, `net6.0`, `net7.0` and `net8.0`, like the MongoDB .NET driver 3.x : it can't be used from .NET Framework)

```
dotnet add package Streamiz.Kafka.Net.MongoDb.RemoteStorage
```

2- Use the helper class to materialize the `MongoDB Store`

``` csharp
var table = builder.Table("table-input", MongoDbRemoteStorage.As<string, string>());
```

3- Configure it, programmatically with `MongoDbRemoteStorageOptions` (`MongoDbRemoteStorage.As<string, string>(new MongoDbRemoteStorageOptions { ConnectionString = "mongodb://localhost:27017" })`) or via the configuration :

``` csharp
config["mongodb.remote.storage.connection.string"] = "mongodb://localhost:27017";
```

| Option | Configuration key | Default |
| --- | --- | --- |
| `ConnectionString` | `mongodb.remote.storage.connection.string` | required |
| `DatabaseName` | `mongodb.remote.storage.database` | `streamiz` |
| `CollectionPrefix` | `mongodb.remote.storage.collection.prefix` | no prefix |
| `CreateIndex` | `mongodb.remote.storage.create.index` | `true` (needs the `createIndex` permission) |
| `BatchSize` | `mongodb.remote.storage.batch.size` | `1000` documents per bulk write in `PutAll` |

An option set in code wins over the configuration. The read and write concerns come from the connection string (e.g. `mongodb://host/?w=majority&readConcernLevel=majority`) or the server defaults.

How the data is stored :
- Each store is a collection named prefix + store name (`$` and the null character are replaced by `_`). Use the prefix (e.g. the application id) when several applications have stores with the same name in one database.
- Each key of a task is one document `{ _id: "<task>|<hex key>", t: "<task>", k: "<hex key>", v: <value bytes> }`. The key is a lowercase hex string because hex strings compare like the bytes they encode (BSON binary values compare by length first), so ranges are sorted like the local stores.
- The unique index `{ t: 1, k: 1 }` is created with the simple collation when `CreateIndex` is true, and every query uses the simple collation, even if the collection has another default collation. If `CreateIndex` is false, create this index before starting the application.
- Putting a `null` value deletes the document, and `Delete` returns the previous value (`findOneAndDelete`).
- `PutAll` sends ordered bulk writes of `BatchSize` documents: a later write of a key in the batch wins, and `null` values delete.
- `Range`, `ReverseRange`, `All` and `ReverseAll` are `find` queries on the task's documents sorted by key, read lazily with a cursor.
- `ApproximateNumEntries` counts the task's documents (`countDocuments`): it is exact and uses the index.
- A MongoDB document is at most 16 MB, so a key and a value must fit in it.
