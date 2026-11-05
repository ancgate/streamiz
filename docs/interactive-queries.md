# Interactive queries

Interactive queries read the state stores of a running application (`KafkaStream.Store(...)`). Each instance of an application only hosts the tasks (and so the store partitions) assigned to it, so to answer a query for any key you need to know which instance hosts it, and forward the query to it. Streamiz gives you this metadata; like Kafka Streams, it doesn't include the RPC layer: your application exposes its own endpoint (HTTP, gRPC, ...) and calls the other instances.

## Querying the local stores

``` csharp
var store = stream.Store(StoreQueryParameters.FromNameAndType("store", QueryableStoreTypes.KeyValueStore<string, string>()));
var value = store.Get("key");
```

Options of `StoreQueryParameters` :
- `WithPartition(int partition)` : query only the store of the task of this partition (instead of all the local tasks). Combined with `QueryMetadataForKey`, it reads only the partition which can contain the key.
- `EnableStaleStores()` : allow querying while the stream threads are not running (starting, rebalancing, restoring their stores), and query the stores of the standby replicas hosted by this instance (see below). The results can be stale.

## Discovering the other instances

Set `ApplicationServer` to the endpoint (`host:port`) where this instance serves interactive queries :

``` csharp
config.ApplicationServer = "my-host-1:8080";
```

Each instance then publishes its metadata (its endpoint, and the tasks and partitions it hosts) in the compacted internal topic `<application.id>-streamiz-metadata`, as soon as its tasks change (after a rebalance) and every `MetadataHeartbeatIntervalMs` (30 seconds by default). Each instance reads this topic, so it knows every instance of the application. An instance writes a tombstone when it stops cleanly; an instance which crashed is forgotten when its last metadata is older than 3 × `MetadataHeartbeatIntervalMs`.

| Method | Description |
| --- | --- |
| `AllMetadata()` | Every instance of the application which is alive : `HostInfo`, `StateStoreNames`, `TopicPartitions` (and the standby ones). |
| `AllMetadataForStore(storeName)` | The instances which host the store (every instance for a global store). |
| `QueryMetadataForKey(storeName, key, keySerializer)` | The instance (`ActiveHost`) and the partition (`Partition`) of the key. The key is serialized and its partition computed with the producer partitioner (`StreamConfig.Partitioner`, murmur2 by default, like the Java producer) on the number of partitions of the store's source topic. |
| `QueryMetadataForKey(storeName, key, partitioner)` | The same, with your own partitioner (when the source topic is not written with the default partitioner). |
| `MetadataForLocalThreads()` | The local stream threads and their tasks. |

``` csharp
var metadata = stream.QueryMetadataForKey("store", key, new StringSerDes());
if (metadata.ActiveHost.Equals(HostInfo.Parse(config.ApplicationServer)))
{
    // the key is local
    var store = stream.Store(StoreQueryParameters
        .FromNameAndType("store", QueryableStoreTypes.KeyValueStore<string, string>())
        .WithPartition(metadata.Partition));
    return store.Get(key);
}
else
{
    // forward the query to metadata.ActiveHost (your own HTTP/gRPC call)
}
```

## Standby replicas

Set `NumStandbyReplicas` (default 0) to keep copies of the state stores of each stateful task on other instances :

``` csharp
config.NumStandbyReplicas = 1;
```

- **What is replicated** : the tasks with at least one store backed by a changelog topic. A standby replica reads the changelog topics continuously and applies the records to its own copy of the stores (stores with logging disabled, and remote stores, have no standby).
- **Placement** : each instance publishes its tasks in the metadata topic (also when `ApplicationServer` isn't set : the topic is then only used for the placement). Every instance computes the same placement from it : for each task, the instances which don't own it as active are ranked by rendezvous hashing of (task, instance), and the first `NumStandbyReplicas` host a standby. The standbys are spread over the instances, and when an instance joins or leaves, only the standbys which ranked it first move. With fewer other instances than `NumStandbyReplicas`, each task has one standby per other instance.
- **Failover** : when a task becomes active on an instance which hosts its standby (after a rebalance), the standby is closed cleanly first (stores flushed, checkpoint written), then the active task opens the same stores and only restores the changelog records written since that checkpoint. This applies to persistent (RocksDB) stores; an in-memory store is rebuilt from the changelog.
- **Queries** : `KeyQueryMetadata.StandbyHosts` and `StreamsMetadata.StandbyStateStoreNames` / `StandbyTopicPartitions` list the standbys. A standby store is only returned by `Store(...)` with `EnableStaleStores()` (combine it with `WithPartition(...)` to read one partition), since it can lag behind the active task.
- **Threading** : the standby replicas of an instance are updated by one dedicated thread with its own restore consumer (`<client.id>-standby-thread`, listed by `MetadataForLocalThreads()`), so they never slow down the processing or the restoration of the active tasks.

## Caveats

- Without `ApplicationServer`, only the local instance is known : `AllMetadata()` returns it with `HostInfo.Unavailable`. With standby replicas (and no `ApplicationServer`), the other instances are listed too, with `HostInfo.Unavailable`.
- `HostInfo.Unavailable` / `KeyQueryMetadata.NotAvailable` are returned when the metadata isn't known yet (the stream is starting or rebalancing, the store is unknown, the partition count of the source topic can't be read, or the partitioner is `random`).
- The metadata of the other instances can lag behind a rebalance (by the time it takes to publish and read it), and a query can reach an instance which just lost the partition : retry when `Store(...)` throws `InvalidStateStoreException`.
- Dead instances are detected with the wall-clock time of their last publication : keep the clocks of the instances synchronized.
- The topic is created with the other internal topics (1 partition, compacted, `ReplicationFactor` of the configuration).
- The placement of the standby replicas follows the metadata topic, so it lags behind a rebalance too : right after a rebalance, a standby can be missing or temporarily on another instance.
- An instance id is its process id. With persistent stores, it's stored in the state directory, so it survives restarts : each instance of an application needs its own `StateDir` (also when several instances run in the same process).
