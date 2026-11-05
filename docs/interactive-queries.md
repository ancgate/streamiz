# Interactive queries

Interactive queries read the state stores of a running application (`KafkaStream.Store(...)`). Each instance of an application only hosts the tasks (and so the store partitions) assigned to it, so to answer a query for any key you need to know which instance hosts it, and forward the query to it. Streamiz gives you this metadata; like Kafka Streams, it doesn't include the RPC layer: your application exposes its own endpoint (HTTP, gRPC, ...) and calls the other instances.

## Querying the local stores

``` csharp
var store = stream.Store(StoreQueryParameters.FromNameAndType("store", QueryableStoreTypes.KeyValueStore<string, string>()));
var value = store.Get("key");
```

Options of `StoreQueryParameters` :
- `WithPartition(int partition)` : query only the store of the task of this partition (instead of all the local tasks). Combined with `QueryMetadataForKey`, it reads only the partition which can contain the key.
- `EnableStaleStores()` : allow querying while the stream threads are not running (starting, rebalancing, restoring their stores). The results can be stale.

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

## Caveats

- Without `ApplicationServer`, only the local instance is known : `AllMetadata()` returns it with `HostInfo.Unavailable`.
- `HostInfo.Unavailable` / `KeyQueryMetadata.NotAvailable` are returned when the metadata isn't known yet (the stream is starting or rebalancing, the store is unknown, the partition count of the source topic can't be read, or the partitioner is `random`).
- The metadata of the other instances can lag behind a rebalance (by the time it takes to publish and read it), and a query can reach an instance which just lost the partition : retry when `Store(...)` throws `InvalidStateStoreException`.
- Dead instances are detected with the wall-clock time of their last publication : keep the clocks of the instances synchronized.
- The topic is created with the other internal topics (1 partition, compacted, `ReplicationFactor` of the configuration).
