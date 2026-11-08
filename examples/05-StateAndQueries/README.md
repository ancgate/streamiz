# 05 - State stores and interactive queries

How Streamiz keeps state: in-memory and RocksDB stores, their cache and changelog, and how to read the stores of a running application, on one instance or across instances.

All the examples count page views. The input topic `page-views` has the user id as key and the viewed page as value:

```
alice -> home
bob   -> home
alice -> pricing
```

```bash
cd examples/05-StateAndQueries/src
dotnet run -- <example>
```

The tests (`tests/`) run each example with the `TopologyTestDriver` and show the expected outputs and store contents.

Docs: [stores](../../docs/stores.md), [stateful processors](../../docs/stateful-processors.md), [interactive queries](../../docs/interactive-queries.md), [configuration](../../docs/stream-configuration.md).

## in-memory-store

Counts the views of each page in an in-memory key-value store (`InMemory.As<string, long>("views-per-page")`).

- Input: `page-views`. Output: `page-view-counts` (key: page, value: count as a long).
- The store lives in memory; after a restart it's rebuilt by replaying its whole changelog topic.

```
alice -> home        =>  home -> 1
bob   -> home        =>  home -> 2
alice -> pricing     =>  pricing -> 1
```

## rocksdb-store

The same count with `RocksDb.As<string, long>(...)`. The store is on disk under `StateDir`: it can be bigger than the memory, and after a clean restart only the end of the changelog is replayed (from the checkpointed offset). `RocksDbConfigHandler` lets you tune RocksDB per store.

## caching

`WithCachingEnabled(CacheSize.OfMb(1))` puts a cache in front of the store. Between two commits, only the latest value of each key is kept, then written to the store and forwarded:

```
alice -> home, bob -> home, carol -> home
  with the cache, at the next commit  =>  home -> 3
  without the cache (WithCachingDisabled)  =>  home -> 1, home -> 2, home -> 3
```

Less traffic to the store, its changelog and downstream, at the cost of up to `CommitIntervalMs` of delay. `StreamConfig.DefaultStateStoreCacheMaxBytes` sets the default cache size (0 disables it).

## changelog-config

Each store is backed by a compacted changelog topic `<application.id>-<store>-changelog`, which restores the store after a restart or a rebalance.

- `WithLoggingEnabled(new Dictionary<string, string> { ... })` passes topic configs used when Streamiz creates the changelog.
- `WithLoggingDisabled()` removes it: cheaper, but the store starts empty when its task restarts or moves.

```
alice -> home, bob -> home  =>  changelog: home -> 1, home -> 2
```

## local-queries

Materializes four kinds of stores and queries them from the running application every 10 seconds with `KafkaStream.Store(...)`:

| Store | Built with | Query |
| --- | --- | --- |
| Key-value: views per page | `Count(InMemory.As(...))` | `QueryableStoreTypes.KeyValueStore<string, long>()`, `Get`, `All` |
| Window: views per page per minute | `WindowedBy(TumblingWindowOptions.Of(1 min)).Count(InMemoryWindows.As(...))` | `QueryableStoreTypes.WindowStore<string, long>()`, `Fetch(key, from, to)` |
| Session: browsing sessions of a user (30 min gap) | `WindowedBy(SessionWindowOptions.Of(30 min)).Count(InMemorySessions.As(...))` | `QueryableStoreTypes.SessionStore<string, long?>()`, `Fetch(key)` |
| Versioned: user profiles with 1 day of history | `builder.Table("user-profiles", InMemoryVersioned.As(...))` | `QueryableStoreTypes.VersionedKeyValueStore<string, string>()`, `Get(key, asOf)` |

Topics: `page-views`, `user-profiles` (key: user id, value: profile).

```
user-profiles: alice -> plan=free (09:00), alice -> plan=pro (10:30)
page-views:    alice -> home (10:00:10), bob -> home (10:00:40), carol -> home (10:01:05), alice -> pricing (10:01:20)

views of home                 => 3
home per minute               => 10:00 -> 2, 10:01 -> 1
sessions of alice             => [10:00:10, 10:01:20] -> 2 views
profile of alice now / 10:00  => plan=pro / plan=free
```

The queries are in `PageViewQueries`: they only use the read-only store interfaces, so the same code runs against the stores of the `TopologyTestDriver` in the tests. A store can be queried once the stream is `RUNNING` (or with `EnableStaleStores()`, while it starts or restores). Each instance only sees the partitions it hosts.

## interactive-queries-http

Serves the counts of every instance over HTTP (ASP.NET Core minimal API):

- `GET /count/{page}`: the count when this instance hosts the page, otherwise a `307` redirect to the instance which hosts it, and `503` while the instances are unknown (startup, rebalance).
- `GET /instances`: the instances and their partitions (`AllMetadataForStore`).

`config.ApplicationServer` is the `host:port` of this instance: Streamiz publishes it with the partitions of the instance in the internal topic `<application.id>-streamiz-metadata`, so every instance knows where each key lives. `QueryMetadataForKey(store, key, serdes)` returns the host and the partition of a key, and `StoreQueryParameters.WithPartition(partition)` reads only that partition's store. Streamiz doesn't provide the RPC layer: `CountQueryRouter` is the part you write, and it's unit-tested with a fake backend.

Run two instances (each with its own port and state directory):

```bash
APPLICATION_SERVER=localhost:7070 dotnet run -- interactive-queries-http
APPLICATION_SERVER=localhost:7071 dotnet run -- interactive-queries-http
curl -L http://localhost:7070/count/home    # answered by whichever instance hosts "home"
```

## standby-replicas

`config.NumStandbyReplicas = 1` keeps a copy of each store on another instance, fed from the changelog. When an instance dies, the instance that takes over its partitions already has the data: it only replays the end of the changelog (with RocksDB stores; in-memory standbys are rebuilt). Meanwhile `EnableStaleStores()` lets you query the local standby copies, and `QueryMetadataForKey(...).StandbyHosts` lists the instances which have one.

Try a failover:

```bash
APPLICATION_SERVER=localhost:7070 dotnet run -- standby-replicas
APPLICATION_SERVER=localhost:7071 dotnet run -- standby-replicas
# each instance prints the active and standby hosts of "home", and its local count (active or standby copy)
# stop one instance: the other takes over its partitions without restoring the whole store
```

The `TopologyTestDriver` runs a single task, so the tests check the configuration and the topology; standbys are covered by the library's own multi-instance tests.
