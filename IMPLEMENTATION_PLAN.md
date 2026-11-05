# Streamiz: plan for features that are not implemented yet

This plan covers every feature that `README.md` (the "Compare Kafka Streams vs Streamiz" table) and `roadmap.md` list as missing. Each one was checked against the code on `develop` (version 1.8.2).

Each feature lists what exists today, how it would be built, the files it touches, how it would be tested, and a rough size:

| Size | Meaning |
|------|---------|
| S | under 1 week |
| M | 1–2 weeks |
| L | 2–4 weeks |
| XL | more than a month |

## 0. Findings from the scan

**Build.** With the .NET 8 SDK, the solution builds cleanly. The only warnings say the `net5.0` target is out of support.

**README is out of date.**
- KTable-KTable FK join and batch state restore are marked "Plan for 1.8.0", but neither exists in 1.8.2.
- The table should be corrected as features land.

**Session windows.** The interfaces exist but are empty:
- `core/State/ISessionStore.cs`, `IReadOnlySessionStore.cs` and `Supplier/ISessionBytesStoreSupplier.cs` are marked "NOT IMPLEMENTED FOR MOMENT".
- The `Materialized` overloads for session stores are commented out.

**FK join.** Only naming constants exist (`core/Table/Internal/KTable.cs` lines 35–42). Nothing uses them.

**Partition assignment is the main constraint.**
- Streamiz has no custom partition assignor. It uses the plain librdkafka consumer group with the `Range` strategy.
- Confluent.Kafka does not expose a pluggable assignor.
- So there is no channel to send host or standby information during a rebalance. That blocks standby replicas and remote Interactive Queries, which need their own way to share that information (section 6).

**Bugs found along the way.** These are worth fixing before or alongside the features they affect:
1. ~~Async `BUFFERED` retry can lose records.~~ **Not a bug.** Once a record is buffered, new records queue behind it (FIFO), and offsets are only committed for records already processed. Order and delivery are kept; the regression test `KStreamMapAsyncBufferedKeepsOrderAndDoesNotLoseRecords` covers it.
2. ~~Possible null reference in `ExternalStreamThread`.~~ **Not reachable.** A call with no polled record never adds to the buffer, so it can't be the call that makes the buffer full.
3. **Fixed:** `InnerExceptionHandler` returning `CONTINUE` replayed the failing record forever. This affected both the stream thread and the async (`MapAsync`...) thread. The failing record is now skipped when it is consumed again.
   - Fixing it also exposed a test-infrastructure bug, now fixed. The in-memory cluster's admin client hid `DeleteRecordsAsync` with `new` instead of overriding it, so every async commit in `ASYNC_CLUSTER_IN_MEMORY` tests threw `NotImplementedException`.
4. **Fixed:** Azure remote store problems. `Put(key, null)` didn't delete; `Range` relied on a filter the SDK can't translate and didn't filter by task; `PutAll` wasn't split into chunks of 100; `Delete` didn't return the old value. See Phase 8.
5. **Fixed:** `StateStoreMetrics.RestoreSensor` was defined but never recorded. See Phase 1.

## Recommended order

| Phase | Features | Why this order |
|---|---|---|
| 1. Quick wins (done) | `AlterRecordTimestamp` rename, KIP-613 e2e latency, KIP-424 wall-clock suppress, batch restore, README fixes | Self-contained, low risk, and they warm up the codebase |
| 2. Error handling (done) | Processing exception handler + dead letter queue, at-most-once (bug 3 already fixed) | Users ask for these often; small blast radius |
| 3. Windowing (done) | Sliding windows, then session windows | Session windows build on the store work; sliding is simpler |
| 4. Joins (done) | KTable-KTable FK join | Large but follows a well-known Kafka design (KIP-213) |
| 5. Time-aware state (done) | Versioned key-value store, then KIP-923 stream-table join grace | KIP-923 depends on the versioned store |
| 6. Performance (done) | Batch cache eviction | Needs benchmarks; touches the hot path |
| 7. Async V2 (done, opt-in) | ParallelConsumer-style async processing | Largest redesign; best done once DLQ exists |
| 8. Remote storage (done) | AWS (DynamoDB), GCP (Bigtable) + Azure fixes | Independent packages, can run in parallel with anything |
| 9. Cluster features | Interactive Queries metadata, then standby replicas | Need a new metadata mechanism (no custom assignor) |
| 10. Needs scoping | KEDA autoscaling, broadcast, network shuffle, cross-cluster, "Extensions" | Roadmap entries are one line each; design must be agreed first |

Each feature should ship as its own PR, with unit tests through `TopologyTestDriver`, and with updates to `docs/` and the README table.

---

## Phase 1: Quick wins (done)

### 1.1 Rename `WithRecordTimestamp` to `AlterRecordTimestamp` (done)

- `IKStream.AlterRecordTimestamp` added.
- `WithRecordTimestamp` is kept as an `[Obsolete]` alias that forwards to it. Remove it in the next major version.
- Tests: `KStreamAlterRecordTimestampTests`, including one test for the obsolete alias.

### 1.2 KIP-613: end-to-end latency metrics (done)

- New processor-node metric `record-e2e-latency-avg/min/max`, at INFO level.
- It is recorded at source nodes and at terminal nodes (nodes with no children). The value is the task wall clock minus the record timestamp.
- As in Kafka, a terminal node records just before its parent forwards the record to it.
- Documented in `docs/monitoring.md`; test in `ProcessorNodeMetricsTests`.

### 1.3 KIP-424: suppress until a wall-clock time limit (done)

**API:** `SuppressedBuilder.UntilWallClockTimeLimit<K,V>(TimeSpan, IBufferConfig)`.

**How it works:**
- The buffer stores the wall-clock time at which each key was first buffered. Later updates keep that original timer.
- Instead of a periodic punctuator, one processing-time punctuation is scheduled for when the oldest buffered record is due. It is rescheduled after each eviction, and nothing runs while the buffer is empty.
  - Why: a periodic punctuator made `TopologyTestDriver.AdvanceWallClockTime` step through time in small increments, which emitted records too early in tests.
- After a restart, a one-shot punctuation emits records restored from the changelog.

**Scheduler fixes needed for this:**
- A punctuator can now register or cancel punctuations safely while punctuations are running.
- Cancelled punctuations no longer run once more.
- The "punctuated" count only includes punctuations that actually ran.

**Docs and tests:** documented in `docs/stateful-processors.md`; 8 new tests in `KTableSuppressTests`.

**Restart behaviour (fixed in the cleanup):** a test now restarts a stream thread and checks that the restored record is emitted with no new input (`SuppressWallClockRestoreTests`). It found two bugs, both fixed:
- **The startup check ran before the restore finished.** Streamiz runs punctuations on tasks that are still restoring, so the check saw an empty buffer and never ran again. It now waits until the task is running.
- **Suppress crashed on records without headers.** Both the size estimate and the changelog serialization failed when a record's headers were null.

**Open question:** Kafka Streams only punctuates running tasks, but Streamiz also punctuates restoring ones, so user punctuators can run against stores that are still restoring. Changing this affects every punctuator, so it needs its own decision.

### 1.4 Batch state store restoration (done)

- New internal batch restore callback. `ProcessorStateManager` and `GlobalStateManager` pass each polled batch to a store in one call.
  - Records keep changelog order, with tombstones included.
  - Stores without the callback keep the per-record path.
  - The public `ProcessorContext.Register` is unchanged.
- Store changes:
  - `RocksDbKeyValueStore`: one RocksDB `WriteBatch` per batch.
  - RocksDB segmented (window) store: one write per run of records in the same segment, with exactly the same expired-segment behaviour as record-by-record restore.
  - `InMemoryKeyValueStore`: loops over `Put`.
- **Follow-ups, done in the cleanup:**
  - `InMemoryWindowStore` and the suppress buffer also restore in batches. The suppress buffer updates its metrics once per batch.
  - The restore metric (`restore-rate`, `restore-latency-avg/max`, DEBUG level) is recorded for every batch. Metered stores create it before initializing the inner store, so global stores are covered too.
  - The test mock bug is fixed: `SyncConsumer.Position` returned one past the real position. The batch restore test now uses an odd number of records, which failed before the fix.

### 1.5 README / docs corrections (done)

- The README table now marks batch restoring as supported, and the FK join note no longer says "Plan for 1.8.0".
- `roadmap.md` items are ticked as they land.
- Still optional: silence or drop the `net5.0` end-of-life warnings in a major version.

---

## Phase 2: Error handling (done)

### 2.1 Processing exception handler + dead letter queue (done)

**`ProcessingExceptionHandler` (KIP-1033):**
- Called when a processor or a DSL function throws while processing one record.
- `CONTINUE` skips the record and keeps processing, without closing tasks or rejoining the group. The record counts as dropped.
- `FAIL` (default) keeps today's behaviour: the exception goes to the stream thread, then the `InnerExceptionHandler`.
- The handler gets an `ErrorHandlerContext`: source topic, partition, offset, timestamp, headers, raw key and value, processor name and task id.
- If the handler itself throws, the original exception is raised.
- Deserialization and production errors keep their own handlers.

**`DeadLetterQueueTopicName` (KIP-1034):**
- Skipped source records are sent to this topic when the deserialization handler or the processing handler returns `CONTINUE`, or when an async processor's retry policy ends with `SKIP`.
- Each record keeps its raw key, value, headers and timestamp, plus `__streams.errors.*` headers: exception, message, stacktrace, topic, partition, offset.
- With exactly-once, the record is written in the task's transaction. Streamiz does not create the topic.

**Not covered:**
- Production errors are not sent to the dead letter queue. They are reported asynchronously, so resending from the delivery callback needs its own design.
- Global store sources only log that they can't send to the dead letter queue, because they have no producer.

**Found while testing:** each record is deserialized twice, once for the timestamp extractor and once for processing. So a `DeserializationExceptionHandler` is called twice per bad record; this was already the case before. The dead letter queue only sends from the processing path, so a record is sent once. Deserializing only once would be a good follow-up.

**Tests:** `ProcessingExceptionHandlerTests` (7 tests).

### 2.2 At-most-once guarantee (done)

- New `ProcessingGuarantee.AT_MOST_ONCE`.
- **When offsets are committed:**
  - The stream thread commits each polled partition's next offset right after the poll, before processing.
  - Periodic, revoke and close commits still flush the producer and stores and write checkpoints, but don't commit consumer offsets, so committed offsets never move backwards.
  - The async thread commits each record before calling the external system.
- **Producer settings:** setting the guarantee disables idempotence and sets `MessageSendMaxRetries = 0`, because a producer retry can write an output record twice. Users can raise retries afterwards and accept possible duplicates.
- **Tests:** `AtMostOnceProcessingTests` restarts a thread after a crash. With at-most-once, nothing is processed again; with at-least-once, the failing record is processed again. Also config and producer tests.
- **Not covered by tests:** at-most-once on the async thread (`TopologyTestDriver` forces at-least-once), and records lost on a rebalance.

### 2.3 Async bug fixes (done)

Bugs 1 and 2 turned out not to be real (see section 0). Bug 3 is fixed, with tests in `InnerExceptionHandlerTests` and `KStreamMapAsyncTests`.

---

## Phase 3: Windowing (done)

### 3.1 Sliding windows, KIP-450 (done)

**API:** `IKGroupedStream.WindowedBy(SlidingWindowOptions)`. It returns the existing `ITimeWindowedKStream`, so every Count, Aggregate and Reduce overload works. `SlidingWindowOptions.Of(timeDifference[, grace])`; without a grace argument, there is no grace period.

**Behaviour:**
- Windows are `[start, start + timeDifference]`, inclusive on both ends as in Kafka. They are stored by start, so output keys use `TimeWindowedSerDes<K>(inner, timeDifference)`.
- The processor is a port of Kafka's forward path:
  - in-order and early records, left and right windows, out-of-order records within grace, late records dropped.
  - Each record reads a small range of windows (at most 2·timeDifference + 1).
- The store retention must be at least 2·timeDifference + grace.

**Tests and docs:** 17 tests (Count, Aggregate, Reduce), with each expected result worked out by hand. Documented in `docs/stateful-processors.md`.

**Differences from Kafka:**
- The default retention is still Streamiz's one day; large windows need `WithRetention`.
- Null values are skipped, as Kafka's sliding aggregate does.

### 3.2 Session windows (done)

**Store layer:**
- `SessionWindow` (inclusive ends).
- The `ISessionStore` and `IReadOnlySessionStore` interfaces: `FindSessions`, `FetchSession`, `Fetch`, the backward variants, `Put` and `Remove`.
- `SessionKeySchema`, with the layout `key | end | start` as in Kafka.
- RocksDB and in-memory session stores, with changelog logging, caching and metered layers, and batch restore.
- `Stores.*SessionStore`, `Materialized.Create(ISessionBytesStoreSupplier)`, `InMemorySessions.As` / `RocksDbSessions.As`, `SessionWindowedSerDes<K>`, `QueryableStoreTypes.SessionStore` and `TopologyTestDriver.GetSessionStore`.
- 73 store tests.

**DSL:**
- `IKGroupedStream.WindowedBy(SessionWindowOptions)` returns `ISessionWindowedKStream`, with Count, Aggregate (initializer, aggregator, `Merger`) and Reduce, in the same overload set as time windows.
- The processor is a port of Kafka's:
  - it looks for sessions within ± the gap and merges them;
  - it removes the merged sessions and forwards a tombstone for each;
  - it puts the new session in the store and forwards it, using the session end as the record timestamp.
  - Close time = stream time − grace − gap.
- `Suppress(UntilWindowClose(gap + grace))` works on session tables.
- The session store can be queried from a running `KafkaStream`.
- 21 DSL tests.

**Count tombstones (decided: null):**
- Session `Count` returns `IKTable<Windowed<K>, long?>`, with the new `NullableInt64SerDes` as its default value serde.
- Merged sessions produce real null tombstones, and `Suppress(UntilWindowClose)` drops the ones for sessions that were never emitted.

**Not done:** emit-final strategies (KIP-825). The default retention for sessions is one day, where Kafka uses gap + grace.

---

## Phase 4: KTable-KTable foreign-key join, KIP-213 (done)

**API:** `IKTable.Join` / `LeftJoin(otherTable, foreignKeyExtractor, joiner[, materialized, named])`, in Func and `IValueJoiner` forms.

**Design:** Kafka's subscription/response design.
- Two internal topics:
  - `-subscription-registration-topic`, co-partitioned with the right table;
  - `-subscription-response-topic`, co-partitioned with the left table.
  - Internal topics can now be declared co-partitioned with source topics. Only the foreign-key join uses this, so existing topologies don't change.
- A subscription store keyed by `len(fk) | fk | pk`. The prefix scan uses `Range` with a computed upper bound, so the store interfaces are unchanged.
- Stale responses are dropped by comparing a Murmur3 128-bit hash of the left value (Kafka's seed).
- It handles foreign-key changes, deletes on both sides, null foreign keys, and left-join null semantics.

**Tests:** 33, including a multi-partition run that checks the internal topics' partition counts.

**Not done:**
- `TableJoined` (custom partitioners) and the `(K, V) => KO` extractor overload.
- The wire format follows Kafka's layout but isn't byte-compatible with it.

---

## Phase 5: Versioned stores and stream-table join grace (done)

### 5.1 Versioned key-value store, KIP-889 (done)

**API:**
- `IVersionedKeyValueStore<K,V>`: `Put(key, value, timestamp)` returns Kafka 3.6's return codes; also `Delete`, `Get(key)` and `Get(key, asOfTimestamp)`.
- In-memory and RocksDB implementations. RocksDB keeps a latest-value store plus time segments for older versions.
- Changelog logging, a metered layer, batch restore, `Stores.*Versioned*`, `InMemoryVersioned.As` / `RocksDbVersioned.As`, `QueryableStoreTypes.VersionedKeyValueStore` and `TopologyTestDriver.GetVersionedKeyValueStore`.

**Rules:**
- Puts older than stream time − history retention are refused.
- The changelog is compacted, with `min.compaction.lag.ms` = retention + the window changelog additional retention.

**Versioned table sources:** `builder.Table(topic, InMemoryVersioned.As(...))` works. `Change<T>.IsLatest` exists, and Filter and MapValues pass it on.

**Out-of-order updates (decided: as in Kafka):**
- Out-of-order updates are forwarded downstream with `Change.IsLatest = false`, and `ToStream()` emits them.
- These ignore them: table-table joins (both sides), `groupBy` (so `Count`, `Reduce` and `Aggregate` don't apply them), and the foreign-key join (both sides).
- Records older than the history retention are still dropped.
- Suppress doesn't keep the flag.

**Not supported:**
- Versioned stores for aggregations, global tables, or materialized Filter/MapValues; these throw a topology error.
- After a restart without restoration, the RocksDB store's retention check is lenient until a new record arrives.

### 5.2 KIP-923: grace period for stream-table joins (done)

**API:** `StreamTableJoinProps.WithGracePeriod(TimeSpan)`. The table must be a versioned source table, and the grace period can't be larger than its history retention; both are checked when the topology is built.

**How it works:**
- Stream records wait in a time-ordered buffer, backed by a changelog and restored on restart.
- They are joined with the table value as of their own timestamp once stream time − grace passes them. Records older than that are joined right away.
- With a versioned table, the join always reads the value as of the stream record's timestamp, even without a grace period (KIP-914).

**Tests:** 13, including a thread restart that restores the buffer.

**Differences from Kafka:**
- The buffer is in memory, restored from its changelog; Kafka uses a RocksDB buffer.
- The buffer has no metrics.
- A grace period on a table derived through Filter or MapValues isn't supported.

---

## Phase 6: Batch cache eviction (done)

- **Batching:** the key-value cache gives each compaction pass to the caching store in one batch, oldest first.
  - The store reads all old values first, writes all dirty entries with one `PutAll`, then forwards each entry in its original order, with the same old and new values as before.
  - Each changelog record keeps its own entry's timestamp.
  - Window and session caches keep the per-entry path, because their stores have no batch write.
- **Bug fixed:** an update to a key that was already cached was lost when the cache was over capacity.
  - The new value was dropped, but its size was still counted.
  - The next flush then wrote neither value.
  - A test covers it.
- **Performance:** the gain was small, about 0–15% on a cached RocksDB table. Most of the time goes to the cache's own bookkeeping, which sorts entries by last access on every flush. That would be the next thing to optimize.
- **Still open:** errors in eviction callbacks are only logged, with a null logger, as before.

---

## Phase 7: External async processing V2 (done, opt-in)

**How to enable:** set `StreamConfig.AsyncProcessingMode = AsyncProcessingMode.V2`. The default is still V1.
- The public API is unchanged.
- New setting: `RetryPolicyBuilder.MaxConcurrency(n)`, default 16.

**What changes in V2:**
- The request topic and the external thread are gone. The async processor runs inside the stream task.
- Records for the same key run in order; records for different keys run concurrently, up to `MaxConcurrency`.
- Results are forwarded on the stream thread with their original timestamp and headers.

**Offsets:** a partition commits up to its smallest in-flight record, so a crash can't lose a record.

**Back-pressure:** a saturated processor stops the task from processing, and the task's partitions are paused until the processor is half empty.

**Retries and errors:**
- Retries use non-blocking backoff, and a timeout bounds the total time per record.
- `SKIP` drops the record and sends it to the dead letter queue if one is configured.
- `FAIL` goes through the `ProcessingExceptionHandler`.
- `BUFFERED` retries until the call succeeds.

**Revoke and close:**
- A revoked or closed task waits for its calls, up to max.poll.interval/2, before committing.
- Lost partitions cancel the calls.

**Exactly-once:** results are produced inside the transaction. This isn't tested yet.

**Results:**
- The existing async tests pass in both modes.
- 15 new V2 tests.
- 100 calls of 50 ms each took 366 ms at concurrency 16.

**Deviations:** MapAsync and FlatMapAsync still write their results to the response topic. The mode is only known once the topology is rewritten, and the resulting stream's repartitioning and joins already depend on that topic.

**Next steps:**
- Remove the response topic, which needs the mode to be known when the DSL graph is built.
- Add metrics, such as an in-flight gauge.
- Add an exactly-once test.
- Make V2 the default, then remove V1 in the next major version.

---

## Phase 8: Remote storage for AWS and GCP (done)

### Azure fixes

- `Put(key, null)` and null values in `PutAll` delete the entity.
- `Delete` returns the old value.
- Range and All:
  - query only the task's partition, with a server-side filter on `PartitionKey`;
  - filter and sort by key bytes in the application, since Azure Tables can't compare byte arrays;
  - so each call reads the task's whole partition.
- `PutAll` sends transactions of at most 100 entities.
- Table names keep only letters and digits, start with a letter, and are 3 to 63 characters.
- `ApproximateNumEntries` is still 0, now documented.
- The test mock now rejects filters the real SDK can't translate.

### AWS: `Streamiz.Kafka.Net.Aws.RemoteStorage` (DynamoDB, AWSSDK.DynamoDBv2 4.0.108)

- **Data model:** one table per store, partition key = task id, binary sort key = `0x00` + key bytes, so the order is the same as local stores.
- **Operations:**
  - `Delete` returns the old value (`ALL_OLD`).
  - `PutAll` uses `BatchWriteItem` in batches of 25, retrying unprocessed items.
  - Range and All are paginated queries in either direction.
  - `ApproximateNumEntries` is an exact `COUNT` query.
  - The table can be created on demand (pay per request).
- **Options:** `aws.remote.storage.*`, covering region, service URL (for LocalStack), credentials and a table prefix.
- **Tests:** 26.

### GCP: `Streamiz.Kafka.Net.Gcp.RemoteStorage` (Bigtable, Google.Cloud.Bigtable.V2 3.33.0)

- **Data model:**
  - One shared table by default.
  - Row key = length-prefixed application id, store name and task id, then the key bytes, so tasks, stores and applications are isolated.
  - One cell per row.
- **Operations:**
  - Reverse ranges use reversed scans.
  - `PutAll` uses `MutateRows` in batches.
  - `Delete` reads, then deletes; it's not atomic, but the task is the only writer of its rows.
  - The table can be created on demand.
  - Supports the emulator.
- **Test project:** the test project's Newtonsoft.Json pin moved from 13.0.3 to 13.0.4, which the Google packages require.
- **Tests:** 16.

### Azure Cosmos DB: `Streamiz.Kafka.Net.Azure.Cosmos.RemoteStorage` (Microsoft.Azure.Cosmos 3.63.2)

- **Data model:**
  - One container per store, partitioned by task (`/t`).
  - Item `{id, t, k: hex key, v: base64 value}`.
  - The id is the hex key; keys over 500 bytes use a SHA-256 id instead, because of the 1,023-byte id limit.
- **Operations:**
  - Ordered by `k`; ranges are partition queries with `ORDER BY`, read lazily page by page.
  - `Delete` is atomic: a transactional batch reads then deletes.
  - `PutAll` uses transactional batches of up to 100 operations.
  - The database and container are created if missing.
- **Authentication:** connection string, account endpoint + key, or a `TokenCredential`.
- **Tests:** 25.

### MongoDB: `Streamiz.Kafka.Net.MongoDb.RemoteStorage` (MongoDB.Driver 3.12.0)

- **Data model:**
  - One collection per store.
  - Document `{_id: task|hex, t, k: hex key, v}`.
  - A unique index on `{t, k}` with the simple collation.
- **Why hex keys:** BSON binary values sort by length first, so keys are stored as hex strings, which sort like the bytes.
- **Operations:**
  - `Delete` uses `FindOneAndDelete`, so it returns the old value.
  - `PutAll` uses ordered bulk writes.
- **Targets:** `netstandard2.1` and `net6.0`–`net8.0`. The 3.x driver dropped `netstandard2.0`, so .NET Framework apps can't use this package.
- **Tests:** 21, one of them against a real MongoDB server. It only runs when `STREAMIZ_MONGODB_CONNECTION_STRING` is set.

**Not done:**
- No integration tests against LocalStack, the Bigtable emulator, the Cosmos DB emulator or Azurite.
- No remote window or session stores.

---

## Phase 9: Interactive Queries metadata and standby replicas

Both need to know which instance owns which tasks. Kafka Streams carries that in its custom assignor's user data, and librdkafka gives Streamiz no equivalent. The proposed workaround is a small internal **metadata topic**.

### 9.1 Interactive Queries across instances (done)

**Config:** `ApplicationServer` (host:port) and `MetadataHeartbeatIntervalMs` (30 s).

**Metadata topic:**
- When `ApplicationServer` is set, each instance publishes its threads, tasks and partitions as JSON to the compacted topic `<appId>-streamiz-metadata`, keyed by process id.
- It publishes on every change and on every heartbeat, and writes a tombstone on a clean stop.
- A reader on every instance keeps the state of all instances. An instance is considered dead when its heartbeat is older than 3 intervals.

**API on `KafkaStream`:**
- `AllMetadata()`, `AllMetadataForStore(store)`, `MetadataForLocalThreads()`.
- `QueryMetadataForKey(store, key, serdes | partitioner)`. It uses the producer partitioner, murmur2 by default like Java clients.
- `StoreQueryParameters.WithPartition(n)` and `EnableStaleStores()`.

**Tests:** 12, including two instances on one mock cluster: the reported host holds the key, and the instance's metadata disappears once it's stopped.

**Docs:** `docs/interactive-queries.md`.

**Limitations:**
- Metadata lags a rebalance by a few hundred milliseconds.
- Crashed instances are only dropped after the heartbeat expiry.
- The writer's clock is used for expiry, so clock skew between instances matters.
- No RPC layer: users expose their own HTTP or gRPC endpoint.

### 9.2 Standby replicas (XL, after 9.1)

**Plan:**
1. **Config.** Add `NumStandbyReplicas` (default 0).
2. **Placement.**
   - Every instance computes standby placement the same way, from the member list in the metadata topic.
   - It uses rendezvous hashing on `(taskId, instanceId)` and skips the instance that owns the active task.
3. **`StandbyTask : AbstractTask`.**
   - It has no processing and no source partitions.
   - It owns a `ProcessorStateManager` and checkpoint file.
   - `TaskManager` keeps `standbyTasks` next to `activeTasks`, and creates or closes them when the metadata changes.
4. **Changelog reader.**
   - `StoreChangelogReader` gets a standby mode: partitions assigned with `Assign` (not as a group member) on the restore consumer, which never reach "completed" and are polled continuously in the `StreamThread` loop.
   - Active restoration still has priority.
5. **Promotion.** When a standby's task becomes active, reuse its state directory and checkpoint, so restoration only replays the remaining gap.
6. **Queries.** Stale-store IQ (9.1) can then read standby stores.

**Tests:**
- Use `ASYNC_CLUSTER_IN_MEMORY` with two or three instances: standby catch-up, failover restore time, and promotion keeping the checkpoint.
- Extend `TaskManagerTests` and `StoreChangelogReaderTests`.

---

## Phase 10: Roadmap items that need scoping first

These appear in `roadmap.md` as one-liners. No code exists for any of them. A short design issue should be agreed before work starts:

| Item | Proposed first step |
|---|---|
| **Auto scaling (KEDA)** | KEDA already has a Kafka scaler based on consumer-group lag. First deliverable: docs and a sample `ScaledObject`, plus a lag metric exported through the Prometheus and OpenTelemetry packages. A custom KEDA external scaler only if the lag metric isn't enough. |
| **Broadcast pattern** | Define the semantics: send every record to all instances, or to all partitions? A `BroadcastTo(topic)` sink that writes to every partition of a topic is the simple version. |
| **Network shuffles instead of repartition topics** | Big research item. It changes the fault-tolerance model, since repartition topics are what make recovery possible. Recommend a spike or RFC only. |
| **Cross-cluster** | Clarify the goal: consuming from one cluster and producing to another? That needs separate consumer and producer bootstrap configs and gives up EOS across clusters. |
| **"Extensions Streamiz"** | Clarify: DI and hosting integration (`IHostedService`, `IServiceCollection.AddStreamiz`) is the likely meaning. That is small and can be done any time. |

---

## Testing and development setup

- The .NET 8 SDK is enough to build the whole solution (`dotnet build kafka-stream-net.sln`). In this environment it was installed with `apt-get install dotnet-sdk-8.0` (Ubuntu 24.04).
- Unit tests: `dotnet test test/Streamiz.Kafka.Net.Tests`.
- Integration tests in `test/Streamiz.Kafka.Net.IntegrationTests` need Docker or Kafka (see `environment/`).
- Every feature PR should include:
  - `TopologyTestDriver` tests;
  - store-level unit tests for new stores;
  - updated docs under `docs/`;
  - the README table updated.
