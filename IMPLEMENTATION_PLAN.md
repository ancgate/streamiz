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
4. **Azure remote store problems:**
   - `Put(key, null)` stores null instead of deleting.
   - `Range` uses a comparer the Azure SDK cannot translate (it only works against the mock), and it doesn't filter by `PartitionKey`.
   - `PutAll` doesn't split into chunks of 100 entities, which Azure requires.
   - `Delete` returns null instead of the old value.
5. **`StateStoreMetrics.RestoreSensor` is defined but never recorded.**

## Recommended order

| Phase | Features | Why this order |
|---|---|---|
| 1. Quick wins (done) | `AlterRecordTimestamp` rename, KIP-613 e2e latency, KIP-424 wall-clock suppress, batch restore, README fixes | Self-contained, low risk, and they warm up the codebase |
| 2. Error handling | Processing exception handler + dead letter queue, at-most-once (bug 3 already fixed) | Users ask for these often; small blast radius |
| 3. Windowing | Sliding windows, then session windows | Session windows build on the store work; sliding is simpler |
| 4. Joins | KTable-KTable FK join | Large but follows a well-known Kafka design (KIP-213) |
| 5. Time-aware state | Versioned key-value store, then KIP-923 stream-table join grace | KIP-923 depends on the versioned store |
| 6. Performance | Batch cache eviction | Needs benchmarks; touches the hot path |
| 7. Async V2 | ParallelConsumer-style async processing | Largest redesign; best done once DLQ exists |
| 8. Remote storage | AWS (DynamoDB), GCP (Bigtable) + Azure fixes | Independent packages, can run in parallel with anything |
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

## Phase 2: Error handling

### 2.1 Processing exception handler + dead letter queue (M)

**Today:**
- Handlers exist for deserialization, production and "inner" errors.
- There is no per-record processing handler (Kafka's KIP-1033), and `StreamTask.Process` has no try/catch.
- There is no DLQ (KIP-1034).
- `InnerExceptionHandler` returning `CONTINUE` now skips the failing record (bug 3, fixed). It still closes all tasks and rejoins the group, which is heavy for a single bad record. The per-record handler below avoids that.

**Plan:**
- **New per-record handler.** Add `ProcessingExceptionHandler(ErrorHandlerContext, record, Exception) → FAIL | CONTINUE`, configured in `StreamConfig`.
  - `ErrorHandlerContext` holds topic, partition, offset, headers, processor node id and task id.
- **Catch per record.** Add a try/catch around the per-record body in `StreamTask.Process`.
  - On `CONTINUE`, the record counts as consumed (offset advances) and a dropped-records sensor is recorded.
  - On `FAIL`, behaviour stays as today.
- **DLQ config.** Add `DeadLetterQueueTopicName` (`errors.deadletterqueue.topic.name`).
  - When it is set and any handler (deserialization, processing, production, async `SKIP`) returns `CONTINUE`, send the original source key/value bytes to the DLQ through the task's `RecordCollector`.
  - Add headers: `__streamiz.errors.exception`, `.message`, `.stacktrace`, `.topic`, `.partition`, `.offset`.
  - Under EOS, the DLQ write is part of the same transaction.
- **Advanced hook.** Handlers can return extra `ProducerRecord`s to send as well.
- **Docs.** Explain when to use the new per-record handler and when to use `InnerExceptionHandler`.

**Files:**
- `StreamConfig.cs`, `ExceptionHandlerResponse.cs`
- `StreamTask.cs`, `AbstractProcessor.cs` (deserialize paths)
- `RecordCollector.cs`, `StreamThread.cs`
- `ExternalProcessorTopologyExecutor.cs`

**Tests:**
- New `ProcessingExceptionHandlerTests` and `DeadLetterQueueTests`, checking DLQ topic contents and headers through the test driver.
- Extend the existing `*ExceptionHandlerTests`.

### 2.2 At-most-once guarantee (S/M)

**Today:** `ProcessingGuarantee` has only `AT_LEAST_ONCE` and `EXACTLY_ONCE`. `StreamsProducer` throws for anything else.

**Plan:**
- Add `AT_MOST_ONCE`.
- In `StreamThread`, right after a poll is added to the tasks, commit the polled positions synchronously, before processing.
- The producer uses no idempotence and no retries. `TaskManager.CommitOffsetsOrTransaction` then becomes a no-op for offsets in this mode.
- Make `StreamsProducer`, `RecordCollector` and `ExternalStreamThread` handle the new value.
- Validate config: reject combining it with EOS-only settings.

**Tests:**
- `StreamConfigTests`.
- A `TaskManagerTests` / `StreamThreadTests` case: crash after commit and before processing, and assert the record is not replayed.

### 2.3 Async bug fixes (done)

Bugs 1 and 2 turned out not to be real (see section 0). Bug 3 is fixed, with tests in `InnerExceptionHandlerTests` and `KStreamMapAsyncTests`.

---

## Phase 3: Windowing

### 3.1 Sliding windows, KIP-450 (L)

**Today:** nothing. The current window API (`WindowOptions<W>.WindowsFor(ts)`) assumes fixed windows computed per record. Sliding windows can't use it, because they depend on the records already in the store.

**Plan:**
- **Options.** `SlidingWindowOptions.Of(timeDifference[, grace])`. It does not extend `WindowOptions<W>`, matching Kafka.
- **DSL.** `IKGroupedStream.WindowedBy(SlidingWindowOptions)` returns `ITimeWindowedKStream<K,V>`, so `Count`, `Aggregate` and `Reduce` work unchanged for users.
  - `TimeWindowedKStream` gets a second constructor that picks the sliding processor supplier.
- **Processor.** New `KStreamSlidingWindowAggregate` supplier + `KStreamSlidingWindowAggregateProcessor`, ported from Kafka's algorithm. For each record at time `t`:
  - fetch windows for the key with start in `[t - 2·timeDiff, t + timeDiff]`, using `IReadOnlyWindowStore.Fetch(key, from, to)`;
  - update every window containing `t`;
  - create the left window `[t - timeDiff, t]` if it is missing, seeding it with the aggregate of the records it covers;
  - create the right window `[t + 1, t + 1 + timeDiff]` if a later record exists and that window is missing;
  - drop late records past grace and record `droppedRecordsSensor`.
- **Store and serde.** Store retention is `timeDiff * 2 + grace` (validated in `WindowStoreMaterializer`). Windows are fixed size, so `TimeWindowedSerDes` is reused.
- **Store fix.** Implement the missing reverse fetch (`IReadOnlyWindowStore` TODO) to make the scan efficient. If it slips, use a forward fetch.

**Tests:**
- New `SlidingWindowKStreamCountTests`, `...AggTests`, `...ReduceTests`, modelled on the existing `TimeWindowKStream*Tests`.
- Port Kafka's `KStreamSlidingWindowAggregateTest` cases for out-of-order records, grace and early records.

### 3.2 Session windows (XL, split into 3 PRs)

**Today:** only empty interfaces.

**PR A: store layer**
- `SessionWindow : Window`, with an inclusive end and its own `Overlap`.
- `ISessionStore<K,AGG>` members:
  - `FindSessions(key, earliestSessionEndTime, latestSessionStartTime)`
  - `Remove(Windowed<K>)`
  - `Put(Windowed<K>, AGG)`
  - `FetchSession(key, start, end)`
- `IReadOnlySessionStore` members: `Fetch(key)` and `Fetch(from, to)`.
- `SessionKeySchema : IKeySchema`, with layout `key | end(8) | start(8)`.
- RocksDB store: `RocksDbSessionBytesStoreSupplier` produces `RocksDbSessionStore`, built on the existing `RocksDbSegmentedBytesStore` with the new key schema.
- `InMemorySessionStore` (+ supplier).
- Wrappers:
  - `MeteredSessionStore`, `CachingSessionStore` (reusing `SegmentedCacheFunction`), `ChangeLoggingSessionBytesStore`.
  - `SessionStoreBuilder` and `Stores.PersistentSessionStore` / `InMemorySessionStore`.
- Uncomment the `Materialized` overloads and add `InMemorySessions` / `RocksDbSessions` helpers.
- Tests: store tests modelled on `InMemoryWindowStoreTests` / `RocksDbWindowStoreTests`, plus `SessionKeySchemaTests`.

**PR B: DSL**
- `SessionWindowOptions.With(inactivityGap[, grace])`.
- `IKGroupedStream.WindowedBy(SessionWindowOptions)` returns `ISessionWindowedKStream<K,V>`, which offers:
  - `Count`
  - `Aggregate(initializer, aggregator, merger)`
  - `Reduce`
- `KStreamSessionWindowAggregateProcessor`. For each record:
  - find overlapping sessions within the gap;
  - merge them with `merger`;
  - remove the old sessions, forwarding tombstones for them;
  - put the merged session.
- A session variant of `GroupedStreamAggregateBuilder.BuildWindow` (today it is typed to `ITimestampedWindowStore`).
- `SessionWindowedSerDes<K>`, which serialises the key with start and end.
- A session-aware KTable value getter.
- Tests: `SessionWindowKStreamCountTests` / `AggTests` / `ReduceTests` covering merging, late records and grace.

**PR C: queries and test driver**
- `QueryableStoreTypes.SessionStore`.
- `TopologyTestDriver.GetSessionStore`.
- Suppress `UntilWindowClose` support for session-windowed tables.
- Docs.

---

## Phase 4: KTable-KTable foreign-key join, KIP-213 (XL)

**Today:** only the name constants exist.

**API:**
- `IKTable<K,V>.Join<VR,KO,VO>(IKTable<KO,VO> other, Func<V,KO> foreignKeyExtractor, Func<V,VO,VR> joiner, Materialized ...)`
- `LeftJoin` with the same signature.

**Topology:** the same design as Kafka, built from existing pieces. `RepartitionNode` already shows how to add an internal topic, sink and source. There are two new internal topics:
- `...-SUBSCRIPTION-REGISTRATION-topic`, keyed by foreign key and co-partitioned with the right table;
- `...-SUBSCRIPTION-RESPONSE-topic`, keyed by primary key and co-partitioned with the left table.

**Left side (`SubscriptionSendProcessor`):**
- On each left change, extract the old and new FK.
- Send `SubscriptionWrapper{hash(value), instruction, primaryKey}`:
  - a subscribe to the new FK;
  - an unsubscribe to the old FK if it changed.

**Right side, receiving subscriptions (`SubscriptionReceiveProcessor`):**
- Store each subscription in a subscription store keyed by `CombinedKey(fk, pk)`.
- The store's bytes layout is `len(fk) | fk | pk`.
- Look up the right value and send a response.

**Right side, on right-table updates (`ForeignTableJoinProcessor`):**
- Scan the subscription store for the FK prefix and send a response for each matching PK.
- This needs a **prefix scan** on stores. Add `PrefixScan` to `IReadOnlyKeyValueStore` (RocksDB has it natively; in-memory can use `Range` on the sorted map). Or build it on the existing `Range(prefix, prefix+0xFF…)` as a first step.

**Left side, receiving responses (`ResponseJoinProcessor`):**
- Drop the response if the hash no longer matches the current left value (stale).
- Otherwise call the joiner and forward.
- Left join emits `joiner(v, null)` when there is no right value.

**Supporting pieces:**
- `SubscriptionWrapper` / `SubscriptionResponseWrapper` / `CombinedKey` serdes.
- Murmur3 128-bit value hash (a small, self-contained port).
- Internal topic partition counts come from the source tables through `CopartitionSources`.

**Tests:**
- Port Kafka's `KTableKTableForeignKeyJoinIntegrationTest` scenarios to `TopologyTestDriver`: FK changes, deletes on both sides, left join with nulls, out-of-order responses.
- A multi-partition test using `ASYNC_CLUSTER_IN_MEMORY`.

---

## Phase 5: Versioned stores and stream-table join grace

### 5.1 Versioned key-value store, KIP-889 (L)

**Today:** nothing.

**Plan:**
- New interface `IVersionedKeyValueStore<K,V>` with:
  - `Put(key, value, timestamp)`
  - `Get(key)` (latest)
  - `Get(key, asOfTimestamp)`
  - `Delete(key, timestamp)`
  - `HistoryRetention`
- Start with **`InMemoryVersionedKeyValueStore`**: a sorted map from key to a timestamp-ordered list of versions, pruned by history retention.
- Then add **`RocksDbVersionedKeyValueStore`**: a "latest value" column family plus timestamp segments, following Kafka's layout.
- Logging and metering wrappers, `Stores.PersistentVersionedKeyValueStore`, `Materialized` support.
- Source KTables can be materialized as versioned. `TableSourceNode` today assumes timestamped key-value only; relax that check.
- Add `Change<T>.IsLatest` so downstream processors ignore out-of-order updates.

**Tests:** store tests (get-as-of, retention pruning, out-of-order puts) and KTable source tests with versioned materialization.

### 5.2 KIP-923: grace period for stream-table joins (M, needs 5.1)

**Plan:**
- Add `Joined.WithGracePeriod(TimeSpan)` / `StreamTableJoinProps`.
- If a grace period is set, the table must be versioned. Validate this when the topology is built.
- `StreamTableJoinNode` adds a time-ordered buffer store on the stream side, based on the existing `ITimeOrderedKeyValueBuffer` design but storing stream records.
- `KStreamKTableJoinProcessor` buffers each record until `streamTime - grace` passes it, then joins with `versionedGetter.Get(key, record.Timestamp)`.
- Records later than the grace period are joined right away, as today.

**Tests:** extend `KStreamTableJoinTests` and `StreamTableJoinTimestampSynchronizationIntegrationTests` with out-of-order table updates.

---

## Phase 6: Batch cache eviction (M)

**Today:** `MemoryCache` compaction calls one eviction callback per entry. `CachingKeyValueStore` and `CachingWindowStore` each do one `wrapped.Put` and one forward per entry.

**Plan:**
- During `Compact` / over-capacity compaction, collect the evicted dirty entries in eviction order.
- Call a new batch callback once per compaction pass.
- `CachingKeyValueStore` writes values with `wrapped.PutAll(...)` and tombstones with `Delete`, then forwards in the original order.
- `CachingWindowStore` groups by segment.
- The single-entry path stays for removals that aren't compaction.

**Tests:** extend `CacheKeyValueStoreTests`, `MemoryCacheTests` and the caching window store tests to check ordering, tombstones and flush listener output. Check throughput with a benchmark in `PerformanceTests` before and after.

---

## Phase 7: External async processing V2 (XL)

**Today (V1):**
- Each `MapAsync` / `FlatMapAsync` / `ForeachAsync` adds two internal topics (`-request` and `-response`).
- One `ExternalStreamThread` per instance runs every call one at a time with `task.Wait()` and `Thread.Sleep` retries.
- The external thread has no rebalance handling, no EOS, and no state.

**V2 design (similar to ParallelConsumer):**
- **No more request/response topics.** The async processor runs inside the normal stream task.
- **Concurrency per task, ordered by key.**
  - Each async processor keeps a bounded pool of in-flight `Task`s (`MaxConcurrency`, default 16).
  - Records with the same key run in order (per-key queue). Different keys run in parallel.
  - When the in-flight limit is reached, the partition is paused (`consumer.Pause`) and later resumed.
- **Results go back on the stream thread.**
  - Completed calls go into a completion queue.
  - The stream thread drains the queue each loop iteration and forwards downstream in offset order per key, so downstream processors and stores stay single-threaded.
- **Offset tracking.** Commit only up to the highest contiguous completed offset per partition, so no record is lost on a crash.
- **Retries and timeouts** use `async` backoff (`Task.Delay`) with a `CancellationToken` timeout. When retries run out, the record goes to the DLQ (Phase 2) or fails, depending on `RetryPolicy`.
- **Rebalance.** On revoke, cancel or drain in-flight work for the revoked partitions before committing.
- **EOS.** Outputs are produced from the stream thread inside the current transaction and offsets are contiguous, so EOS can be supported.
- **Rollout.**
  - Keep the public API (`MapAsync` etc.).
  - Select the engine with `StreamConfig.AsyncProcessingMode = V1 | V2`, defaulting to V1 for one release, then switch the default.
  - Remove V1 and the `ExternalStreamTask` stubs in the next major version.

**Tests:**
- The existing `KStream*AsyncTests` must pass under both modes.
- New tests:
  - per-key ordering when completions arrive out of order;
  - backpressure and pause;
  - revoke while calls are in flight;
  - commit offsets with gaps.
- An integration test with a slow fake external service, checking throughput scales with `MaxConcurrency`.

---

## Phase 8: Remote storage for AWS and GCP (M each)

**Template:** copy the shape of `remote/Streamiz.Kafka.Net.Azure.RemoteStorage`:
- an `XRemoteStorage.As<K,V>()` helper returning `Materialized` with logging disabled;
- an `XRemoteStorageSupplier : IKeyValueBytesStoreSupplier`;
- an `XRemoteStorageOptions` class with `[StreamConfigProperty]` options;
- `Internal/XStore : IKeyValueStore<Bytes,byte[]>` with `IsLocally = false`, plus a virtual client factory so tests can mock it;
- a signed `InternalsVisibleTo` in `core/KafkaStream.cs`.

**Wiring:** add each new project to the `.sln`, the test csproj, `release.yml`, `update-version.sh`, the README package table and `docs/stores.md`.

**AWS (`Streamiz.Kafka.Net.Aws.RemoteStorage`)**
- DynamoDB, through `AWSSDK.DynamoDBv2`.
- Partition key = task id, sort key = key bytes. Range queries are then native and ordered.
- `BatchWriteItem` in chunks of 25.

**GCP (`Streamiz.Kafka.Net.Gcp.RemoteStorage`)**
- Bigtable, through `Google.Cloud.Bigtable.V2`.
- Row key = `taskId#keyBytes`, which gives ordered prefix and range reads.
- `MutateRows` for `PutAll`.

**Azure fixes (bug 4), done first so the template is correct:**
- A null value means delete.
- Filter by `PartitionKey`.
- Do range filtering on the client, or use the key's lexicographic encoding for OData.
- Split `PutAll` into chunks of 100.
- Return the old value from `Delete`.

**Tests:**
- Unit tests with mocked clients, like `AzureRemoteTests`.
- Optional integration tests against LocalStack and the Bigtable emulator, behind a CI flag.

**Later:** remote window and session suppliers.

---

## Phase 9: Interactive Queries metadata and standby replicas

Both need to know which instance owns which tasks. Kafka Streams carries that in its custom assignor's user data, and librdkafka gives Streamiz no equivalent. The proposed workaround is a small internal **metadata topic**.

### 9.1 Interactive Queries across instances (L)

**Today:** local only. `KafkaStream.Store(...)` queries the active tasks on this instance. `application.server` is commented out in `StreamConfig`. In `StoreQueryParameters`, partition and stale-store options are commented out.

**Plan:**
1. **Config.** Add `ApplicationServer` (`host:port`).
2. **Metadata topic.**
   - Add a compacted internal topic `<appId>-streamiz-metadata`.
   - After each rebalance (`StreamsRebalanceListener.PartitionsAssigned` / `Revoked`), each instance writes `{instanceId, applicationServer, activeTasks, standbyTasks, generation}`, keyed by instance id. It writes a tombstone on clean shutdown.
   - Every instance reads the topic in the background and keeps a `StreamsMetadataState`.
3. **Public API on `KafkaStream`:**
   - `AllMetadata()`
   - `AllMetadataForStore(storeName)`
   - `QueryMetadataForKey(storeName, key, keySerializer)`, which returns `KeyQueryMetadata{ActiveHost, StandbyHosts, Partition}`. The partition is computed with the same partitioner as the source topic.
4. **`StoreQueryParameters`.** Bring back `WithPartition()` and `EnableStaleStores()`, and implement them in `StreamThreadStateStoreProvider` (which has a TODO for this).
5. **Out of scope:** an RPC layer. As in Kafka Streams, users expose their own HTTP or gRPC endpoint. A sample in `launcher/` would show one with ASP.NET minimal APIs.

**Risk:** metadata can briefly be stale during a rebalance. The generation number lets callers detect it, and the docs must explain it.

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
