# 13 - Testing

How to unit test a topology with the `TopologyTestDriver`, without Kafka. `src/` holds small topologies (`Topologies.cs`), and `tests/` shows the testing techniques.

```bash
cd examples/13-Testing/tests
dotnet test
```

Docs: [topology test driver](../../docs/topology-test-driver.md).

## Make the topology testable

Expose a method that builds the topology on a `StreamBuilder`, and the topic and store names as constants. The test builds it, starts a driver, and pipes records:

```csharp
var builder = new StreamBuilder();
OrderRouting.Build(builder);
var config = new StreamConfig<StringSerDes, StringSerDes> { ApplicationId = "test-" + Guid.NewGuid() };

using var driver = new TopologyTestDriver(builder.Build(), config);
var orders = driver.CreateInputTopic<string, string>("orders");
var standard = driver.CreateOutputTopic<string, string>("orders-standard");

orders.PipeInput("o-1", "2 coffees");
Assert.AreEqual("2 COFFEES", standard.ReadKeyValue().Message.Value);
```

## The tests

| File | What it shows |
| --- | --- |
| `TopologyTestDriverBasicsTests` | Input and output topics; keys, values, headers and timestamps; `PipeInput`, `PipeInputs`; `ReadKeyValue`, `ReadValueList`, `ReadKeyValueList`, `ReadKeyValuesToMap` |
| `StateStoreTests` | `GetKeyValueStore`, `GetWindowStore`, `GetSessionStore`, `GetVersionedKeyValueStore` |
| `TimeTests` | Stream time driven by record timestamps (window close, suppress, versioned joins); wall-clock time with `AdvanceWallClockTime` |
| `ClusterModeTests` | `ASYNC_CLUSTER_IN_MEMORY` and `IntegrationTestUtils.WaitUntilMinKeyValueRecordsReceived` |
| `ErrorHandlingTests` | Dead letter queue output; exceptions thrown by `PipeInput` |

## Two clocks

- **Stream time** is the largest record timestamp seen so far. It moves only when records arrive, so pass timestamps to `PipeInput` (use a fixed start date, never `DateTime.Now`). Windows close, suppress emits and versioned joins pick a version based on it.
- **Wall-clock time** is simulated by the driver and moves only with `AdvanceWallClockTime`. It drives processing-time punctuators and `UntilWallClockTimeLimit`.

## Two modes

- `SYNC_TASK` (default): each `PipeInput` is processed before it returns, and outputs can be read right away. Deterministic: use it for almost everything.
- `ASYNC_CLUSTER_IN_MEMORY`: an in-memory cluster with real stream threads, consumer groups and partitions. Outputs arrive asynchronously (wait for them), and records of different partitions can come out in any order (compare as sets). Use it when the behaviour depends on threads or partitions.

## Pitfalls

- `TestOutputTopic.IsEmpty` and `QueueSize` count the records not read yet by this output topic: check them before reading if you need to know what's pending, since reading consumes the records.
- `AdvanceWallClockTime(d)` moves the wall clock by exactly `d` and runs each due processing-time punctuation once, at the new time: advancing by 3.5 intervals runs a punctuation once, not three times (as in Kafka's `TopologyTestDriver`).
- Use a unique `ApplicationId` per test, so tests don't share state directories.
- Reading a topic consumes it: a second `ReadValueList()` only returns the records produced since the first one.
- Without an exception handler, an exception thrown by the topology is thrown by `PipeInput` in `SYNC_TASK` mode: `Assert.Throws<...>(() => input.PipeInput(...))`.
- Windowed keys need windowed serdes to be read: `new TimeWindowedSerDes<string>(new StringSerDes(), windowSizeMs)`.
- `config.Logger` is used while the driver runs: dispose the logger factory after the driver (declare it first with `using`).
