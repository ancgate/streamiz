# 04 - Processor API

When the DSL operations aren't enough: your own processors and transformers, state stores you manage yourself, punctuations (code that runs on a schedule), custom state store implementations, global stores, headers and routing to specific downstream processors.

The building blocks:
- `IProcessor<K, V>`: receives each record; terminal (used with `IKStream.Process`).
- `ITransformer<K, V, K1, V1>`: receives each record and returns the record to forward, or `null` to drop it (used with `IKStream.Transform` and `TransformValues`).
- `ProcessorBuilder` / `TransformerBuilder`: build the supplier given to the DSL, from a class or a lambda, optionally with a state store.
- `ProcessorContext<K, V>`: state stores (`GetStateStore`), punctuations (`Schedule`), forwarding (`Forward`), record metadata, wall clock.

```bash
cd src
dotnet run                        # lists the examples
dotnet run -- deduplicate-events  # runs one against Kafka (KAFKA_BOOTSTRAP, default localhost:9092)
```

The tests in `tests/` run every example with the `TopologyTestDriver` (`dotnet test tests`).

Documentation: [Processor API](../../docs/processor-api.md), [state stores](../../docs/stores.md).

## Transformers

### `mask-card-numbers`

`TransformValues` with a lambda transformer: mask the card number of each payment. The key doesn't change, so no repartitioning is needed later. The transformer passes `record.Headers` to the record it returns: a record created without headers would clear them. Input `payments`, output `payments-masked`.

| Input | Output |
| --- | --- |
| pay-1 = `card=4111111111111111;amount=10.00` (header `source: web`) | pay-1 = `card=************1111;amount=10.00` (header `source: web`) |

### `rekey-by-country`

`Transform` with a transformer class (`CountryKeyTransformer`): change the key. Input `user-events` (user id → `country;action`), output `events-by-country`.

| Input | Output |
| --- | --- |
| u1 = `FR;signup` | FR = `u1:signup` |
| u2 = `DE;login` | DE = `u2:login` |

## State stores

### `deduplicate-events`

A transformer with its own state store (added with `TransformerBuilder.StateStore`, backed by a changelog topic): drop events whose id was already forwarded in the last 10 minutes. Returning `null` drops a record. Input `payment-events` (event id → payload), output `payment-events-unique`.

| Input | Output |
| --- | --- |
| evt-1 at 12:00 | evt-1 |
| evt-1 at 12:02 | nothing: duplicate |
| evt-2 at 12:03 | evt-2 |
| evt-1 at 12:11 | evt-1: 11 minutes after the last one |

## Punctuations

`context.Schedule(interval, type, punctuator)` runs code on a schedule, in `Init`:
- `STREAM_TIME`: driven by record timestamps. It runs when the stream time (the highest timestamp seen) has moved forward by the interval since the last run; the first run happens right after the first record. No records, no punctuation.
- `PROCESSING_TIME`: driven by the wall clock, even when no record arrives.

### `count-per-minute`

Count readings per sensor in a store and, every minute of stream time, forward the counts and reset them. Input `sensor-readings`, output `readings-per-minute`.

| Input | Output |
| --- | --- |
| sensor-a at 12:00:00 | sensor-a = `1` (first punctuation, right after the first record) |
| sensor-a at 12:00:10, sensor-b at 12:00:20, sensor-a at 12:00:30 | nothing yet |
| sensor-a at 12:01:05 | sensor-a = `3`, sensor-b = `1` |

### `silent-sensor-alert`

Remember when each sensor was last heard of (wall-clock time) and, every 10 seconds of wall-clock time, alert once on the sensors silent for 30 seconds. Input `sensor-heartbeats`, output `sensor-alerts`.

| Wall clock | Input | Output |
| --- | --- | --- |
| 0s | sensor-1, sensor-2 | |
| 20s | sensor-2 | |
| 30s (punctuation) | | sensor-1 = `silent for 30s` |
| 50s (punctuation) | | sensor-2 = `silent for 30s` |

(In the tests, the exact seconds vary a little: the `TopologyTestDriver` moves its clock slightly further than asked when a punctuation runs.)

## Custom state store

### `custom-state-store`

Implement `IKeyValueStore<Bytes, byte[]>` (here a sorted dictionary, `SortedDictionaryKeyValueStore`) and an `IKeyValueBytesStoreSupplier`, then give the supplier to `Materialized.Create(...)`. Streamiz wraps your store with its usual layers (serdes, changelog, caching, metrics). `Init` must register the store with `context.Register(root, restoreCallback)` so it can be restored from its changelog. Input `page-views` (page → user), output `page-view-counts`.

| Input | Output |
| --- | --- |
| `/home`, `/home`, `/pricing` | `/home` = `1`, `/home` = `2`, `/pricing` = `1` |

## Global stores and global tables

### `global-store-currency-rates`

`StreamBuilder.AddGlobalStore`: a store fed with all the partitions of a topic on every instance, through your own processor (`RatesUpdater`). Any processor can then read it by name. Payments (`currency;amount`) are converted to EUR. Inputs `currency-rates`, `payments`; output `payments-in-eur`.

| Input | Output |
| --- | --- |
| `currency-rates` USD = `0.92` | |
| `payments` pay-1 = `USD;100.00` | pay-1 = `92.00 EUR` |
| `payments` pay-2 = `GBP;10.00` | pay-2 = `10.00 GBP (no rate)` |
| `currency-rates` GBP = `1.17`, then `payments` pay-3 = `GBP;10.00` | pay-3 = `11.70 EUR` |

### `global-table-lookup`

Read the store of a global table from a transformer. A table's store is an `ITimestampedKeyValueStore`: values come with their timestamp. Inputs `product-catalog`, `orders` (order id → `productId;quantity`); output `orders-described`.

| Input | Output |
| --- | --- |
| `product-catalog` p1 = `Laptop` | |
| `orders` o1 = `p1;2` | o1 = `2 x Laptop` |
| `orders` o2 = `p9;1` | o2 = `1 x unknown product p9` |

## Headers and routing

### `trace-headers`

Read and add record headers: keep the `trace-id` header (or create one from the record's position) and add `processed-by`. The headers of the record returned by a transformer become the headers of the output record. Input `orders`, output `orders-traced`.

| Input headers | Output headers |
| --- | --- |
| `trace-id: abc-123` | `trace-id: abc-123`, `processed-by: order-enricher` |
| none (offset 0 of partition 0) | `trace-id: orders-0-0`, `processed-by: order-enricher` |

### `route-to-named-sinks`

`context.Forward(key, value, childName)` sends a record to one downstream processor only, chosen by name. The router returns `null` so nothing else is forwarded. Its two children are sinks named with `To(topic, name)`. Input `orders` (order id → amount); outputs `orders-high-value` (over 1000) and `orders-standard`.

| Input | Output |
| --- | --- |
| o1 = `1500.00` | `orders-high-value`: o1 |
| o2 = `20.00`, o3 = `999.99` | `orders-standard`: o2, o3 |
