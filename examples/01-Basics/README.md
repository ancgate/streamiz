# 01 - Basics

The building blocks of a Streamiz topology: stateless operations on streams, routing, word count, aggregations,
tables, timestamps, repartitioning and deduplication.

Every example runs against Kafka with `dotnet run -- <name>` from `src/` (see the [examples README](../README.md)),
and is tested without Kafka in `tests/` with the `TopologyTestDriver`: the tests are the best place to see the
exact input and output of each example.

Values marked *JSON* are serialized with Streamiz's `JsonSerDes<T>`; the classes are in [`src/Models.cs`](src/Models.cs).

| Example | Shows |
| --- | --- |
| [filter-and-transform](#filter-and-transform) | `Filter`, `FilterNot`, `Peek`, `SelectKey`, `MapValues` |
| [flatmap-order-lines](#flatmap-order-lines) | `FlatMap` |
| [branch-and-merge](#branch-and-merge) | `Branch`, `Merge` |
| [dynamic-routing](#dynamic-routing) | `To` with a topic extractor |
| [word-count](#word-count) | `FlatMapValues`, `GroupBy`, `Count`, `ToStream` |
| [total-per-customer](#total-per-customer) | `GroupByKey`, `Aggregate`, `Reduce` |
| [table-basics](#table-basics) | `Table`, table `Filter` / `MapValues`, updates and tombstones |
| [table-aggregate](#table-aggregate) | Table `GroupBy`, `Aggregate` with adder and subtractor, `Count` |
| [event-time-extractor](#event-time-extractor) | A custom `ITimestampExtractor` |
| [alter-record-timestamp](#alter-record-timestamp) | `AlterRecordTimestamp` |
| [explicit-repartition](#explicit-repartition) | `Repartition` with `Repartitioned` |
| [drop-duplicates](#drop-duplicates) | `DropDuplicate` |

Docs: [stateless processors](../../docs/stateless-processors.md), [stateful processors](../../docs/stateful-processors.md).

## filter-and-transform

[`FilterAndTransform.cs`](src/Examples/FilterAndTransform.cs): drops empty orders and the `test` customer, logs
each order with `Peek`, re-keys by customer with `SelectKey`, and keeps the total with `MapValues`.

`orders` (key: order id, value: `Order` JSON) → `order-totals-by-customer` (key: customer, value: double)

```bash
dotnet run -- filter-and-transform
```

| Input `orders` | Output `order-totals-by-customer` |
| --- | --- |
| `o1` → alice, book, 2 × 10 | `alice` → 20 |
| `o2` → bob, pen, 0 × 1 | *(filtered: empty)* |
| `o3` → test, x, 1 × 5 | *(filtered: test customer)* |
| `o4` → alice, lamp, 1 × 30 | `alice` → 30 |

## flatmap-order-lines

[`FlatMapOrderLines.cs`](src/Examples/FlatMapOrderLines.cs): one order with several lines becomes one record per
product, keyed by product.

`multi-line-orders` (key: order id, value: `MultiLineOrder` JSON) → `product-demand` (key: product, value: int)

```bash
dotnet run -- flatmap-order-lines
```

| Input `multi-line-orders` | Output `product-demand` |
| --- | --- |
| `o1` → [apple × 3, pear × 1] | `apple` → 3, `pear` → 1 |
| `o2` → [] | *(nothing)* |
| `o3` → [apple × 2] | `apple` → 2 |

## branch-and-merge

[`BranchAndMerge.cs`](src/Examples/BranchAndMerge.cs): `Branch` sends each payment to the first branch whose
predicate matches (card, PayPal, Apple Pay, anything else), and `Merge` puts the two wallets back together.

`payments` (key: payment id, value: `Payment` JSON) → `payments-card`, `payments-wallet`, `payments-other`

```bash
dotnet run -- branch-and-merge
```

| Input `payments` | Output topic |
| --- | --- |
| `p1` → card | `payments-card` |
| `p2` → paypal | `payments-wallet` |
| `p3` → cash | `payments-other` |
| `p4` → apple-pay | `payments-wallet` |

## dynamic-routing

[`DynamicRouting.cs`](src/Examples/DynamicRouting.cs): the output topic is computed for each record from its
value. The topics must exist: Streamiz doesn't create output topics.

`site-events` (key: event id, value: `SiteEvent` JSON) → `events-<type>`

```bash
dotnet run -- dynamic-routing
```

| Input `site-events` | Output topic |
| --- | --- |
| `e1` → click by alice | `events-click` |
| `e2` → purchase by bob | `events-purchase` |
| `e3` → click by carol | `events-click` |

## word-count

[`WordCount.cs`](src/Examples/WordCount.cs): splits sentences into words (`FlatMapValues`), re-keys by word
(`GroupBy`, which repartitions), counts in a state store named `word-count-store`, and streams the count updates.

`sentences` (string) → `word-counts` (key: word, value: long)

```bash
dotnet run -- word-count
```

| Input `sentences` | Output `word-counts` |
| --- | --- |
| `Hello world` | `hello` → 1, `world` → 1 |
| `hello Streamiz` | `hello` → 2, `streamiz` → 1 |

## total-per-customer

[`TotalPerCustomer.cs`](src/Examples/TotalPerCustomer.cs): on orders already keyed by customer, `GroupByKey`
(no repartition), then `Aggregate` the total spent (a `double`, a different type than the records) and `Reduce`
to the largest order (the same type as the records).

`customer-orders` (key: customer, value: `Order` JSON) → `customer-totals` (double), `customer-largest-order` (`Order` JSON)

```bash
dotnet run -- total-per-customer
```

| Input `customer-orders` | `customer-totals` | `customer-largest-order` |
| --- | --- | --- |
| alice: book 2 × 10 | alice → 20 | alice → book |
| bob: pen 1 × 2 | bob → 2 | bob → pen |
| alice: lamp 1 × 30 | alice → 50 | alice → lamp |
| alice: mug 1 × 5 | alice → 55 | alice → lamp |

## table-basics

[`TableBasics.cs`](src/Examples/TableBasics.cs): a `KTable` holds the latest value of each key; a `null` value (a
tombstone) deletes it. A table `Filter` turns a key that stops matching into a delete downstream.

`user-countries` (key: user, value: country code) → `user-countries-known`

```bash
dotnet run -- table-basics
```

| Input `user-countries` | Output `user-countries-known` |
| --- | --- |
| `u1` → fr | `u1` → FR |
| `u2` → de | `u2` → DE |
| `u1` → es | `u1` → ES |
| `u2` → unknown | `u2` → *null* (filtered out: deleted) |
| `u1` → *null* | `u1` → *null* (deleted) |

## table-aggregate

[`TableAggregate.cs`](src/Examples/TableAggregate.cs): grouping a table needs an adder and a subtractor, because a
row can move from one group to another. Here the total balance and the number of accounts per branch.

`accounts` (key: account id, value: `Account` JSON) → `branch-balances` (double), `branch-account-counts` (long)

```bash
dotnet run -- table-aggregate
```

| Input `accounts` | `branch-balances` |
| --- | --- |
| `A1` → paris, 100 | paris → 100 |
| `A2` → paris, 50 | paris → 150 |
| `A1` → lyon, 120 | paris → 50 (A1's 100 subtracted), then lyon → 120 |
| `A2` → *null* | paris → 0 |

## event-time-extractor

[`EventTimeExtractor.cs`](src/Examples/EventTimeExtractor.cs): an `ITimestampExtractor` reads the time a measure
was taken from the payload, so windows and joins use when it happened, not when Kafka received it. It falls back
to the partition time if the payload has no timestamp (a negative timestamp would drop the record).

`sensor-readings` (key: sensor, value: `SensorReading` JSON) → `sensor-readings-by-event-time`

```bash
dotnet run -- event-time-extractor
```

| Input `sensor-readings` | Output record timestamp |
| --- | --- |
| measured at 08:00, received at 09:00 | 08:00 |

## alter-record-timestamp

[`AlterTimestamp.cs`](src/Examples/AlterTimestamp.cs): `AlterRecordTimestamp` changes the timestamp in the middle
of a topology: invoices are re-timestamped with their due date. (It replaces `WithRecordTimestamp`, now obsolete.)

`invoices` (key: invoice id, value: `Invoice` JSON) → `invoices-by-due-date`

```bash
dotnet run -- alter-record-timestamp
```

| Input `invoices` (issued at 08:00) | Output |
| --- | --- |
| `i1` → 99, due in 30 days | `i1`, timestamp = due date |
| `i2` → 0 | *(filtered)* |

## explicit-repartition

[`ExplicitRepartition.cs`](src/Examples/ExplicitRepartition.cs): after `SelectKey`, `Repartition` writes the
records to an internal topic named `<application id>-orders-by-customer-repartition` with 4 partitions and reads
them back, so that each customer's orders are in one partition. `GroupBy` and joins do this automatically;
`Repartition` lets you name the topic and choose its partitions and serdes.

`orders-by-id` (key: order id, value: `Order` JSON) → `orders-by-customer`

```bash
dotnet run -- explicit-repartition
```

| Input `orders-by-id` | Output `orders-by-customer` |
| --- | --- |
| `o1` → alice, 2 × book | `alice` → 2 x book |
| `o2` → bob, 1 × pen | `bob` → 1 x pen |

## drop-duplicates

[`DropDuplicates.cs`](src/Examples/DropDuplicates.cs): `DropDuplicate` drops a record when a record of the same key
seen within half the interval before or after it is equal according to the comparer. With 10 minutes, a payment
resent with the same id and content within 5 minutes is dropped. The seen records are kept in a window store.

`payment-requests` (key: payment id, value: `Payment` JSON) → `payment-requests-deduplicated`

```bash
dotnet run -- drop-duplicates
```

| Input `payment-requests` | Output |
| --- | --- |
| 08:00 `p1` → card 10 | `p1` → card 10 |
| 08:01 `p2` → paypal 5 | `p2` → paypal 5 |
| 08:02 `p1` → card 10 | *(duplicate: dropped)* |
| 08:20 `p1` → card 10 | `p1` → card 10 |
| 08:21 `p1` → card 15 | `p1` → card 15 (different amount) |
