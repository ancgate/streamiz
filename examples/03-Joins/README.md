# 03 - Joins

Every kind of join Streamiz supports: stream-stream, stream-table, stream-global table, table-table and foreign-key joins, plus versioned tables and grace periods for joins that read a table "as of" a record's time.

All examples use string keys and values. Topics joined by key (stream-stream, stream-table, table-table) must be co-partitioned: same key, same number of partitions. Global tables and foreign-key joins don't have that constraint.

```bash
cd src
dotnet run                          # lists the examples
dotnet run -- orders-payments-join  # runs one against Kafka (KAFKA_BOOTSTRAP, default localhost:9092)
```

The tests in `tests/` run every example with the `TopologyTestDriver` (`dotnet test tests`): read them for more input/output cases.

Documentation: [stateful processors - joins](../../docs/stateful-processors.md).

## Stream-stream joins

Both sides are streams of events. Records are matched by key when their timestamps are at most the join window apart. Each side is buffered in a window store for the duration of the window.

> Streamiz emits the result of a left or outer join as soon as a record arrives without a match (with `null` for the missing side), then again when the match arrives. Kafka Streams 3.x waits for the window to close before emitting non-matched records.

### `orders-payments-join`

Inner join of `orders` (order id → amount) and `payments` (order id → payment method) within 10 minutes, with `JoinWindowOptions` and `StreamJoinProps` (serdes of the window stores). Output: `orders-paid`.

| Input | Output |
| --- | --- |
| `orders` o1 = `120.50` at 12:00 | |
| `payments` o1 = `card` at 12:05 | o1 = `amount=120.50 paid-by=card` |
| `orders` o2 = `35.00` at 12:00, `payments` o2 = `paypal` at 12:15 | nothing: 15 minutes apart |

### `orders-payments-left-join`

Left join: every order gets a status. Output: `orders-payment-status`.

| Input | Output |
| --- | --- |
| `orders` o1 = `120.50` | o1 = `amount=120.50 payment=pending` |
| `orders` o2 = `35.00` | o2 = `amount=35.00 payment=pending` |
| `payments` o1 = `card` (5 min later) | o1 = `amount=120.50 paid-by=card` |

### `orders-payments-outer-join`

Outer join: reconcile both sides. Output: `orders-reconciliation`.

| Input | Output |
| --- | --- |
| `orders` o1 = `120.50` | o1 = `order of 120.50 without payment` |
| `payments` o1 = `card` | o1 = `order of 120.50 paid by card` |
| `payments` o3 = `paypal` | o3 = `payment by paypal without order` |

## Stream-table joins

Each stream record is joined with the current value of the table for its key. Table updates alone don't produce output.

### `clicks-profile-join`

Enrich `page-clicks` (user id → page) with the `user-profiles` table (user id → `name;country`). Clicks of unknown users are dropped. Output: `enriched-clicks`.

| Input | Output |
| --- | --- |
| `user-profiles` u1 = `Alice;FR` | |
| `page-clicks` u1 = `/home` | u1 = `Alice (FR) visited /home` |
| `page-clicks` u2 = `/pricing` | nothing: no profile |
| `user-profiles` u1 = `Alice;DE`, then `page-clicks` u1 = `/checkout` | u1 = `Alice (DE) visited /checkout` |

### `clicks-profile-left-join`

Same with a left join: clicks of unknown users are kept as anonymous clicks.

| Input | Output |
| --- | --- |
| `page-clicks` u2 = `/pricing` (no profile) | u2 = `anonymous visited /pricing` |

## Stream-global table join

### `orders-catalog-global-join`

A global table holds all the partitions of its topic on every instance, so the stream doesn't need the same key: a key mapper picks the table key from the order. `orders` (order id → `productId;quantity`) joined with `product-catalog` (product id → `name;price`). Output: `orders-with-product`.

| Input | Output |
| --- | --- |
| `product-catalog` p1 = `Laptop;999.00` | |
| `orders` o1 = `p1;2` | o1 = `2 x Laptop = 1998.00` |
| `orders` o3 = `p9;1` | nothing: unknown product (use `LeftJoin` to keep it) |

## Table-table joins

### `customer-account-table-joins`

`customers` (id → name) joined with `accounts` (id → balance), as an inner, a left and an outer join. The result is a table, updated when either side changes; deleting a row (a `null` value) deletes the joined row. Outputs: `customer-accounts-inner`, `customer-accounts-left`, `customer-accounts-outer`.

| Input | Inner | Left | Outer |
| --- | --- | --- | --- |
| `customers` c1 = `Alice` | | c1 = `Alice: no account` | c1 = `Alice: no account` |
| `accounts` c1 = `100` | c1 = `Alice: 100` | c1 = `Alice: 100` | c1 = `Alice: 100` |
| `customers` c2 = `Bob` | | c2 = `Bob: no account` | c2 = `Bob: no account` |
| `accounts` c3 = `50` | | | c3 = `unknown customer: 50` |
| `accounts` c1 = `150` | c1 = `Alice: 150` | c1 = `Alice: 150` | c1 = `Alice: 150` |
| `customers` c1 = `null` (delete) | c1 = `null` | ... | ... |

## Foreign-key joins

Join two tables with different keys: a row of the left table references a row of the right table through a field of its value. The result is keyed like the left table and follows changes of both sides, including a change of the foreign key.

### `orders-products-fk-join`

`orders-table` (order id → `productId;quantity`) joined with `products` (product id → name). Output: `orders-enriched`.

| Input | Output |
| --- | --- |
| `products` p1 = `Laptop`, p2 = `Mouse` | |
| `orders-table` o1 = `p1;2` | o1 = `2 x Laptop` |
| `products` p1 = `Laptop Pro` | o1 = `2 x Laptop Pro` |
| `orders-table` o1 = `p2;1` | o1 = `1 x Mouse` |
| `orders-table` o2 = `p9;1` | nothing: unknown product |

### `orders-products-fk-left-join`

Same with a left join.

| Input | Output |
| --- | --- |
| `orders-table` o2 = `p9;1` | o2 = `1 x unknown product` |
| `products` p9 = `Keyboard` | o2 = `1 x Keyboard` |

## Versioned tables and grace periods

A versioned table keeps the history of each key for a retention period (`InMemoryVersioned.As` / `RocksDbVersioned.As`). Joins can then read the value valid at a record's timestamp instead of the latest one.

### `price-as-of-order-time`

`orders-by-product` (product id → `orderId;quantity`) joined with the versioned `prices` table. Each order gets the price valid at its own timestamp. Output: `priced-orders`.

| Input | Output |
| --- | --- |
| `prices` p1 = `10.00` at 12:00:00 | |
| `prices` p1 = `12.00` at 12:01:00 | |
| `orders-by-product` p1 = `o1;2` at 12:00:30 | p1 = `o1: 2 x 10.00` (not 12.00) |
| `orders-by-product` p1 = `o2;1` at 12:01:30 | p1 = `o2: 1 x 12.00` |

### `price-join-with-grace`

`StreamTableJoinProps.WithGracePeriod(1 min)`: orders wait until the stream time is 1 minute past them, so table updates that arrive late (out of order) are still used. Requires a versioned table whose history retention is at least the grace period.

| Input | Output |
| --- | --- |
| `prices` p1 = `10.00` at 12:00:00 | |
| `orders-by-product` p1 = `o1;2` at 12:01:00 | nothing yet: buffered |
| `prices` p1 = `11.00` at 12:00:30 (late) | |
| `orders-by-product` p1 = `o2;1` at 12:02:30 | p1 = `o1: 2 x 11.00` (o2 is buffered in turn) |

### `versioned-table-out-of-order`

An update older than the latest value of its key is stored in the history and emitted by `ToStream()`, but joins and aggregations ignore it, because it isn't the latest value. Outputs: `price-changes` (the table's changes) and `product-prices` (prices joined with `product-names`).

| Input | `price-changes` | `product-prices` |
| --- | --- | --- |
| `product-names` p1 = `Laptop` | | |
| `prices` p1 = `999` at 12:10 | p1 = `999` | p1 = `Laptop: 999` |
| `prices` p1 = `899` at 12:05 (out of order) | p1 = `899` | nothing |
