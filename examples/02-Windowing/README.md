# 02 - Windowing

Aggregations over time: tumbling, hopping, sliding and session windows, final results with `Suppress`, rate
limiting, grace periods and late records, and reading windowed results back.

Every example runs against Kafka with `dotnet run -- <name>` from `src/` (see the [examples README](../README.md)),
and is tested without Kafka in `tests/` with the `TopologyTestDriver`: the tests show the exact input and output.

Windows use the record timestamp (event time). In the tables below, times are the record timestamps, and a window
is written `[start, end)` (end excluded) or `[start, end]` (end included).

| Example | Shows |
| --- | --- |
| [tumbling-clicks-per-minute](#tumbling-clicks-per-minute) | `TumblingWindowOptions`, windowed `Count`, `TimeWindowedSerDes` |
| [hopping-average-temperature](#hopping-average-temperature) | `HoppingWindowOptions`, windowed `Aggregate` |
| [sliding-failed-logins](#sliding-failed-logins) | `SlidingWindowOptions` |
| [session-count](#session-count) | `SessionWindowOptions`, session `Count` (`long?`), merging sessions |
| [session-cart-total](#session-cart-total) | Session `Aggregate` with a merger |
| [session-last-page](#session-last-page) | Session `Reduce` |
| [tumbling-final-results](#tumbling-final-results) | `Suppress` `UntilWindowClose` |
| [session-final-results](#session-final-results) | `Suppress` `UntilWindowClose` on sessions |
| [rate-limit-stream-time](#rate-limit-stream-time) | `Suppress` `UntilTimeLimit` |
| [rate-limit-wall-clock](#rate-limit-wall-clock) | `Suppress` `UntilWallClockTimeLimit` |
| [late-records-grace](#late-records-grace) | Grace period, late records dropped |
| [read-windowed-results](#read-windowed-results) | Reading windowed keys with `TimeWindowedSerDes` |

Docs: [stateful processors](../../docs/stateful-processors.md) (windowed `Count`/`Aggregate`/`Reduce`, sliding and
session windows, `Suppress`).

## Windowed keys

A windowed aggregation returns a table keyed by `Windowed<K>`: the original key plus its window. To write it to a
topic, use a windowed serdes, and read the topic back with the same one:

- `TimeWindowedSerDes<K>(innerSerdes, windowSizeMs)` for tumbling, hopping and sliding windows. It stores the window
  start; the size rebuilds the end.
- `SessionWindowedSerDes<K>(innerSerdes)` for session windows. It stores the start and the end.

## tumbling-clicks-per-minute

[`TumblingClicksPerMinute.cs`](src/Examples/TumblingClicksPerMinute.cs): clicks per page per minute. Each click
falls in exactly one window, and each click updates its window's count.

`page-clicks` (key: page, value: user) → `clicks-per-minute` (key: windowed page, value: long)

```bash
dotnet run -- tumbling-clicks-per-minute
```

| Input `page-clicks` | Output `clicks-per-minute` |
| --- | --- |
| 08:00:10 home | home [08:00, 08:01) → 1 |
| 08:00:20 about | about [08:00, 08:01) → 1 |
| 08:00:50 home | home [08:00, 08:01) → 2 |
| 08:01:10 home | home [08:01, 08:02) → 1 |

## hopping-average-temperature

[`HoppingAverageTemperature.cs`](src/Examples/HoppingAverageTemperature.cs): 5-minute windows starting every
minute give a moving average. The aggregate is a sum and a count (an average can't be updated from the previous
average alone), turned into the average with `MapValues`.

`temperatures` (key: sensor, value: double) → `temperature-5min-average` (key: windowed sensor, value: double)

```bash
dotnet run -- hopping-average-temperature
```

| Input `temperatures` | Output `temperature-5min-average` |
| --- | --- |
| 08:00:00 s1 → 20 | s1 → 20 in each of [07:56, 08:01), [07:57, 08:02), [07:58, 08:03), [07:59, 08:04), [08:00, 08:05) |
| 08:00:30 s1 → 22 | s1 → 21 in each of the same 5 windows |

## sliding-failed-logins

[`SlidingFailedLogins.cs`](src/Examples/SlidingFailedLogins.cs): a sliding window `[start, start + 10s]` (both ends
included) exists for every group of records at most 10 seconds apart, so no burst is cut by a fixed window
boundary. Windows with at least 3 failures become alerts.

`failed-logins` (key: user, value: IP) → `login-alerts` (key: windowed user, value: long)

```bash
dotnet run -- sliding-failed-logins
```

| Input `failed-logins` | Output `login-alerts` |
| --- | --- |
| 08:00:00 alice | |
| 08:00:01 bob | |
| 08:00:03 alice | |
| 08:00:06 alice | alice [07:59:56, 08:00:06] → 3 |
| 08:00:30 alice | *(too far from the others)* |

A burst at 08:00:08, 08:00:11 and 08:00:14 also gives an alert, for [08:00:04, 08:00:14], although it spans the
08:00:10 boundary of 10-second tumbling windows.

## session-count

[`SessionCount.cs`](src/Examples/SessionCount.cs): a visit lasts while page views are less than 5 minutes apart.
When a view extends a session, or bridges two sessions, the old sessions are deleted downstream (a `null` count)
and the new session is emitted. Session `Count` returns `long?` for that reason, written with
`NullableInt64SerDes` (a `null` count is a real Kafka tombstone).

`page-views` (key: user, value: page) → `page-views-per-session` (key: session-windowed user, value: long?)

```bash
dotnet run -- session-count
```

| Input `page-views` | Output `page-views-per-session` |
| --- | --- |
| 08:00 alice /home | alice [08:00, 08:00] → 1 |
| 08:02 alice /products | alice [08:00, 08:00] → *null*, alice [08:00, 08:02] → 2 |
| 08:20 alice /home | alice [08:20, 08:20] → 1 |
| 08:00 bob, 08:08 bob | bob [08:00, 08:00] → 1, bob [08:08, 08:08] → 1 |
| 08:04 bob (late, bridges both) | both deleted (*null*), then bob [08:00, 08:08] → 3 |

The store can be read with `driver.GetSessionStore` (tests) or interactive queries (`QueryableStoreTypes.SessionStore`).

## session-cart-total

[`SessionCartTotal.cs`](src/Examples/SessionCartTotal.cs): session `Aggregate` takes an initializer, an aggregator
and a merger, which combines two sessions' aggregates when a record bridges them. The aggregate is a class, so
the tombstones of merged sessions stay `null`.

`cart-additions` (key: user, value: `CartItem` JSON) → `cart-per-session` (key: session-windowed user, value: `CartSummary` JSON)

```bash
dotnet run -- session-cart-total
```

| Input `cart-additions` | Output `cart-per-session` |
| --- | --- |
| 08:00 alice book 10 | [08:00, 08:00] → book |
| 08:15 alice lamp 30 | [08:15, 08:15] → lamp |
| 08:08 alice pen 2 | both deleted, then [08:00, 08:15] → book, lamp, pen (42) |

## session-last-page

[`SessionLastPage.cs`](src/Examples/SessionLastPage.cs): session `Reduce` keeps the latest page of each visit,
its exit page.

`page-views-for-exit` (key: user, value: page) → `exit-page-per-session`

```bash
dotnet run -- session-last-page
```

| Input | Last output |
| --- | --- |
| 08:00 /home, 08:01 /products, 08:03 /checkout | alice [08:00, 08:03] → /checkout |

## tumbling-final-results

[`TumblingFinalResults.cs`](src/Examples/TumblingFinalResults.cs): `Suppress(UntilWindowClose(...))` buffers the
updates of each window and emits only the final count once the window closes, at its end + the grace period
(30 seconds). Pass the windows' grace period to `UntilWindowClose`.

`page-clicks-final` (key: page, value: user) → `clicks-per-minute-final`

```bash
dotnet run -- tumbling-final-results
```

| Input `page-clicks-final` | Output |
| --- | --- |
| 08:00:10, 08:00:20, 08:00:50 home; 08:00:30 about | *(nothing yet)* |
| 08:01:20 home (stream time < 08:01:30) | *(nothing yet)* |
| 08:01:31 home | home [08:00, 08:01) → 3, about [08:00, 08:01) → 1 |

## session-final-results

[`SessionFinalResults.cs`](src/Examples/SessionFinalResults.cs): on sessions, `UntilWindowClose` emits each visit
once, with its final count, and none of the intermediate sessions or tombstones. A session can still be extended
up to the inactivity gap after its end, so give gap + grace as the suppress grace period.

`page-views-final` (key: user, value: page) → `visits-final`

```bash
dotnet run -- session-final-results
```

| Input `page-views-final` | Output `visits-final` |
| --- | --- |
| 08:00, 08:02 alice | *(nothing yet)* |
| 08:06 bob | *(nothing: alice's visit could still grow until 08:07)* |
| 08:08 carol | alice [08:00, 08:02] → 2 |

## rate-limit-stream-time

[`RateLimitStreamTime.cs`](src/Examples/RateLimitStreamTime.cs): `UntilTimeLimit` on a table emits the latest value
of a key once a minute has passed since its first buffered update. The minute is measured in stream time, which
only advances with new records.

`stock-prices` (key: symbol, value: double) → `stock-prices-per-minute`

```bash
dotnet run -- rate-limit-stream-time
```

| Input `stock-prices` | Output |
| --- | --- |
| 08:00:00 ACME 10, 08:00:10 ACME 11, 08:00:30 ACME 12, 08:00:40 XYZ 5 | *(nothing yet)* |
| 08:01:10 ACME 13 | ACME → 13 (XYZ's minute ends at 08:01:40) |

## rate-limit-wall-clock

[`RateLimitWallClock.cs`](src/Examples/RateLimitWallClock.cs): `UntilWallClockTimeLimit` measures the minute in real
time: the latest value is emitted even if no new record arrives. The test advances the driver's clock with
`driver.AdvanceWallClockTime`.

`stock-prices-wall-clock` (key: symbol, value: double) → `stock-prices-every-minute`

```bash
dotnet run -- rate-limit-wall-clock
```

| Input | Output |
| --- | --- |
| ACME 10, ACME 11, then 30 s pass | *(nothing yet)* |
| 31 more seconds pass, no new record | ACME → 11 |

## late-records-grace

[`LateRecordsAndGrace.cs`](src/Examples/LateRecordsAndGrace.cs): a window accepts late records until stream time
(the highest timestamp seen) passes its end + the grace period. After that, late records are dropped and counted
in the dropped-records metric. Without a grace argument, tumbling and hopping windows accept late records for one
day; sliding and session windows for none.

`orders-per-minute` (key: shop, value: order id) → `orders-per-minute-counts`

```bash
dotnet run -- late-records-grace
```

| Input `orders-per-minute` | Output |
| --- | --- |
| 08:00:10 o1 | [08:00, 08:01) → 1 |
| 08:01:10 o2 | [08:01, 08:02) → 1 |
| 08:00:50 o3 (late, within grace) | [08:00, 08:01) → 2 |
| 08:02:00 o4 | [08:02, 08:03) → 1 |
| 08:00:40 o5 (late, after 08:01:30) | *(dropped)* |

## read-windowed-results

[`ReadWindowedResults.cs`](src/Examples/ReadWindowedResults.cs): reads the output of `tumbling-clicks-per-minute`
with the same `TimeWindowedSerDes` and turns each windowed count into text.

`clicks-per-minute` (key: windowed page, value: long) → `clicks-per-minute-readable`

```bash
dotnet run -- read-windowed-results
```

| Input `clicks-per-minute` | Output |
| --- | --- |
| home [08:00, 08:01) → 3 | home → 3 clicks from 08:00 to 08:01 |
