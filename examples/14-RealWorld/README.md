# 14 - Real world

End-to-end scenarios on realistic data: the [sample datasets](../data/README.md) of an online shop, a website,
temperature sensors, card transactions and stock trades. Each scenario combines several features (joins, windows,
state, punctuation, error handling) the way a real application does, and its tests replay a whole dataset and check
the results against the facts planted in the data.

| Scenario | Dataset | Shows |
| --- | --- | --- |
| [ecommerce-revenue](#ecommerce-revenue) | ecommerce | Versioned table join (price at the order's time), global table join on a value field, re-keying, hourly revenue per category, a queryable revenue per customer |
| [order-payment-matching](#order-payment-matching) | ecommerce | Stream-stream join within 30 minutes; unpaid orders and late payments with a transformer, a state store and stream-time punctuation |
| [fraud-detection](#fraud-detection) | banking + bad | Sliding windows (velocity), per-account state (unusual amount, 2 countries), merged alerts, dead letter queue for malformed records |
| [iot-averages](#iot-averages) | iot | Hopping windows (moving average), grace period with late and out-of-order readings |
| [iot-alerts](#iot-alerts) | iot | Anomalies with a moving average, offline / back-online devices with stream-time punctuation |
| [clickstream-analytics](#clickstream-analytics) | clickstream | Session windows with a merger (visits, funnel), filtering crawlers, rate-based bot detection, page views per minute |
| [stock-ohlc](#stock-ohlc) | stocks | 1-minute candles (tumbling), 5-minute moving average (hopping), 5% price-move alerts |

## Running a scenario against Kafka

```bash
cd examples/DataGenerator
dotnet run -- create-topics --with-outputs       # input and output topics, 3 partitions

cd ../14-RealWorld/src
dotnet run -- fraud-detection                    # leave it running

# in another terminal
cd examples/DataGenerator
dotnet run -- load banking                       # the dataset, with its event times
dotnet run -- load bad                           # malformed records, for the dead letter queue
dotnet run -- live banking --rate 20 --bad 0.01  # then live transactions
```

Read an output topic with any consumer, e.g. `kcat -C -b localhost:9092 -t banking.fraud-alerts -K '\t'`.
Windowed output keys are binary (`TimeWindowedSerDes` / `SessionWindowedSerDes`): read them with Streamiz, or look
at the values only.

The scenarios read and write JSON with camelCase names ([`CamelJsonSerDes`](src/CamelJsonSerDes.cs), with
System.Text.Json): the format of the data files. The records are in [`Models.cs`](src/Models.cs).

## Tests

```bash
cd examples/14-RealWorld
dotnet test tests
```

The tests read the data files with the generator's own loader (same keys, timestamps and order as `datagen load`),
pipe them into the `TopologyTestDriver` and check the results two ways:

- against an independent computation of the same result with LINQ over the raw data (e.g. the revenue of every
  category and hour), and
- against the documented facts of the dataset (`facts.json`: e.g. accounts A0017 and A0042 are the velocity
  frauds).

A few tests read windowed results from the state stores (interactive queries) rather than from the output topics,
or use a slice of a dataset: the test driver reads big output topics slowly.

## ecommerce-revenue

[`EcommerceRevenue.cs`](src/Examples/EcommerceRevenue.cs)

```
ecommerce.products ──► versioned table (7 days of prices)
ecommerce.customers ─► global table
ecommerce.orders ──► re-key by productId ──► join products (price at orderedAt) ──► join customers (on customerId)
   ├──► re-key by orderId ──► ecommerce.orders-enriched
   ├──► group by category ──► tumbling 1 hour ──► sum ──► ecommerce.revenue-by-category-hourly
   └──► group by customer ──► sum ──► store revenue-per-customer
```

- **Prices change.** The products table is versioned (`InMemoryVersioned`): a stream-table join looks up the
  version valid at the order's timestamp, not the latest. P0007 drops from 193.02 to 144.76 at 2026-01-06T12:00Z: its
  41 orders before are priced 193.02, its 47 orders after 144.76, whatever the order the records are read in.
- **Customers** are reference data read by every instance: a global table, joined on a field of the value
  (`order.CustomerId`), no repartition. A global table isn't synchronized with the stream's time: at startup, it's
  loaded up to its end, so an order can see a tier upgrade that happened after it. Use a (versioned) table when that
  matters.
- Re-keying (`SelectKey`, `GroupBy`) repartitions through internal topics.

Results: 5,000 enriched orders; revenue per category and hour, e.g. the busiest hour is 18:00 UTC; the revenue of
every customer is in the `revenue-per-customer` store.

## order-payment-matching

[`OrderPaymentMatching.cs`](src/Examples/OrderPaymentMatching.cs)

```
ecommerce.orders ──┬──► join payments within 30 minutes ──► ecommerce.orders-paid
ecommerce.payments ┤
                   └──► merge ──► PaymentDeadlineTracker (store order-payment-state, punctuation every minute)
                                    ├──► ecommerce.orders-unpaid
                                    └──► ecommerce.payments-late
```

A join finds the orders paid in time. It can't find what didn't happen: an order without a payment. The
[`PaymentDeadlineTracker`](src/Examples/OrderPaymentMatching.cs) transformer keeps the pending orders in a state
store; every minute of **stream time** (driven by the record timestamps, so replaying a day takes seconds), it reports
the orders older than 30 minutes as unpaid, once. A payment that comes after that is reported as late. Pending orders
are forgotten a day after the order, so the store doesn't grow forever.

Results: 4,495 orders paid within 30 minutes; 505 unpaid (263 never paid, 242 paid late); 242 late payments.

Stream time only moves when records arrive: the last orders of a load are reported once newer records come in (the
test pipes one more order an hour later). Orders and payments are co-partitioned (both keyed by order id), and
stream time is per partition.

## fraud-detection

[`FraudDetection.cs`](src/Examples/FraudDetection.cs)

```
banking.transactions ──┬──► group by account ──► sliding 5 minutes ──► count ≥ 10 ──► velocity alerts ──┐
                       └──► group by account ──► AccountProfile (count, total, last country and time) ──┤
                                                   ──► large-amount and multi-country alerts ──────────┴──► banking.fraud-alerts
malformed values ──► banking.transactions-dlq
```

| Rule | How | Found |
| --- | --- | --- |
| Velocity: 10 transactions within 5 minutes | Sliding windows (`SlidingWindowOptions`): every 5-minute span of an account's transactions | A0017, A0042 |
| Multi-country: 2 countries within 10 minutes | The profile remembers the last country and time | A0099, A0150 |
| Large amount: > 500 EUR and > 10 times the average so far | The profile keeps the count and the total | A0203, A0256 |

No other account of the 300 raises an alert. The 8 malformed records of the `bad` dataset are skipped
(`DeserializationExceptionHandler` returns `CONTINUE`) and copied to `banking.transactions-dlq` with their original
bytes and error headers.

## iot-averages

[`IotMonitoring.cs`](src/Examples/IotMonitoring.cs)

```
iot.readings ──► group by device ──► hopping 10 minutes every minute, grace 5 minutes ──► average ──► iot.temperature-avg-10min
```

A 10-minute moving average per device, updated every minute. Readings arrive out of order: a window accepts a
reading until 5 minutes (of stream time) after its end. The 121 out-of-order readings (30 to 90 seconds late) all
count. D42's readings of 11:00 to 11:09 arrive 15 minutes late and are dropped: D42 has no average for the window
[11:00, 11:10), and its window [10:51, 11:01) has 9 readings, not 10.

## iot-alerts

[`IotMonitoring.cs`](src/Examples/IotMonitoring.cs)

```
iot.readings ──┬──► group by device ──► DeviceStats (moving average) ──► anomalies ──┐
               └──► OfflineDetector (store device-last-seen, punctuation every minute) ┴──► iot.alerts
```

- **Anomalies**: an exponential moving average per device; a reading more than 8°C away from it is an anomaly
  (and doesn't move the average). Found: D07 at 09:30, 09:31 and 09:32, D23 at 10:30.
- **Offline**: the transformer remembers when each device was last heard of; every minute of stream time, a device
  silent for 5 minutes is reported offline, once; its next reading reports it back online. Found: D13 (offline for
  good), D31 (offline at about 09:05, back at 09:40) and D42 (its late readings leave a gap of 11 minutes in what
  arrives).

## clickstream-analytics

[`ClickstreamAnalytics.cs`](src/Examples/ClickstreamAnalytics.cs)

```
clickstream.pageviews ──► drop "bot" / "crawler" user agents
   ├──► group by user ──► tumbling 1 minute ──► count = 61 ──► clickstream.bot-suspects
   ├──► group by user ──► session windows (30 minutes) ──► SessionSummary (views, funnel step) ──► clickstream.sessions
   └──► group by page type ──► tumbling 1 minute ──► count ──► clickstream.pageviews-per-minute
```

- The declared crawler (5,845 views) is filtered out by its user agent.
- Scrapers hide behind a browser user agent but no human views 60 pages a minute: BOT-SCRAPER-1 and BOT-SCRAPER-2 are
  flagged once per minute of their bursts, and nobody else.
- Session windows rebuild the 2,118 human visits (plus 3 scraper sessions). A session window is identified by its
  start and end: when a view extends a session (or bridges two, which merge), the output topic gets a `null` (a
  tombstone) for each session as it was, then the new one. The tests keep the last value of each session window.
  Each session counts its views and keeps the furthest funnel step: 778 reach the cart, 446 the checkout, 331 buy.

## stock-ohlc

[`StockOhlc.cs`](src/Examples/StockOhlc.cs)

```
stocks.ticks ──┬──► group by symbol ──► tumbling 1 minute ──► candle (open, high, low, close, volume) ──► stocks.ohlc-1min
               ├──► group by symbol ──► hopping 5 minutes every minute ──► average ──► stocks.sma-5min
               └──► group by symbol ──► PriceWatch (prices of the last 5 minutes) ──► stocks.alerts
```

- Candles: the first and last tick of each minute (ticks of a symbol are in time order) and the extremes.
- Alerts: a price 5% below the highest of the last 5 minutes (drop) or 5% above the lowest (jump), once per move.
  Found: ACME's crash (drop, at 16:01:30, from a high of 126.35 to 118.83) and GLBX's jump (at 18:30:17, from 59.23 to
  62.91), nothing else.

The same topology works on live crypto trades: map them to the `stocks.ticks` schema (see
[public data sources](../data/README.md#public-real-world-data-sources)).
