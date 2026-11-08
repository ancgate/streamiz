# Sample data

Realistic datasets to try Streamiz with: an online shop, a website's clickstream, temperature sensors, card
transactions and stock trades. They're synthetic (no real person or company), deterministic (the same files every
time) and small enough to commit, but they have what real data has: reference data that changes, late and
out-of-order events, gaps, outliers, bots and malformed records. Each pattern is planted on purpose and documented
below, so you know what a correct topology must find.

- Every file is [JSON Lines](https://jsonlines.org/): one JSON object per line, UTF-8, camelCase names, times in
  ISO 8601 UTC.
- Every dataset has a `facts.json`: the expected results (counts, the planted anomalies), computed by the generator.
  The [14-RealWorld](../14-RealWorld/README.md) tests check the scenarios against them.
- [`DataGenerator`](../DataGenerator/README.md) (`datagen`) loads a dataset into Kafka with the event times as record
  timestamps, produces new events continuously, and regenerates these files.

```bash
cd examples/DataGenerator
dotnet run -- create-topics                  # the topics of all the datasets (3 partitions)
dotnet run -- load ecommerce                 # as fast as possible
dotnet run -- load iot --speedup 60          # replays 4 hours in 4 minutes
```

| Dataset | Story | Records | Topics |
| --- | --- | --- | --- |
| [ecommerce](#ecommerce) | An online shop over 3 days: customers, products with price changes, orders, payments, shipments | 14,248 | `ecommerce.customers`, `.products`, `.orders`, `.payments`, `.shipments` |
| [clickstream](#clickstream) | 2 days of page views of the shop's website, with a crawler and 2 scrapers | 17,179 | `clickstream.pageviews` |
| [iot](#iot) | 50 temperature sensors, a reading per minute for 4 hours | 11,891 | `iot.devices`, `iot.readings` |
| [banking](#banking) | 2 days of card transactions of 300 accounts, with 6 frauds | 7,698 | `banking.accounts`, `banking.transactions` |
| [stocks](#stocks) | One trading session of 10 stocks, with a crash and a jump | 12,017 | `stocks.ticks` |
| [bad](#bad) | Malformed transactions | 8 | `banking.transactions` |

## How the files become topics

| File | Topic | Key | Record timestamp |
| --- | --- | --- | --- |
| `ecommerce/customers.jsonl` | `ecommerce.customers` | `customerId` | `updatedAt` |
| `ecommerce/products.jsonl` | `ecommerce.products` | `productId` | `updatedAt` |
| `ecommerce/orders.jsonl` | `ecommerce.orders` | `orderId` | `orderedAt` |
| `ecommerce/payments.jsonl` | `ecommerce.payments` | `orderId` | `paidAt` |
| `ecommerce/shipments.jsonl` | `ecommerce.shipments` | `orderId` | `shippedAt` |
| `clickstream/pageviews-*.jsonl` | `clickstream.pageviews` | `userId` | `viewedAt` |
| `iot/devices.jsonl` | `iot.devices` | `deviceId` | `installedAt` |
| `iot/readings-*.jsonl` | `iot.readings` | `deviceId` | `measuredAt` |
| `banking/accounts.jsonl` | `banking.accounts` | `accountId` | `openedAt` |
| `banking/transactions-*.jsonl` | `banking.transactions` | `accountId` | `timestamp` |
| `stocks/ticks.jsonl` | `stocks.ticks` | `symbol` | `timestamp` |
| `bad/transactions-malformed.jsonl` | `banking.transactions` | `key` | `timestamp` (the line wraps the raw `value`) |

The value is the JSON line itself. The record timestamp is the event time, so windows and joins work on when things
happened, not on when they were loaded.

Files are in **arrival order**: the order in which the events reach Kafka. It's the time order, except for the
records that are late or out of order on purpose (iot). `datagen load` keeps the order of each file and interleaves
the topics of a dataset by time, so a join sees a product's new price before the orders that follow it.

The ecommerce topics share their keys on purpose: orders, payments and shipments are keyed by `orderId`, so they're
co-partitioned (same number of partitions) and can be joined without repartitioning. Orders join products on
`productId` and customers on `customerId`: that takes a re-key, or a global table.

## ecommerce

An online shop from 2026-01-05 to 2026-01-07 (UTC). Customers and products exist from 2026-01-04.

| File | Records | Fields |
| --- | --- | --- |
| `customers.jsonl` | 521 (500 customers + 21 tier upgrades) | `customerId` (C0001-C0500), `name`, `email`, `country` (DE, ES, FR, GB, IT, US), `tier` (bronze, silver, gold), `updatedAt` |
| `products.jsonl` | 238 (200 products + 38 price changes) | `productId` (P0001-P0200), `name`, `category` (Beauty, Books, Clothing, Electronics, Grocery, Home, Sports, Toys), `price`, `updatedAt` |
| `orders.jsonl` | 5,000 | `orderId` (O000001-O005000), `customerId`, `productId`, `quantity` (1-3), `orderedAt` |
| `payments.jsonl` | 4,737 | `paymentId`, `orderId`, `amount` (quantity × price at the order's time), `method` (card, paypal, applepay, giftcard), `paidAt` |
| `shipments.jsonl` | 3,752 | `orderId`, `carrier` (DHL, FedEx, LaPoste, UPS), `shippedAt`, `deliveredAt` |

```json
{"orderId":"O000001","customerId":"C0408","productId":"P0099","quantity":3,"orderedAt":"2026-01-05T00:02:24Z"}
{"paymentId":"PAY000001","orderId":"O000003","amount":17.77,"method":"paypal","paidAt":"2026-01-05T00:07:20Z"}
```

Customers and products are changelogs: a new line for a key replaces the previous one (a table). The planted patterns:

- **A price cut.** P0007 goes from 193.02 to 144.76 (-25%) at 2026-01-06T12:00Z, with 41 orders before and 47 after.
  An order must be priced at the price valid when it was placed: a versioned table, or a stream-table join
  processed in time order.
- **Payments.** 90% of the orders are paid 1 to 10 minutes after the order, 5% are paid late (35 to 120 minutes),
  5% are never paid: 4,495 payments within 30 minutes, 242 late, 263 orders never paid.
- **Daily rhythm.** Orders follow the time of day; the busiest hour is 18:00 UTC.
- **A few big customers, a few best sellers.** Orders are skewed towards the first customers and products: C0001
  has paid 11,281.07, the median customer about 850.
- Shipments: 80% of the paid orders, 2 to 24 hours after the payment, delivered 1 to 3 days later.

`facts.json`: counts, the featured product, the busiest hour, the top 5 customers by paid amount.

## clickstream

The shop's website from 2026-01-05 to 2026-01-06, in 4 files of 12 hours.

| Field | |
| --- | --- |
| `userId` | U0001-U1000, BOT-CRAWLER, BOT-SCRAPER-1, BOT-SCRAPER-2 |
| `page` | `/`, `/category/<name>`, `/product/<id>`, `/cart`, `/checkout`, `/confirmation` |
| `userAgent` | a browser, or the crawler's |
| `viewedAt` | |

```json
{"userId":"U0046","page":"/category/electronics","userAgent":"Mozilla/5.0 (Macintosh; Mac OS X 14_6) Safari/605.1","viewedAt":"2026-01-05T00:01:13Z"}
```

- **Visits.** 1,000 users make 2,118 visits (10,134 page views): 5 seconds to 3 minutes between pages, at least
  45 minutes between visits, so a 30-minute session window rebuilds them exactly.
- **A funnel.** A visit goes home or category → product → cart → checkout → confirmation, and can stop at any
  step: 778 visits reach the cart, 446 the checkout, 331 buy.
- **A declared crawler.** BOT-CRAWLER says so in its user agent (`ExampleCrawler/1.0 (+https://example.com/bot)`)
  and views a page every 20 to 40 seconds, day and night: 5,845 views.
- **Scrapers.** BOT-SCRAPER-1 (2026-01-05T03:12Z and 2026-01-06T02:40Z) and BOT-SCRAPER-2 (2026-01-05T15:05Z) use a
  browser user agent, but load 400 pages in about 2 minutes. No human views more than 4 pages in a minute.

`facts.json`: page views, visits, funnel counts, the crawler and the scrapers.

## iot

50 temperature sensors from 08:00 to 12:00 UTC on 2026-01-05.

| File | Records | Fields |
| --- | --- | --- |
| `devices.jsonl` | 50 | `deviceId` (D01-D50), `location` (warehouse-a 18°C, warehouse-b 16°C, office 21°C, cold-room 4°C), `model`, `installedAt` |
| `readings-part1.jsonl`, `readings-part2.jsonl` | 11,841 | `deviceId`, `temperature` (°C), `humidity` (%), `battery` (%), `measuredAt` |

```json
{"deviceId":"D01","temperature":18.95,"humidity":45.2,"battery":94,"measuredAt":"2026-01-05T08:00:01Z"}
```

Each device measures once a minute, at its number of seconds past the minute (D07 at :07). The temperature stays
within about ±1°C of its location's value, except:

- **Spikes.** D07 reads +15°C at 09:30, 09:31 and 09:32; D23 reads -12°C at 10:30.
- **Silences.** D13 stops at 10:01 for good; D31 stops from 09:00 to 09:40.
- **Late readings.** D42's readings of 11:00 to 11:09 arrive 15 minutes late, as if a gateway had buffered them
  (in the file, each one is among the readings measured 15 minutes later). With a 5-minute grace period, they're
  too late for their windows. In the order of arrival, D42 is silent from 10:59 to 11:10.
- **Out of order.** 121 readings (1%) arrive 30 to 90 seconds late: within any reasonable grace period.
- **Low battery.** D44 starts at 12% and keeps draining.

`facts.json`: the spikes, silences, late and out-of-order readings.

## banking

Card transactions of 300 accounts from 2026-01-05 to 2026-01-06, in 2 files of a day.

| File | Records | Fields |
| --- | --- | --- |
| `accounts.jsonl` | 300 | `accountId` (A0001-A0300), `holder`, `homeCountry` (BE, DE, ES, FR, IT, NL, PT), `openedAt` |
| `transactions-*.jsonl` | 7,398 | `transactionId`, `accountId`, `amount`, `currency` (EUR), `country`, `merchant`, `category` (entertainment, groceries, restaurants, shopping, transport), `timestamp` |

```json
{"transactionId":"T000001","accountId":"A0211","amount":33.64,"currency":"EUR","country":"DE","merchant":"Le Bistro","category":"restaurants","timestamp":"2026-01-05T00:00:04Z"}
```

A normal account makes 15 to 35 transactions, at least 15 minutes apart, of 0.3 to 3 times its typical amount
(20 to 150 EUR); 95% in its home country, the others abroad. Six frauds are hidden in them, and nothing else matches
the 3 rules:

| Rule | Accounts |
| --- | --- |
| **Velocity**: at least 10 transactions within 5 minutes | A0017 (2026-01-05T14:00Z), A0042 (2026-01-06T09:30Z): 12 small payments 10 to 15 seconds apart |
| **Multi-country**: 2 countries within 10 minutes | A0099 (2026-01-05T20:15Z, then BR), A0150 (2026-01-06T03:40Z, then TH) |
| **Large amount**: more than 500 EUR and 10 times the account's average so far (after 5 transactions) | A0203 (2026-01-06T16:05Z, 1,201.20), A0256 (2026-01-06T21:50Z, 1,852.40) |

`facts.json`: the rules and the fraudulent accounts.

## stocks

One trading session (14:30 to 21:00 UTC on 2026-01-05) of 10 made-up symbols: ACME, GLBX, INIT, NOVA, ORBT,
PIXL, QNTM, RIVR, SOLR, VRTX.

| File | Records | Fields |
| --- | --- | --- |
| `ticks.jsonl` | 12,017 | `symbol`, `price`, `volume` (shares), `timestamp` |

```json
{"symbol":"ACME","price":124.47,"volume":400,"timestamp":"2026-01-05T14:30:01Z"}
```

A trade every 10 to 30 seconds per symbol; prices follow a random walk with small steps, so no stock moves 5% in
5 minutes by chance. Except:

- **A crash.** ACME loses 8% between 16:00 and 16:02.
- **A jump.** GLBX gains 6% at 18:30.

`facts.json`: open, high, low, close and number of trades of each symbol, the crash and the jump.

## bad

8 malformed values for `banking.transactions`, to see a deserialization exception handler and a dead letter queue
at work: truncated JSON, a string amount, plain text, a number, an array, an invalid date, XML, an out-of-range
number. Each line is `{"key", "value", "timestamp"}`: `datagen load bad` produces `value` as is.

## Public real-world data sources

When you want the real thing, these public sources stream (or publish) real events. Check each source's current terms
before you use it: they change, and some forbid commercial use or redistribution. Whatever the source, bridging it
into Kafka is the same:

1. Read events with an HTTP client: a stream (Server-Sent Events, WebSocket), or a poll.
2. Pick a key that matches how you'll process them (per wiki, per symbol, per aircraft...).
3. Use the event's own time as the record timestamp, so that windows work on event time.
4. When polling, drop the events already seen (by id) and respect the documented poll interval and rate limits.

| Source | What | Access | Terms (check them) | Bridge |
| --- | --- | --- | --- | --- |
| [Wikimedia EventStreams](https://wikitech.wikimedia.org/wiki/Event_Platform/EventStreams) | Every edit on Wikipedia and the other Wikimedia wikis, a few dozen per second | Server-Sent Events, no key: `https://stream.wikimedia.org/v2/stream/recentchange` | Wikimedia's [Terms of Use](https://foundation.wikimedia.org/wiki/Policy:Terms_of_Use) and [User-Agent policy](https://meta.wikimedia.org/wiki/User-Agent_policy): identify your client | Built in: `datagen bridge wikimedia` (key: `wiki`, timestamp: `meta.dt`). Try: edits per wiki per minute, bot vs human edits, sessions of an editor |
| [USGS earthquake feeds](https://earthquake.usgs.gov/earthquakes/feed/v1.0/geojson.php) | Earthquakes worldwide, updated every minute | GeoJSON over HTTP, no key, e.g. `https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson` | US government data, generally public domain: see the USGS [policies](https://www.usgs.gov/information-policies-and-instructions) | Poll every minute; key: the feature `id`; timestamp: `properties.time` (epoch ms). An event can be updated (`properties.updated`): a table keyed by id keeps the latest version |
| [GitHub events](https://docs.github.com/en/rest/activity/events) | Public activity: pushes, pull requests, stars... | REST, `https://api.github.com/events`: 60 requests per hour without a token | [GitHub Terms of Service](https://docs.github.com/en/site-policy/github-terms/github-terms-of-service) and API rate limits | Poll with `ETag` and the `X-Poll-Interval` header; key: `repo.name`; timestamp: `created_at`; dedupe on `id`. To replay history instead, [GH Archive](https://www.gharchive.org/) has hourly files of the same events |
| [NYC TLC trip records](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) | Every yellow and green taxi and for-hire trip in New York, millions per month | Monthly Parquet files | NYC open data, see the page's terms | Batch, not live: convert a month to JSON Lines (e.g. with DuckDB: `COPY (SELECT * FROM 'yellow_tripdata_2025-01.parquet') TO 'trips.jsonl'`), then produce it in pickup-time order with the pickup time as timestamp. Try: trips per zone per hour, revenue per hour (hopping), airport trips |
| [Coinbase Exchange](https://docs.cdp.coinbase.com/exchange/docs/websocket-overview) / [Binance](https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams) market data | Live crypto trades and prices | WebSocket, no key for public market data, e.g. Coinbase's `ticker` / `matches` channels, Binance's `<symbol>@trade` streams | Each exchange's API terms; Binance isn't available in every country | Map each trade to the `stocks` schema (`symbol`, `price`, `volume`, `timestamp`), keyed by symbol: the [stock-ohlc](../14-RealWorld/README.md#stock-ohlc) scenario then works on live data |
| [OpenSky Network](https://openskynetwork.github.io/opensky-api/rest.html) | Live positions of aircraft (ADS-B) | REST, `/api/states/all`: anonymous access is heavily rate-limited, an account gets more | Research and non-commercial use, see OpenSky's terms | Poll every 10 seconds or more; key: `icao24`; timestamp: the state's `time_position`. Try: aircraft per area per minute, aircraft that stop reporting (like the iot offline alerts) |

For a quick file load without `datagen`, [kcat](https://github.com/edenhill/kcat) produces a JSON Lines file with
a key, but with the produce time as timestamp (use a timestamp extractor that reads the event time from the value):

```bash
jq -r '"\(.orderId)|\(tojson)"' ecommerce/orders.jsonl | kcat -P -b localhost:9092 -t ecommerce.orders -K '|'
```
