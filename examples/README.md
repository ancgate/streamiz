# Streamiz examples

Runnable examples covering the features of Streamiz. Each group is a small console application: run it without arguments to list its examples, or with an example name to run it against Kafka. Each group also has a test project that runs every example's topology with the `TopologyTestDriver`, so you can read the expected input and output, and the examples stay correct as the library evolves.

## Prerequisites

- The .NET 8 SDK.
- A Kafka cluster to run an example for real. `environment/docker-compose.yml` at the root of the repository starts one on `localhost:9092`:

  ```bash
  cd environment && docker compose up -d
  ```

- Set `KAFKA_BOOTSTRAP` if your cluster is somewhere else (default `localhost:9092`).

The tests don't need Kafka.

## Running

```bash
cd examples/01-Basics/src
dotnet run                 # lists the examples of the group
dotnet run -- word-count   # runs one of them until Ctrl+C
```

```bash
cd examples
dotnet test Streamiz.Examples.sln   # runs the tests of every group
```

Each example prints the input topics to create and its topology when it starts.

## Groups

| Folder | What it covers |
| --- | --- |
| [01-Basics](01-Basics/README.md) | Stateless operations, word count, aggregations, tables, timestamps, dynamic routing |
| [02-Windowing](02-Windowing/README.md) | Tumbling, hopping, sliding and session windows, suppress |
| [03-Joins](03-Joins/README.md) | Stream-stream, stream-table, stream-global table, table-table, foreign-key joins, versioned tables and grace periods |
| [04-ProcessorApi](04-ProcessorApi/README.md) | Processors and transformers, punctuation, custom and global state stores |
| [05-StateAndQueries](05-StateAndQueries/README.md) | In-memory and RocksDB stores, caching, interactive queries across instances, standby replicas |
| [06-RemoteStorage](06-RemoteStorage/README.md) | Azure Table, Azure Cosmos DB, AWS DynamoDB, GCP Bigtable and MongoDB stores |
| [07-ErrorHandling](07-ErrorHandling/README.md) | Deserialization, processing, production and inner exception handlers, dead letter queue |
| [08-ProcessingGuarantees](08-ProcessingGuarantees/README.md) | At-least-once, exactly-once and at-most-once |
| [09-Serdes](09-Serdes/README.md) | Built-in serdes, JSON, Avro and Protobuf with Schema Registry, CloudEvents, custom serdes |
| [10-AsyncProcessing](10-AsyncProcessing/README.md) | Calling external services with `MapAsync` / `ForeachAsync`, retries, V1 and V2 engines |
| [11-Observability](11-Observability/README.md) | Logging, metrics with Prometheus and OpenTelemetry, end-to-end latency, state listeners |
| [12-Hosting](12-Hosting/README.md) | Running Streamiz in a .NET Generic Host / worker service and in ASP.NET Core |
| [13-Testing](13-Testing/README.md) | Unit testing topologies with the `TopologyTestDriver` |
| [14-RealWorld](14-RealWorld/README.md) | End-to-end scenarios on the sample data: e-commerce revenue, order-payment matching, fraud detection, IoT monitoring, clickstream analytics, stock candles |

## Sample data

[`data/`](data/README.md) has realistic datasets to try the examples with: an online shop (customers, products
with price changes, orders, payments, shipments), a website's clickstream with bots, temperature sensors with late
and out-of-order readings, card transactions with frauds, a day of stock trades, and malformed records. They're
deterministic, and the patterns planted in them (and the results expected) are documented. The data README also
lists public live sources (Wikimedia, USGS, GitHub, NYC taxis, crypto exchanges, OpenSky) and how to bridge them
into Kafka.

[`DataGenerator/`](DataGenerator/README.md) (`datagen`) loads a dataset into Kafka with its event times, produces
live events (with late, out-of-order and malformed ones on demand), and bridges Wikimedia's live edits:

```bash
cd examples/DataGenerator
dotnet run -- create-topics
dotnet run -- load ecommerce
dotnet run -- live iot --rate 50 --late 0.02
```

## Layout

```
examples/
  Directory.Build.props          shared settings (net8.0)
  Streamiz.Examples.Common/      IExample + ExampleRunner used by every group
  data/                          sample datasets (JSON Lines) and their documented facts
  DataGenerator/                 datagen: load, live, create-topics, bridge, generate-files
  NN-Group/
    README.md                    what each example shows, with sample input and output
    src/                         console application (one file per example)
    tests/                       TopologyTestDriver tests of every example
```

`12-Hosting` is laid out differently: `src/Worker` (a Generic Host worker service) and `src/WebApi` (an ASP.NET Core app) share the topology and hosting code in `src/Shared`, and run with `dotnet run` in their own folder.
