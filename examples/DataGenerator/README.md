# DataGenerator (`datagen`)

A console tool for the [sample datasets](../data/README.md): it loads them into Kafka, produces new events
continuously, creates the topics, bridges a public live stream, and regenerates the files.

Kafka is at `KAFKA_BOOTSTRAP` (default `localhost:9092`), like the examples. Run it from this folder:

```bash
cd examples/DataGenerator
dotnet run                     # usage and the list of datasets
```

Datasets: `ecommerce`, `clickstream`, `iot`, `banking`, `stocks`, `bad`.

## create-topics

```bash
dotnet run -- create-topics                              # the input topics of every dataset, 3 partitions
dotnet run -- create-topics --partitions 6 --with-outputs  # and the output topics of the 14-RealWorld scenarios
```

Topics that already exist are left as they are. The replication factor is 1 (a local cluster).

## load

```bash
dotnet run -- load ecommerce                    # as fast as possible (the default)
dotnet run -- load iot --speedup 60             # replays the original time gaps 60 times faster
dotnet run -- load clickstream --as-fast-as-possible
```

Each record is produced with its key and the event time as its timestamp (see
[how the files become topics](../data/README.md#how-the-files-become-topics)). The files of a dataset are produced
in arrival order: each file in its own order, the topics interleaved by time. With `--speedup`, the tool waits so
that the time between two records is the original one divided by N: `--speedup 1` replays in real time.

Loading the same dataset twice produces the records twice: delete and recreate the topics (or use a new
application id) to start over.

## live

```bash
dotnet run -- live iot --rate 50
dotnet run -- live banking --rate 20 --late 0.02 --out-of-order 0.05 --bad 0.01
dotnet run -- live clickstream --rate 100 --duration 300
```

Produces new random events with the current time as event time, in the same schemas and topics as the files, until
Ctrl+C (or `--duration` seconds):

| Option | |
| --- | --- |
| `--rate N` | events per second (default 10, approximately) |
| `--late P` | ratio of late events: 5 to 15 minutes old |
| `--out-of-order P` | ratio of out-of-order events: 30 to 90 seconds old |
| `--bad P` | ratio of malformed values (truncated JSON) |

| Dataset | Events |
| --- | --- |
| `iot` | readings of D01-D50 |
| `stocks` | ticks of the 10 symbols, prices following a random walk |
| `banking` | transactions of A0001-A0300 |
| `clickstream` | page views of U0001-U1000 |
| `ecommerce` | orders; 90% of them get a payment, produced right away with an event time 1 to 10 minutes later |

Use it to watch a running example react, for instance with `--late` and `--bad` to see grace periods and the dead
letter queue at work. Unlike the files, live data isn't deterministic.

## bridge wikimedia

```bash
dotnet run -- bridge wikimedia                           # to the topic wikimedia.recentchange
dotnet run -- bridge wikimedia --topic edits --max 1000  # stop after 1000 events
```

Real data: every edit on Wikipedia and the other Wikimedia wikis, a few dozen per second, read from Wikimedia's
public [EventStreams](https://wikitech.wikimedia.org/wiki/Event_Platform/EventStreams) service (Server-Sent Events,
no account needed). Each event is produced as is (JSON), keyed by wiki (`enwiki`, `frwiki`, `commonswiki`...), with
its event time (`meta.dt`) as timestamp. It needs internet access, and Wikimedia's terms of use apply. See
[Public real-world data sources](../data/README.md#public-real-world-data-sources) for other live sources and how to
bridge them.

## generate-files

```bash
dotnet run -- generate-files
```

Regenerates `examples/data` (or the folder given with `--data <dir>`). The generator uses fixed random seeds, so the
files are the same every time, byte for byte: the tests and the documented facts depend on it. To change a dataset,
change its generator in [`Datasets/`](Datasets), regenerate, and update the facts in the
[data README](../data/README.md) and the [14-RealWorld](../14-RealWorld/README.md) tests.

| File | |
| --- | --- |
| [`Catalog.cs`](Catalog.cs) | The datasets: files, topics, keys, timestamps |
| [`Datasets/*.cs`](Datasets) | One generator per dataset, writing its files and `facts.json` |
| [`Loader.cs`](Loader.cs) | Reads a dataset in arrival order and produces it (also used by the 14-RealWorld tests) |
| [`Live.cs`](Live.cs) | The live mode |
| [`WikimediaBridge.cs`](WikimediaBridge.cs) | The Wikimedia bridge |
