# 12 - Hosting

Running Streamiz inside a .NET host, with configuration, dependency injection, logging, graceful shutdown, health checks and an HTTP endpoint serving a state store.

| Project | What it is |
| --- | --- |
| `src/Shared` | The parts both hosts use: settings, the topology, the hosted service, the health tracker, the DI registration |
| `src/Worker` | A Generic Host worker service (`Host.CreateApplicationBuilder`) |
| `src/WebApi` | An ASP.NET Core minimal API (`WebApplication.CreateBuilder`) |
| `tests` | Tests of the topology, the health mapping and the DI registration, without Kafka |

```bash
cd examples/12-Hosting/src/Worker
dotnet run
```

```bash
cd examples/12-Hosting/src/WebApi
dotnet run
curl http://localhost:5080/health
curl http://localhost:5080/totals/alice
```

Docs: [interactive queries](../../docs/interactive-queries.md), [stream configuration](../../docs/stream-configuration.md).

## The topology

`OrderTotalsTopology` sums the amount spent by each customer. Orders are `customer` → `sku:quantity` on `hosted-orders`; prices come from `IPriceCatalog`, a service injected by DI (`ConfigurationPriceCatalog` reads the `Prices` section). The totals go to `order-totals` and to the `order-totals-store` store.

| Input `hosted-orders` | Output `order-totals` (customer → double) |
| --- | --- |
| `alice` → `coffee:2` | `alice` → `8.4` |
| `bob` → `apple:4` | `bob` → `2` |

Because the topology is a class with injected dependencies, the test builds it with a fake catalog and runs it in a `TopologyTestDriver`.

## Configuration

`StreamizSettings` is bound from the `Streamiz` section, so any configuration source works:

```json
"Streamiz": {
  "ApplicationId": "streamiz-hosting-worker",
  "BootstrapServers": "localhost:9092",
  "NumStreamThreads": 2,
  "Guarantee": "AT_LEAST_ONCE"
}
```

```bash
Streamiz__BootstrapServers=broker:9092 dotnet run
```

## Lifecycle

`StreamizHostedService` (a `BackgroundService`) starts the `KafkaStream` with the host, gives it the host's `ILoggerFactory`, and disposes it when the host stops (Ctrl+C, SIGTERM), which closes the stream cleanly and commits the offsets. It's registered once and exposed both as itself and as an `IHostedService`, so endpoints can reach the running stream.

## Health

`StreamHealth` follows `KafkaStream.StateChanged`. `/health` returns 200 while the stream is `RUNNING` or `REBALANCING`, and 503 otherwise (`CREATED`, `PENDING_SHUTDOWN`, `NOT_RUNNING`, `ERROR`). Use it as the readiness probe. Without Kafka, it returns `{"state":"ERROR","healthy":false,...}` with 503.

## Interactive queries

`/totals/{customer}` reads the local `order-totals-store` with `KafkaStream.Store(...)`. It returns 503 while the stream isn't running. With several instances, a key can be on another instance: set `ApplicationServer` (done in `WebApi/appsettings.json`) and use `KafkaStream.QueryMetadataForKey` to find which one, then forward the request.
