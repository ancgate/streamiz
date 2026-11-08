# 11 - Observability

Logs, metrics and lifecycle events of a Streamiz application.

```bash
cd examples/11-Observability/src
dotnet run -- <example>
```

Docs: [monitoring](../../docs/monitoring.md), [stream configuration](../../docs/stream-configuration.md).

## logging

Streamiz logs through Microsoft.Extensions.Logging: set `config.Logger` to your `ILoggerFactory` (console, Serilog, Application Insights...), and use the same factory for your own logs in the topology. This example logs suspicious logins from a `Peek`.

`logins`: `bob` → `BR` → `suspicious-logins`: `bob` → `BR`, and the warning `Login of bob from BR (partition 0, offset 1)`.

Streamiz uses `config.Logger` while the stream runs: dispose the factory after disposing the `KafkaStream`.

## prometheus-metrics

Package `Streamiz.Kafka.Net.Metrics.Prometheus`: `config.UsePrometheusReporter(TimeSpan.FromSeconds(30), 9090)` exposes the metrics on `http://localhost:9090/metrics` for Prometheus to scrape (`environment/confs/prometheus.yaml` scrapes it). `exposeLibrdkafkaStatistics: true` adds librdkafka's statistics (consumer lag, broker latency...).

Recording levels: `INFO` for thread, task and processor metrics, including `record-e2e-latency-avg/min/max` (KIP-613: the time between a record's timestamp and its processing, on source and terminal nodes). `DEBUG` adds state store and cache metrics.

## opentelemetry-metrics

Package `Streamiz.Kafka.Net.Metrics.OpenTelemetry`: `config.UseOpenTelemetryReporter(...)` exports the meter `Streamiz` with OTLP/gRPC to a collector. The endpoint comes from `OTEL_EXPORTER_OTLP_ENDPOINT` (default `http://localhost:4317`; `environment/confs/otel-collector-config.yaml` configures a collector). The callback customizes the meter provider, here with a service name.

## custom-metrics-reporter

`config.MetricsReporter` receives the sensors of each stream thread every `MetricsIntervalMs` (30 s minimum). This reporter prints the end-to-end latency and the processing rate:

```
process-rate             task 0-0                         1234.57
record-e2e-latency-avg   KSTREAM-SOURCE-0000000000        42.5
record-e2e-latency-max   KSTREAM-SOURCE-0000000000        120
```

The test checks the formatting on hand-built rows: `Sensor` objects can't be created outside Streamiz, and the `TopologyTestDriver` doesn't export metrics.

## stream-listeners

`KafkaStream.StateChanged` (`CREATED` → `REBALANCING` → `RUNNING` → `PENDING_SHUTDOWN` → `NOT_RUNNING`, or `ERROR`) and the restoration events `OnRestoreStartEvent`, `OnRestoreBatchEvent` and `OnRestoreEndEvent`, fired while a state store is rebuilt from its changelog. Run the example twice: the second run restores `user-profiles-store`.

```
state CREATED -> REBALANCING partitions are being (re)assigned, stores may be restoring
restoring user-profiles-store from streamiz-example-stream-listeners-user-profiles-store-changelog [[0]]: offsets 0 to 3
restored user-profiles-store from ...: 3 records
state REBALANCING -> RUNNING processing records
```

The current state is only available through `StateChanged`: keep the last value yourself (see `12-Hosting`'s `StreamHealth`).
