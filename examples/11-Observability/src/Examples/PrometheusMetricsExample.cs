using Streamiz.Examples.Common;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.Metrics;
using Streamiz.Kafka.Net.Metrics.Prometheus;

namespace Streamiz.Examples.Observability.Examples;

/// <summary>
/// Prometheus: the Streamiz.Kafka.Net.Metrics.Prometheus package exposes the metrics on an HTTP endpoint
/// (http://localhost:9090/metrics here) that Prometheus scrapes.
/// </summary>
public class PrometheusMetricsExample : IExample
{
    public string Name => "prometheus-metrics";

    public string Description => "Counts words and exposes Streamiz metrics on http://localhost:9090/metrics for Prometheus.";

    public IEnumerable<string> InputTopics => new[] { "sentences" };

    public void BuildTopology(StreamBuilder builder) => WordCountTopology.Build(builder);

    public void Configure(StreamConfig config)
    {
        // INFO: thread, task and processor metrics (incl. record-e2e-latency); DEBUG adds store and cache metrics
        config.MetricsRecording = MetricsRecordingLevel.INFO;
        // refresh the exposed values every 30 s (the minimum), on port 9090,
        // and also expose librdkafka's own statistics (consumer lag, broker latency...)
        config.UsePrometheusReporter(TimeSpan.FromSeconds(30), 9090, exposeLibrdkafkaStatistics: true);
    }
}
