using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using Streamiz.Examples.Common;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.Metrics;
using Streamiz.Kafka.Net.Metrics.OpenTelemetry;

namespace Streamiz.Examples.Observability.Examples;

/// <summary>
/// OpenTelemetry: the Streamiz.Kafka.Net.Metrics.OpenTelemetry package exports the metrics (meter "Streamiz")
/// with OTLP/gRPC to a collector (environment/confs/otel-collector-config.yaml configures one).
/// The exporter reads the standard OTEL_EXPORTER_OTLP_ENDPOINT variable (default http://localhost:4317).
/// </summary>
public class OpenTelemetryMetricsExample : IExample
{
    public string Name => "opentelemetry-metrics";

    public string Description => "Counts words and exports Streamiz metrics with OTLP to an OpenTelemetry collector.";

    public IEnumerable<string> InputTopics => new[] { "sentences" };

    public void BuildTopology(StreamBuilder builder) => WordCountTopology.Build(builder);

    public void Configure(StreamConfig config)
    {
        config.MetricsRecording = MetricsRecordingLevel.DEBUG;
        // export every 30 s; the callback customizes the meter provider (here: the service name shown in your backend)
        config.UseOpenTelemetryReporter(TimeSpan.FromSeconds(30), meterProvider =>
            meterProvider.SetResourceBuilder(ResourceBuilder.CreateDefault().AddService("streamiz-word-count")));
    }
}
