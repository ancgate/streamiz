using System.Globalization;
using Streamiz.Examples.Common;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.Metrics;

namespace Streamiz.Examples.Observability.Examples;

/// <summary>
/// A custom metrics reporter: <c>config.MetricsReporter</c> receives the sensors of each stream thread every
/// <c>MetricsIntervalMs</c>. Here it prints the end-to-end latency (KIP-613) and the processing rate.
/// </summary>
public class CustomMetricsReporterExample : IExample
{
    /// <summary>One metric value, flattened from a sensor.</summary>
    public record MetricRow(string Name, IReadOnlyDictionary<string, string> Tags, object Value);

    public string Name => "custom-metrics-reporter";

    public string Description => "Prints the end-to-end latency and processing rate of each node every 30 seconds.";

    public IEnumerable<string> InputTopics => new[] { "sentences" };

    public void BuildTopology(StreamBuilder builder) => WordCountTopology.Build(builder);

    public void Configure(StreamConfig config)
    {
        config.MetricsRecording = MetricsRecordingLevel.INFO;
        config.MetricsIntervalMs = 30_000; // 30 s is the minimum
        config.MetricsReporter = sensors =>
        {
            foreach (var line in Format(ToRows(sensors)))
                Console.WriteLine(line);
        };
    }

    public static IEnumerable<MetricRow> ToRows(IEnumerable<Sensor> sensors)
        => sensors.SelectMany(sensor => sensor.Metrics.Values)
            .Select(metric => new MetricRow(metric.Name, metric.Tags, metric.Value));

    /// <summary>
    /// Keeps the metrics this dashboard cares about and formats one line per metric:
    /// record-e2e-latency-avg/min/max (ms between the record timestamp and its processing, recorded on source and
    /// terminal nodes) and process-rate (records per second).
    /// </summary>
    public static IEnumerable<string> Format(IEnumerable<MetricRow> rows)
        => rows.Where(row => row.Name.StartsWith("record-e2e-latency") || row.Name == "process-rate")
            .OrderBy(row => row.Name)
            .Select(row =>
            {
                var where = row.Tags.TryGetValue("processor_node_id", out var node) ? node
                    : row.Tags.TryGetValue("task_id", out var task) ? $"task {task}"
                    : "thread";
                return string.Format(CultureInfo.InvariantCulture, "{0,-24} {1,-32} {2:0.##}",
                    row.Name, where, Convert.ToDouble(row.Value, CultureInfo.InvariantCulture));
            });
}
