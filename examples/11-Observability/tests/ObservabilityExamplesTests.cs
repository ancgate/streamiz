using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using NUnit.Framework;
using Streamiz.Examples.Common;
using Streamiz.Examples.Observability.Examples;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.Metrics;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;

namespace Streamiz.Examples.Observability.Tests;

internal static class TestDrivers
{
    public static TopologyTestDriver For(IExample example)
    {
        var builder = new StreamBuilder();
        example.BuildTopology(builder);
        return new TopologyTestDriver(builder.Build(), ExampleRunner.CreateConfig(example));
    }
}

/// <summary>Collects the log messages, to assert on them.</summary>
internal class CapturingLoggerProvider : ILoggerProvider
{
    public ConcurrentQueue<(string Category, LogLevel Level, string Message)> Messages { get; } = new();

    public ILogger CreateLogger(string categoryName) => new CapturingLogger(categoryName, Messages);

    public void Dispose()
    {
    }

    private class CapturingLogger : ILogger
    {
        private readonly string category;
        private readonly ConcurrentQueue<(string, LogLevel, string)> messages;

        public CapturingLogger(string category, ConcurrentQueue<(string, LogLevel, string)> messages)
        {
            this.category = category;
            this.messages = messages;
        }

        public IDisposable BeginScope<TState>(TState state) => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception exception,
            Func<TState, Exception, string> formatter)
            => messages.Enqueue((category, logLevel, formatter(state, exception)));
    }
}

public class LoggingExampleTests
{
    [Test]
    public void SuspiciousLoginsAreLoggedAndForwarded()
    {
        var provider = new CapturingLoggerProvider();
        // disposed after the driver (declared first, disposed last)
        using var loggerFactory = LoggerFactory.Create(builder => builder.AddProvider(provider));

        using var driver = TestDrivers.For(new LoggingExample(loggerFactory));
        var logins = driver.CreateInputTopic<string, string>("logins");
        var suspicious = driver.CreateOutputTopic<string, string>("suspicious-logins");

        logins.PipeInput("alice", "FR");
        logins.PipeInput("bob", "BR");

        CollectionAssert.AreEqual(new[] { "BR" }, suspicious.ReadValueList().ToList());
        var warnings = provider.Messages.Where(m => m.Category == "Examples.Logins").ToList();
        Assert.AreEqual(1, warnings.Count);
        Assert.AreEqual(LogLevel.Warning, warnings[0].Level);
        Assert.AreEqual("Login of bob from BR (partition 0, offset 1)", warnings[0].Message);
    }
}

public class MetricsExamplesTests
{
    [Test]
    public void TheMeasuredTopologyCountsWords()
    {
        using var driver = TestDrivers.For(new PrometheusMetricsExample());
        var sentences = driver.CreateInputTopic<string, string>("sentences");
        var counts = driver.CreateOutputTopic<string, long, StringSerDes, Int64SerDes>("word-counts");

        sentences.PipeInput("s-1", "Kafka streams");
        sentences.PipeInput("s-2", "streams in dotnet");

        var latest = counts.ReadKeyValueList()
            .GroupBy(r => r.Message.Key)
            .ToDictionary(g => g.Key, g => g.Last().Message.Value);
        Assert.AreEqual(2L, latest["streams"]);
        Assert.AreEqual(1L, latest["kafka"]);
    }

    [Test]
    public void PrometheusReporterIsConfigured()
    {
        var config = ExampleRunner.CreateConfig(new PrometheusMetricsExample());

        Assert.AreEqual(MetricsRecordingLevel.INFO, config.MetricsRecording);
        Assert.AreEqual(30_000, config.MetricsIntervalMs);
        Assert.IsTrue(config.ExposeLibrdKafkaStats);
    }

    [Test]
    public void OpenTelemetryReporterIsConfigured()
    {
        var config = ExampleRunner.CreateConfig(new OpenTelemetryMetricsExample());

        Assert.AreEqual(MetricsRecordingLevel.DEBUG, config.MetricsRecording);
        Assert.AreEqual(30_000, config.MetricsIntervalMs);
    }

    [Test]
    public void CustomReporterKeepsLatencyAndRate()
    {
        var node = new Dictionary<string, string> { ["processor_node_id"] = "KSTREAM-SOURCE-0000000000" };
        var task = new Dictionary<string, string> { ["task_id"] = "0-0" };
        var rows = new[]
        {
            new CustomMetricsReporterExample.MetricRow("record-e2e-latency-max", node, 120.0),
            new CustomMetricsReporterExample.MetricRow("record-e2e-latency-avg", node, 42.5),
            new CustomMetricsReporterExample.MetricRow("process-rate", task, 1234.567),
            new CustomMetricsReporterExample.MetricRow("commit-total", task, 10.0) // not reported
        };

        var lines = CustomMetricsReporterExample.Format(rows).ToList();

        CollectionAssert.AreEqual(new[]
        {
            "process-rate             task 0-0                         1234.57",
            "record-e2e-latency-avg   KSTREAM-SOURCE-0000000000        42.5",
            "record-e2e-latency-max   KSTREAM-SOURCE-0000000000        120"
        }, lines);
    }
}

public class StreamListenersExampleTests
{
    [Test]
    public void StateChangesAreDescribed()
    {
        Assert.AreEqual("state REBALANCING -> RUNNING processing records",
            StreamListenersExample.DescribeStateChange(KafkaStream.State.REBALANCING, KafkaStream.State.RUNNING));
        Assert.AreEqual("state RUNNING -> ERROR a stream thread died: check the logs",
            StreamListenersExample.DescribeStateChange(KafkaStream.State.RUNNING, KafkaStream.State.ERROR));
    }

    [Test]
    public void ListenersSeeTheShutdownOfAStreamThatNeverStarted()
    {
        // no Kafka is needed to create and dispose a KafkaStream that is never started
        var example = new StreamListenersExample();
        var builder = new StreamBuilder();
        example.BuildTopology(builder);
        var config = ExampleRunner.CreateConfig(example);
        config.BootstrapServers = "localhost:1";
        config.StateDir = Path.Combine(Path.GetTempPath(), "streamiz-examples-" + Guid.NewGuid());

        var events = new ConcurrentQueue<string>();
        var stream = new KafkaStream(builder.Build(), config);
        StreamListenersExample.Attach(stream, events.Enqueue);
        stream.Dispose();

        CollectionAssert.Contains(events.ToList(), "state CREATED -> PENDING_SHUTDOWN closing");
        CollectionAssert.Contains(events.ToList(), "state PENDING_SHUTDOWN -> NOT_RUNNING stopped");
    }
}
