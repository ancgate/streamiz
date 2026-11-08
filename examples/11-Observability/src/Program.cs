using Streamiz.Examples.Common;
using Streamiz.Examples.Observability.Examples;

var listeners = new StreamListenersExample();

// stream-listeners subscribes to the KafkaStream events, so it creates the stream itself
if (args.Length > 0 && args[0] == listeners.Name)
{
    await listeners.RunAsync();
    return 0;
}

return await ExampleRunner.RunAsync(new IExample[]
{
    new LoggingExample(),
    new PrometheusMetricsExample(),
    new OpenTelemetryMetricsExample(),
    new CustomMetricsReporterExample(),
    listeners
}, args);
