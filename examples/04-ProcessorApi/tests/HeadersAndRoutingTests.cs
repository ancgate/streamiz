using System.Text;
using Confluent.Kafka;
using NUnit.Framework;
using Streamiz.Examples.ProcessorApi.Examples;
using static Streamiz.Examples.ProcessorApi.Tests.ExampleDriver;

namespace Streamiz.Examples.ProcessorApi.Tests;

public class TraceHeadersTests
{
    private static string Header(Headers headers, string key)
        => Encoding.UTF8.GetString(headers.GetLastBytes(key));

    [Test]
    public void AnExistingTraceIdIsKeptAndProcessedByIsAdded()
    {
        using var driver = Create(new TraceHeadersExample());
        var orders = driver.CreateInputTopic<string, string>(TraceHeadersExample.OrdersTopic);
        var output = driver.CreateOutputTopic<string, string>(TraceHeadersExample.OutputTopic);

        orders.PipeInput("o1", "120.50", new Headers { { "trace-id", Encoding.UTF8.GetBytes("abc-123") } });

        var record = output.ReadKeyValue();
        Assert.AreEqual("120.50", record.Message.Value);
        Assert.AreEqual("abc-123", Header(record.Message.Headers, TraceHeadersExample.TraceIdHeader));
        Assert.AreEqual("order-enricher", Header(record.Message.Headers, TraceHeadersExample.ProcessedByHeader));
    }

    [Test]
    public void ARecordWithoutTraceIdGetsOne()
    {
        using var driver = Create(new TraceHeadersExample());
        var orders = driver.CreateInputTopic<string, string>(TraceHeadersExample.OrdersTopic);
        var output = driver.CreateOutputTopic<string, string>(TraceHeadersExample.OutputTopic);

        orders.PipeInput("o1", "120.50");   // offset 0 of partition 0

        var record = output.ReadKeyValue();
        Assert.AreEqual("orders-0-0", Header(record.Message.Headers, TraceHeadersExample.TraceIdHeader));
        Assert.AreEqual("order-enricher", Header(record.Message.Headers, TraceHeadersExample.ProcessedByHeader));
    }
}

public class RouteToNamedSinksTests
{
    [Test]
    public void OrdersAreRoutedByAmount()
    {
        using var driver = Create(new RouteToNamedSinksExample());
        var orders = driver.CreateInputTopic<string, string>(RouteToNamedSinksExample.OrdersTopic);
        var highValue = driver.CreateOutputTopic<string, string>(RouteToNamedSinksExample.HighValueTopic);
        var standard = driver.CreateOutputTopic<string, string>(RouteToNamedSinksExample.StandardTopic);

        orders.PipeInput("o1", "1500.00");
        orders.PipeInput("o2", "20.00");
        orders.PipeInput("o3", "999.99");

        CollectionAssert.AreEqual(new[] { ("o1", "1500.00") }, highValue.ReadAll());
        CollectionAssert.AreEqual(new[] { ("o2", "20.00"), ("o3", "999.99") }, standard.ReadAll());
    }
}
