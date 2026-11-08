using Confluent.Kafka;
using NUnit.Framework;
using Streamiz.Examples.ErrorHandling.Examples;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.Mock;

namespace Streamiz.Examples.ErrorHandling.Tests;

public class ErrorHandlerTests
{
    [Test]
    public void BadJsonIsSkipped()
    {
        var example = new SkipBadJsonExample();
        using var driver = Drivers.For(example);
        var payments = driver.CreateInputTopic<string, string>("payments");
        var amounts = driver.CreateOutputTopic<string, string>("payment-amounts");

        payments.PipeInput("p1", "{\"Id\":\"p1\",\"Amount\":10.5,\"Currency\":\"EUR\"}");
        payments.PipeInput("p2", "{not json");
        payments.PipeInput("p3", "{\"Id\":\"p3\",\"Amount\":3,\"Currency\":\"USD\"}");

        CollectionAssert.AreEqual(new[] { "10.5 EUR", "3 USD" }, amounts.ReadValueList());
        // the record at offset 1 was skipped: the handler is called once per bad record
        Assert.AreEqual(1, example.Skipped.Count);
        StringAssert.StartsWith("payments[0]@1", example.Skipped[0]);
    }

    [Test]
    public void ARecordYourCodeCantProcessIsSkipped()
    {
        var example = new ProcessingErrorsExample();
        using var driver = Drivers.For(example);
        var orders = driver.CreateInputTopic<string, string>("orders");
        var parsed = driver.CreateOutputTopic<string, string>("parsed-orders");

        orders.PipeInput("o1", "keyboard;2");
        orders.PipeInput("o2", "mouse;two");
        orders.PipeInput("o3", "screen;1");

        CollectionAssert.AreEqual(new[] { "2 x keyboard", "1 x screen" }, parsed.ReadValueList());

        // the context of the skipped record
        var skipped = example.Skipped.Single();
        Assert.AreEqual("orders", skipped.Topic);
        Assert.AreEqual(1, skipped.Offset);
        Assert.AreEqual("parse-order", skipped.ProcessorNodeId);
        Assert.AreEqual("mouse;two", System.Text.Encoding.UTF8.GetString(skipped.SourceRawValue));
    }

    [Test]
    public void TheSkippedRecordsGoToTheDeadLetterQueue()
    {
        using var driver = Drivers.For(new DeadLetterQueueExample());
        var orders = driver.CreateInputTopic<string, string>("orders");
        var payments = driver.CreateInputTopic<string, string>("payments");
        var dlq = driver.CreateOutputTopic<string, string>(DeadLetterQueueExample.DeadLetterQueueTopic);

        orders.PipeInput("o1", "keyboard;2");
        orders.PipeInput("o2", "mouse;two");
        payments.PipeInput("p1", "{not json");

        var records = dlq.ReadKeyValueList().ToList();
        Assert.AreEqual(2, records.Count);

        // the processing error: the original record, with the error in its headers
        var processingError = records[0];
        Assert.AreEqual("o2", processingError.Message.Key);
        Assert.AreEqual("mouse;two", processingError.Message.Value);
        Assert.AreEqual(typeof(FormatException).FullName, Drivers.Header(processingError.Message.Headers, "__streams.errors.exception"));
        Assert.AreEqual("orders", Drivers.Header(processingError.Message.Headers, "__streams.errors.topic"));
        Assert.AreEqual("0", Drivers.Header(processingError.Message.Headers, "__streams.errors.partition"));
        Assert.AreEqual("1", Drivers.Header(processingError.Message.Headers, "__streams.errors.offset"));
        Assert.IsNotEmpty(Drivers.Header(processingError.Message.Headers, "__streams.errors.message"));
        Assert.IsNotEmpty(Drivers.Header(processingError.Message.Headers, "__streams.errors.stacktrace"));

        // the deserialization error: the bad JSON as it was consumed
        var deserializationError = records[1];
        Assert.AreEqual("{not json", deserializationError.Message.Value);
        Assert.AreEqual("payments", Drivers.Header(deserializationError.Message.Headers, "__streams.errors.topic"));
    }

    [TestCase(ErrorCode.MsgSizeTooLarge, ProductionExceptionHandlerResponse.CONTINUE)]
    [TestCase(ErrorCode.Local_MsgTimedOut, ProductionExceptionHandlerResponse.RETRY)]
    [TestCase(ErrorCode.NotEnoughReplicas, ProductionExceptionHandlerResponse.RETRY)]
    [TestCase(ErrorCode.TopicAuthorizationFailed, ProductionExceptionHandlerResponse.FAIL)]
    public void ProductionErrorsAreRetriedDroppedOrFatal(ErrorCode error, ProductionExceptionHandlerResponse expected)
    {
        // production errors can't be triggered with the test driver: the handler is a plain function, test it directly
        var report = new DeliveryReport<byte[], byte[]>
        {
            Error = new Error(error),
            Topic = "orders-uppercase",
            Status = PersistenceStatus.NotPersisted
        };

        Assert.AreEqual(expected, ProductionErrorsExample.Handle(report));
    }

    [Test]
    public void TheStreamThreadSkipsTheRecordWhichFailedAndContinues()
    {
        var example = new InnerExceptionContinueExample();
        // the in-memory cluster mode runs a real stream thread: the handler and the rejoin happen as in production
        using var driver = Drivers.For(example, TopologyTestDriver.Mode.ASYNC_CLUSTER_IN_MEMORY);
        var orders = driver.CreateInputTopic<string, string>("orders");
        var checkedOrders = driver.CreateOutputTopic<string, string>("orders-checked");

        orders.PipeInput("o1", "keyboard");
        orders.PipeInput("o2", "poison");
        orders.PipeInput("o3", "screen");

        var values = new List<string>();
        var end = DateTime.Now.AddSeconds(10);
        while (!values.Contains("screen") && DateTime.Now < end)
        {
            values.AddRange(checkedOrders.ReadValueList());
            Thread.Sleep(50);
        }

        // "keyboard" can be emitted twice: the thread rejoins from the last committed offset (at-least-once)
        CollectionAssert.Contains(values, "keyboard");
        CollectionAssert.Contains(values, "screen");
        CollectionAssert.DoesNotContain(values, "poison");
        Assert.AreEqual(1, example.HandledExceptions);
    }

    [Test]
    public void AnExternalCallWhichKeepsFailingGoesToTheDeadLetterQueue()
    {
        using var driver = Drivers.For(new AsyncRetryDeadLetterQueueExample());
        var payments = driver.CreateInputTopic<string, string>("payments");
        var checkedPayments = driver.CreateOutputTopic<string, string>("payments-checked");
        var dlq = driver.CreateOutputTopic<string, string>(AsyncRetryDeadLetterQueueExample.DeadLetterQueueTopic);

        payments.PipeInput("p1", "payment-1");
        payments.PipeInput("p2", "payment-2-unreachable");
        payments.PipeInput("p3", "payment-3");

        CollectionAssert.AreEquivalent(new[] { "payment-1:ok", "payment-3:ok" }, checkedPayments.ReadValueList());

        var dead = dlq.ReadKeyValueList().Single();
        Assert.AreEqual("p2", dead.Message.Key);
        Assert.AreEqual("payment-2-unreachable", dead.Message.Value);
        // the retries were exhausted
        StringAssert.Contains("NoneRetryableException", Drivers.Header(dead.Message.Headers, "__streams.errors.exception"));
    }
}
