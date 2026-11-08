using NUnit.Framework;
using Streamiz.Kafka.Net.SerDes;
using static Streamiz.Examples.Basics.Tests.ExampleDriver;

namespace Streamiz.Examples.Basics.Tests;

public class EventTimeExtractorTests
{
    [Test]
    public void TheRecordTimestampIsTheTimeTheMeasureWasTaken()
    {
        using var driver = Create(new EventTimeExtractor());
        var readings = driver.CreateInputTopic(EventTimeExtractor.InputTopic, new StringSerDes(), new JsonSerDes<SensorReading>());
        var output = driver.CreateOutputTopic(EventTimeExtractor.OutputTopic, TimeSpan.FromSeconds(1),
            new StringSerDes(), new JsonSerDes<SensorReading>());

        // the measure was taken at T0 but reached Kafka one hour later (the sensor was offline)
        readings.PipeInput("s1", new SensorReading { Sensor = "s1", Value = 21.5, MeasuredAt = Ms(T0) }, T0.AddHours(1));

        var record = output.ReadKeyValue();
        Assert.AreEqual(Ms(T0), record.Message.Timestamp.UnixTimestampMs);
    }
}

public class AlterTimestampTests
{
    [Test]
    public void InvoicesAreTimestampedWithTheirDueDate()
    {
        using var driver = Create(new AlterTimestamp());
        var invoices = driver.CreateInputTopic(AlterTimestamp.InputTopic, new StringSerDes(), new JsonSerDes<Invoice>());
        var output = driver.CreateOutputTopic(AlterTimestamp.OutputTopic, TimeSpan.FromSeconds(1),
            new StringSerDes(), new JsonSerDes<Invoice>());

        var dueAt = T0.AddDays(30);
        invoices.PipeInput("i1", new Invoice { Customer = "alice", Amount = 99, DueAt = Ms(dueAt) }, T0);
        invoices.PipeInput("i2", new Invoice { Customer = "bob", Amount = 0, DueAt = Ms(dueAt) }, T0); // filtered

        var records = output.ReadKeyValueList().ToList();
        Assert.AreEqual(1, records.Count);
        Assert.AreEqual("i1", records[0].Message.Key);
        Assert.AreEqual(Ms(dueAt), records[0].Message.Timestamp.UnixTimestampMs);
    }
}

public class ExplicitRepartitionTests
{
    [Test]
    public void OrdersAreRekeyedByCustomerThroughANamedRepartitionTopic()
    {
        // The repartition topic is created with the 4 partitions declared by the example.
        var example = new ExplicitRepartition();
        using var driver = Create(example);
        var orders = driver.CreateInputTopic(ExplicitRepartition.InputTopic, new StringSerDes(), new JsonSerDes<Order>());
        var output = driver.CreateOutputTopic(ExplicitRepartition.OutputTopic, TimeSpan.FromSeconds(1),
            new StringSerDes(), new StringSerDes());

        orders.PipeInput("o1", new Order { Customer = "alice", Product = "book", Quantity = 2, UnitPrice = 10 });
        orders.PipeInput("o2", new Order { Customer = "bob", Product = "pen", Quantity = 1, UnitPrice = 2 });

        CollectionAssert.AreEqual(
            new[] { ("alice", "2 x book"), ("bob", "1 x pen") },
            output.ReadKeyValueList().Select(r => (r.Message.Key, r.Message.Value)).ToArray());
    }

    [Test]
    public void TheTopologyContainsTheNamedRepartitionTopic()
    {
        var builder = new Streamiz.Kafka.Net.StreamBuilder();
        new ExplicitRepartition().BuildTopology(builder);

        StringAssert.Contains($"{ExplicitRepartition.RepartitionName}-repartition", builder.Build().Describe().ToString());
    }
}

public class DropDuplicatesTests
{
    [Test]
    public void APaymentResentWithinFiveMinutesIsDropped()
    {
        using var driver = Create(new DropDuplicates());
        var requests = driver.CreateInputTopic(DropDuplicates.InputTopic, new StringSerDes(), new JsonSerDes<Payment>());
        var output = driver.CreateOutputTopic(DropDuplicates.OutputTopic, TimeSpan.FromSeconds(1),
            new StringSerDes(), new JsonSerDes<Payment>());

        requests.PipeInput("p1", new Payment { Method = "card", Amount = 10 }, T0);
        requests.PipeInput("p2", new Payment { Method = "paypal", Amount = 5 }, T0.AddMinutes(1));
        requests.PipeInput("p1", new Payment { Method = "card", Amount = 10 }, T0.AddMinutes(2));  // duplicate: dropped
        requests.PipeInput("p1", new Payment { Method = "card", Amount = 10 }, T0.AddMinutes(20)); // too late to be a duplicate
        requests.PipeInput("p1", new Payment { Method = "card", Amount = 15 }, T0.AddMinutes(21)); // different amount: kept

        CollectionAssert.AreEqual(
            new[] { ("p1", 10.0), ("p2", 5.0), ("p1", 10.0), ("p1", 15.0) },
            output.ReadKeyValueList().Select(r => (r.Message.Key, r.Message.Value.Amount)).ToArray());
    }
}
