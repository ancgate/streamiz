using NUnit.Framework;
using Streamiz.Kafka.Net.SerDes;

namespace Streamiz.Examples.Basics.Tests;

public class FilterAndTransformTests
{
    [Test]
    public void EmptyAndTestOrdersAreDroppedAndTotalsAreKeyedByCustomer()
    {
        using var driver = ExampleDriver.Create(new FilterAndTransform());
        var orders = driver.CreateInputTopic(FilterAndTransform.InputTopic, new StringSerDes(), new JsonSerDes<Order>());
        var totals = driver.CreateOutputTopic(FilterAndTransform.OutputTopic, TimeSpan.FromSeconds(1),
            new StringSerDes(), new DoubleSerDes());

        orders.PipeInput("o1", new Order { Customer = "alice", Product = "book", Quantity = 2, UnitPrice = 10 });
        orders.PipeInput("o2", new Order { Customer = "bob", Product = "pen", Quantity = 0, UnitPrice = 1 }); // empty: filtered
        orders.PipeInput("o3", new Order { Customer = "test", Product = "x", Quantity = 1, UnitPrice = 5 }); // test customer: filtered
        orders.PipeInput("o4", new Order { Customer = "alice", Product = "lamp", Quantity = 1, UnitPrice = 30 });

        CollectionAssert.AreEqual(
            new[] { ("alice", 20.0), ("alice", 30.0) },
            totals.ReadKeyValueList().Select(r => (r.Message.Key, r.Message.Value)).ToArray());
    }
}

public class FlatMapOrderLinesTests
{
    [Test]
    public void EachLineBecomesOneRecordKeyedByProduct()
    {
        using var driver = ExampleDriver.Create(new FlatMapOrderLines());
        var orders = driver.CreateInputTopic(FlatMapOrderLines.InputTopic, new StringSerDes(), new JsonSerDes<MultiLineOrder>());
        var demand = driver.CreateOutputTopic(FlatMapOrderLines.OutputTopic, TimeSpan.FromSeconds(1),
            new StringSerDes(), new Int32SerDes());

        orders.PipeInput("o1", new MultiLineOrder
        {
            Customer = "alice",
            Lines = { new OrderLine { Product = "apple", Quantity = 3 }, new OrderLine { Product = "pear", Quantity = 1 } }
        });
        orders.PipeInput("o2", new MultiLineOrder { Customer = "bob" }); // no lines: no output
        orders.PipeInput("o3", new MultiLineOrder
        {
            Customer = "carol",
            Lines = { new OrderLine { Product = "apple", Quantity = 2 } }
        });

        CollectionAssert.AreEqual(
            new[] { ("apple", 3), ("pear", 1), ("apple", 2) },
            demand.ReadKeyValueList().Select(r => (r.Message.Key, r.Message.Value)).ToArray());
    }
}

public class BranchAndMergeTests
{
    [Test]
    public void PaymentsAreRoutedByMethodAndWalletsAreMerged()
    {
        using var driver = ExampleDriver.Create(new BranchAndMerge());
        var payments = driver.CreateInputTopic(BranchAndMerge.InputTopic, new StringSerDes(), new JsonSerDes<Payment>());
        var card = driver.CreateOutputTopic(BranchAndMerge.CardTopic, TimeSpan.FromSeconds(1), new StringSerDes(), new JsonSerDes<Payment>());
        var wallet = driver.CreateOutputTopic(BranchAndMerge.WalletTopic, TimeSpan.FromSeconds(1), new StringSerDes(), new JsonSerDes<Payment>());
        var other = driver.CreateOutputTopic(BranchAndMerge.OtherTopic, TimeSpan.FromSeconds(1), new StringSerDes(), new JsonSerDes<Payment>());

        payments.PipeInput("p1", new Payment { Method = "card", Amount = 10 });
        payments.PipeInput("p2", new Payment { Method = "paypal", Amount = 20 });
        payments.PipeInput("p3", new Payment { Method = "cash", Amount = 5 });
        payments.PipeInput("p4", new Payment { Method = "apple-pay", Amount = 30 });

        CollectionAssert.AreEqual(new[] { "p1" }, card.ReadKeyValueList().Select(r => r.Message.Key).ToArray());
        CollectionAssert.AreEqual(new[] { "p2", "p4" }, wallet.ReadKeyValueList().Select(r => r.Message.Key).ToArray());
        CollectionAssert.AreEqual(new[] { "p3" }, other.ReadKeyValueList().Select(r => r.Message.Key).ToArray());
    }
}

public class DynamicRoutingTests
{
    [Test]
    public void EventsAreWrittenToTheTopicOfTheirType()
    {
        using var driver = ExampleDriver.Create(new DynamicRouting());
        var events = driver.CreateInputTopic(DynamicRouting.InputTopic, new StringSerDes(), new JsonSerDes<SiteEvent>());
        var clicks = driver.CreateOutputTopic("events-click", TimeSpan.FromSeconds(1), new StringSerDes(), new JsonSerDes<SiteEvent>());
        var purchases = driver.CreateOutputTopic("events-purchase", TimeSpan.FromSeconds(1), new StringSerDes(), new JsonSerDes<SiteEvent>());

        events.PipeInput("e1", new SiteEvent { Type = "click", User = "alice" });
        events.PipeInput("e2", new SiteEvent { Type = "purchase", User = "bob" });
        events.PipeInput("e3", new SiteEvent { Type = "click", User = "carol" });

        CollectionAssert.AreEqual(new[] { "alice", "carol" }, clicks.ReadKeyValueList().Select(r => r.Message.Value.User).ToArray());
        CollectionAssert.AreEqual(new[] { "bob" }, purchases.ReadKeyValueList().Select(r => r.Message.Value.User).ToArray());
    }
}
