using NUnit.Framework;
using Streamiz.Kafka.Net.SerDes;

namespace Streamiz.Examples.Basics.Tests;

public class WordCountTests
{
    [Test]
    public void EachOccurrenceUpdatesTheCountOfItsWord()
    {
        using var driver = ExampleDriver.Create(new WordCount());
        var sentences = driver.CreateInputTopic<string, string>(WordCount.InputTopic);
        var counts = driver.CreateOutputTopic(WordCount.OutputTopic, TimeSpan.FromSeconds(1),
            new StringSerDes(), new Int64SerDes());

        sentences.PipeInput("s1", "Hello world");
        sentences.PipeInput("s2", "hello Streamiz");

        // the table emits one update per new count
        CollectionAssert.AreEqual(
            new[] { ("hello", 1L), ("world", 1L), ("hello", 2L), ("streamiz", 1L) },
            counts.ReadKeyValueList().Select(r => (r.Message.Key, r.Message.Value)).ToArray());

        // the counts can also be read from the state store
        var store = driver.GetKeyValueStore<string, long>(WordCount.StoreName);
        Assert.AreEqual(2L, store.Get("hello"));
        Assert.AreEqual(1L, store.Get("world"));
    }
}

public class TotalPerCustomerTests
{
    [Test]
    public void TotalsAndLargestOrdersAreUpdatedForEachOrder()
    {
        using var driver = ExampleDriver.Create(new TotalPerCustomer());
        var orders = driver.CreateInputTopic(TotalPerCustomer.InputTopic, new StringSerDes(), new JsonSerDes<Order>());
        var totals = driver.CreateOutputTopic(TotalPerCustomer.TotalsTopic, TimeSpan.FromSeconds(1),
            new StringSerDes(), new DoubleSerDes());
        var largest = driver.CreateOutputTopic(TotalPerCustomer.LargestOrderTopic, TimeSpan.FromSeconds(1),
            new StringSerDes(), new JsonSerDes<Order>());

        orders.PipeInput("alice", new Order { Customer = "alice", Product = "book", Quantity = 2, UnitPrice = 10 }); // 20
        orders.PipeInput("bob", new Order { Customer = "bob", Product = "pen", Quantity = 1, UnitPrice = 2 }); // 2
        orders.PipeInput("alice", new Order { Customer = "alice", Product = "lamp", Quantity = 1, UnitPrice = 30 }); // 30
        orders.PipeInput("alice", new Order { Customer = "alice", Product = "mug", Quantity = 1, UnitPrice = 5 }); // 5

        CollectionAssert.AreEqual(
            new[] { ("alice", 20.0), ("bob", 2.0), ("alice", 50.0), ("alice", 55.0) },
            totals.ReadKeyValueList().Select(r => (r.Message.Key, r.Message.Value)).ToArray());

        // the mug (5) is smaller than the lamp (30): the largest order of alice stays the lamp
        CollectionAssert.AreEqual(
            new[] { ("alice", "book"), ("bob", "pen"), ("alice", "lamp"), ("alice", "lamp") },
            largest.ReadKeyValueList().Select(r => (r.Message.Key, r.Message.Value.Product)).ToArray());

        Assert.AreEqual(55.0, driver.GetKeyValueStore<string, double>(TotalPerCustomer.TotalsStore).Get("alice"));
    }
}
