using System.Text;
using NUnit.Framework;
using Streamiz.Examples.Serdes.Examples;
using Streamiz.Examples.Serdes.Model;
using Streamiz.Kafka.Net.SerDes;

namespace Streamiz.Examples.Serdes.Tests;

public class CustomJsonSerdesExampleTests
{
    [Test]
    public void LargeOrdersAreKept()
    {
        using var driver = TestDrivers.For(new CustomJsonSerdesExample());
        var orders = driver.CreateInputTopic("orders", new StringSerDes(), new SystemTextJsonSerDes<Order>());
        var largeOrders = driver.CreateOutputTopic("large-orders", TimeSpan.FromSeconds(1),
            new StringSerDes(), new SystemTextJsonSerDes<Order>());

        orders.PipeInput("o-1", new Order { Id = "o-1", Customer = "alice", Amount = 30, Currency = "EUR" });
        orders.PipeInput("o-2", new Order { Id = "o-2", Customer = "bob", Amount = 250, Currency = "USD" });

        var results = largeOrders.ReadValueList().ToList();
        Assert.AreEqual(1, results.Count);
        Assert.AreEqual("o-2", results[0].Id);
        Assert.AreEqual(250, results[0].Amount);
    }

    [Test]
    public void TheSerdesWritesCamelCaseJson()
    {
        var serdes = new SystemTextJsonSerDes<Order>();
        var bytes = serdes.Serialize(new Order { Id = "o-1", Customer = "alice", Amount = 30, Currency = "EUR" }, default);

        Assert.AreEqual("{\"id\":\"o-1\",\"customer\":\"alice\",\"amount\":30,\"currency\":\"EUR\"}",
            Encoding.UTF8.GetString(bytes));
        Assert.IsNull(serdes.Serialize(null, default));
    }
}
