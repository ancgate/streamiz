using System.Text;
using NUnit.Framework;
using Streamiz.Examples.Serdes.Examples;
using Streamiz.Examples.Serdes.Model;
using Streamiz.Kafka.Net.SerDes;

namespace Streamiz.Examples.Serdes.Tests;

public class CloudEventsExampleTests
{
    [Test]
    public void LargeOrdersAreRepublishedAsCloudEvents()
    {
        using var driver = TestDrivers.For(new CloudEventsExample());
        var orders = driver.CreateInputTopic("orders-cloudevents", new StringSerDes(), CloudEventsExample.CreateSerDes());
        var largeOrders = driver.CreateOutputTopic("large-orders-cloudevents", TimeSpan.FromSeconds(1),
            new StringSerDes(), CloudEventsExample.CreateSerDes());

        orders.PipeInput("o-1", new Order { Id = "o-1", Customer = "alice", Amount = 30, Currency = "EUR" });
        orders.PipeInput("o-2", new Order { Id = "o-2", Customer = "bob", Amount = 250, Currency = "USD" });

        var results = largeOrders.ReadKeyValueList().ToList();
        Assert.AreEqual(1, results.Count);
        Assert.AreEqual("o-2", results[0].Message.Value.Id);

        // binary content mode: the CloudEvents attributes are Kafka headers
        var headers = results[0].Message.Headers;
        Assert.AreEqual("o-2", Encoding.UTF8.GetString(headers.GetLastBytes("ce_id")));
        Assert.AreEqual(typeof(Order).FullName, Encoding.UTF8.GetString(headers.GetLastBytes("ce_type")));
    }
}
