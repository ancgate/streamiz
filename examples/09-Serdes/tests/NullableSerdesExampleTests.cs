using NUnit.Framework;
using Streamiz.Examples.Serdes.Examples;
using Streamiz.Kafka.Net.SerDes;

namespace Streamiz.Examples.Serdes.Tests;

public class NullableSerdesExampleTests
{
    [Test]
    public void LowStockIsRestockedAndNullMeansDiscontinued()
    {
        using var driver = TestDrivers.For(new NullableSerdesExample());
        var stock = driver.CreateInputTopic<string, long?, StringSerDes, NullableInt64SerDes>("stock-levels");
        var restock = driver.CreateOutputTopic<string, long, StringSerDes, Int64SerDes>("restock-requests");
        var discontinued = driver.CreateOutputTopic<string, byte[], StringSerDes, ByteArraySerDes>("discontinued-products");

        stock.PipeInput("sku-1", 3L);
        stock.PipeInput("sku-2", 50L);
        stock.PipeInput("sku-3", null);

        var requests = restock.ReadKeyValueList().Select(r => (r.Message.Key, r.Message.Value)).ToList();
        CollectionAssert.AreEqual(new[] { ("sku-1", 17L) }, requests);

        // read as raw bytes: the value of a discontinued product is a real Kafka null (a tombstone)
        var removed = discontinued.ReadKeyValueList().ToList();
        Assert.AreEqual(1, removed.Count);
        Assert.AreEqual("sku-3", removed[0].Message.Key);
        Assert.IsNull(removed[0].Message.Value);
    }
}
