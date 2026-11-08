using NUnit.Framework;
using Streamiz.Examples.Serdes.Examples;
using Streamiz.Kafka.Net.SerDes;

namespace Streamiz.Examples.Serdes.Tests;

public class MaterializedSerdesExampleTests
{
    [Test]
    public void ViewsAreCountedPerUserAsLongs()
    {
        using var driver = TestDrivers.For(new MaterializedSerdesExample());
        // the input uses the default serdes of the configuration (string, string)
        var views = driver.CreateInputTopic<string, string>("page-views");
        var counts = driver.CreateOutputTopic<string, long, StringSerDes, Int64SerDes>("views-per-user");

        views.PipeInput("alice", "/home");
        views.PipeInput("bob", "/pricing");
        views.PipeInput("alice", "/docs");

        var results = counts.ReadKeyValueList().Select(r => (r.Message.Key, r.Message.Value)).ToList();
        CollectionAssert.AreEqual(new[] { ("alice", 1L), ("bob", 1L), ("alice", 2L) }, results);

        // the store holds longs too
        var store = driver.GetKeyValueStore<string, long>("views-per-user-store");
        Assert.AreEqual(2L, store.Get("alice"));
    }
}
