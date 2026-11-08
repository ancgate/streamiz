using NUnit.Framework;
using Streamiz.Examples.Common;
using Streamiz.Examples.StateAndQueries.Examples;
using Streamiz.Kafka.Net.SerDes;

namespace Streamiz.Examples.StateAndQueries.Tests;

public class StoreTypesTests
{
    private static IEnumerable<IExample> Examples() => new IExample[] { new InMemoryStoreExample(), new RocksDbStoreExample() };

    [TestCaseSource(nameof(Examples))]
    public void CountsTheViewsOfEachPage(IExample example)
    {
        using var test = new ExampleDriver(example);
        var views = test.Driver.CreateInputTopic<string, string>("page-views");
        var counts = test.Driver.CreateOutputTopic<string, long, StringSerDes, Int64SerDes>("page-view-counts");

        views.PipeInput("alice", "home");
        views.PipeInput("bob", "home");
        views.PipeInput("alice", "pricing");

        // each update of a count is forwarded (the examples disable the store cache)
        var output = counts.ReadKeyValueList().Select(r => (r.Message.Key, r.Message.Value)).ToList();
        CollectionAssert.AreEqual(new[] { ("home", 1L), ("home", 2L), ("pricing", 1L) }, output);

        // the store holds the latest count of each page
        var store = test.Driver.GetKeyValueStore<string, long>(InMemoryStoreExample.StoreName);
        Assert.AreEqual(2L, store.Get("home"));
        Assert.AreEqual(1L, store.Get("pricing"));
    }
}
