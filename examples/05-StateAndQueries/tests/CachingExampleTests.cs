using NUnit.Framework;
using Streamiz.Examples.StateAndQueries.Examples;
using Streamiz.Kafka.Net.SerDes;

namespace Streamiz.Examples.StateAndQueries.Tests;

public class CachingExampleTests
{
    [Test]
    public void WithCacheOnlyTheLatestCountIsForwardedOnCommit()
    {
        using var test = new ExampleDriver(new CachingExample(), b => CachingExample.BuildTopology(b, caching: true));
        var views = test.Driver.CreateInputTopic<string, string>("page-views");
        var counts = test.Driver.CreateOutputTopic<string, long, StringSerDes, Int64SerDes>("page-view-counts");

        views.PipeInput("alice", "home");
        views.PipeInput("bob", "home");
        views.PipeInput("carol", "home");

        // the updates wait in the cache until the next commit
        Assert.IsEmpty(counts.ReadKeyValueList());

        test.Driver.Commit();

        // one record per key, with the latest count
        var output = counts.ReadKeyValueList().Select(r => (r.Message.Key, r.Message.Value)).ToList();
        CollectionAssert.AreEqual(new[] { ("home", 3L) }, output);
    }

    [Test]
    public void WithoutCacheEveryUpdateIsForwarded()
    {
        using var test = new ExampleDriver(new CachingExample(), b => CachingExample.BuildTopology(b, caching: false));
        var views = test.Driver.CreateInputTopic<string, string>("page-views");
        var counts = test.Driver.CreateOutputTopic<string, long, StringSerDes, Int64SerDes>("page-view-counts");

        views.PipeInput("alice", "home");
        views.PipeInput("bob", "home");
        views.PipeInput("carol", "home");

        var output = counts.ReadKeyValueList().Select(r => (r.Message.Key, r.Message.Value)).ToList();
        CollectionAssert.AreEqual(new[] { ("home", 1L), ("home", 2L), ("home", 3L) }, output);
    }
}
