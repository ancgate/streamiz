using NUnit.Framework;
using Streamiz.Examples.StateAndQueries.Examples;
using Streamiz.Kafka.Net.SerDes;

namespace Streamiz.Examples.StateAndQueries.Tests;

public class ChangelogExampleTests
{
    [Test]
    public void EachUpdateOfTheStoreIsWrittenToItsChangelog()
    {
        using var test = new ExampleDriver(new ChangelogExample(), b => ChangelogExample.BuildTopology(b, logging: true));
        var views = test.Driver.CreateInputTopic<string, string>("page-views");
        var changelog = test.Driver.CreateOutputTopic<string, long, StringSerDes, Int64SerDes>(
            ChangelogExample.ChangelogTopic(test.Config.ApplicationId));

        views.PipeInput("alice", "home");
        views.PipeInput("bob", "home");

        // the changelog is what restores the store after a restart or a rebalance
        var records = changelog.ReadKeyValueList().Select(r => (r.Message.Key, r.Message.Value)).ToList();
        CollectionAssert.AreEqual(new[] { ("home", 1L), ("home", 2L) }, records);
    }

    [Test]
    public void WithoutLoggingNothingIsWrittenToTheChangelog()
    {
        using var test = new ExampleDriver(new ChangelogExample(), b => ChangelogExample.BuildTopology(b, logging: false));
        var views = test.Driver.CreateInputTopic<string, string>("page-views");
        var changelog = test.Driver.CreateOutputTopic<string, long, StringSerDes, Int64SerDes>(
            ChangelogExample.ChangelogTopic(test.Config.ApplicationId));

        views.PipeInput("alice", "home");

        Assert.IsEmpty(changelog.ReadKeyValueList());
        // the store still works, it's only not backed up
        Assert.AreEqual(1L, test.Driver.GetKeyValueStore<string, long>(ChangelogExample.StoreName).Get("home"));
    }
}
