using NUnit.Framework;
using Streamiz.Examples.Common;
using Streamiz.Examples.StateAndQueries.Examples;
using Streamiz.Kafka.Net;

namespace Streamiz.Examples.StateAndQueries.Tests;

public class InteractiveQueriesTests
{
    private class FakeBackend : ICountBackend
    {
        public KeyLocation Location { get; set; }
        public Dictionary<(string, int), long> LocalCounts { get; } = new();

        public KeyLocation Locate(string key) => Location;
        public long GetLocal(string key, int partition) => LocalCounts[(key, partition)];
    }

    private static readonly HostInfo Self = new("host-1", 7070);

    [Test]
    public void AKeyHostedHereIsReadFromTheStoreOfItsPartition()
    {
        var backend = new FakeBackend { Location = new KeyLocation(Self, 3) };
        backend.LocalCounts[("home", 3)] = 12;

        var result = new CountQueryRouter(backend, Self).Query("home");

        Assert.AreEqual(12L, result.Count);
        Assert.IsNull(result.RedirectUrl);
    }

    [Test]
    public void AKeyHostedElsewhereRedirectsToItsInstance()
    {
        var backend = new FakeBackend { Location = new KeyLocation(new HostInfo("host-2", 7070), 1) };

        var result = new CountQueryRouter(backend, Self).Query("home");

        Assert.AreEqual("http://host-2:7070/count/home", result.RedirectUrl);
    }

    [Test]
    public void NoAnswerWhileTheMetadataIsUnknown()
    {
        // before the instance knows the others, or during a rebalance
        var backend = new FakeBackend { Location = new KeyLocation(HostInfo.Unavailable, -1) };

        Assert.IsTrue(new CountQueryRouter(backend, Self).Query("home").Unavailable);
    }

    [Test]
    public void TheInstancePublishesItsEndpoint()
    {
        var example = new InteractiveQueriesHttpExample();
        var config = ExampleRunner.CreateConfig(example);

        Assert.AreEqual(InteractiveQueriesHttpExample.ApplicationServer, config.ApplicationServer);
        Assert.DoesNotThrow(() => HostInfo.Parse(config.ApplicationServer));
    }

    [Test]
    public void TheStoreCountsTheViews()
    {
        using var test = new ExampleDriver(new InteractiveQueriesHttpExample());
        var views = test.Driver.CreateInputTopic<string, string>("page-views");

        views.PipeInput("alice", "home");
        views.PipeInput("bob", "home");

        Assert.AreEqual(2L, test.Driver.GetKeyValueStore<string, long>(InteractiveQueriesHttpExample.StoreName).Get("home"));
    }
}
