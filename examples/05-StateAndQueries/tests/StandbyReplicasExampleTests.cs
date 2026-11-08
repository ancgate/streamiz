using NUnit.Framework;
using Streamiz.Examples.Common;
using Streamiz.Examples.StateAndQueries.Examples;

namespace Streamiz.Examples.StateAndQueries.Tests;

/// <summary>
/// Standby replicas need several running instances: the TopologyTestDriver runs one task, so these tests
/// check the configuration and the topology. See the README to try a failover with two instances.
/// </summary>
public class StandbyReplicasExampleTests
{
    [Test]
    public void OneStandbyReplicaIsConfigured()
    {
        var config = ExampleRunner.CreateConfig(new StandbyReplicasExample());

        Assert.AreEqual(1, config.NumStandbyReplicas);
        Assert.IsNotNull(config.ApplicationServer);
    }

    [Test]
    public void TheStoreCountsTheViews()
    {
        using var test = new ExampleDriver(new StandbyReplicasExample());
        var views = test.Driver.CreateInputTopic<string, string>("page-views");

        views.PipeInput("alice", "home");
        views.PipeInput("bob", "pricing");
        views.PipeInput("carol", "home");

        var store = test.Driver.GetKeyValueStore<string, long>(StandbyReplicasExample.StoreName);
        Assert.AreEqual(2L, store.Get("home"));
        Assert.AreEqual(1L, store.Get("pricing"));
    }
}
