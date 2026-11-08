using NUnit.Framework;
using Streamiz.Kafka.Net.Mock;

namespace Streamiz.Examples.Testing.Tests;

/// <summary>
/// ASYNC_CLUSTER_IN_MEMORY: an in-memory Kafka cluster with real stream threads, consumer groups and partitions.
/// Closer to production (rebalances, multi-partition topics, the async V1 thread...), but asynchronous:
/// outputs arrive a bit later, so wait for them, and records of different partitions can come out in any order.
/// Prefer the default SYNC_TASK mode, and use this one when the behaviour depends on the threading model.
/// </summary>
public class ClusterModeTests
{
    [Test]
    public void WaitForTheOutputs()
    {
        using var driver = Drivers.Create(OrderRouting.Build, mode: TopologyTestDriver.Mode.ASYNC_CLUSTER_IN_MEMORY);
        var orders = driver.CreateInputTopic<string, string>(OrderRouting.Input);
        var standard = driver.CreateOutputTopic<string, string>(OrderRouting.Standard);

        orders.PipeInput("o-1", "tea");
        orders.PipeInput("o-2", "cake");

        // polls the output topic until 2 records arrived (or the timeout expired)
        var records = IntegrationTestUtils.WaitUntilMinKeyValueRecordsReceived(standard, 2, TimeSpan.FromSeconds(10));

        // order across keys (partitions) isn't guaranteed: compare as sets
        CollectionAssert.AreEquivalent(new[] { "TEA", "CAKE" }, records.Select(r => r.Message.Value).ToList());
    }
}
