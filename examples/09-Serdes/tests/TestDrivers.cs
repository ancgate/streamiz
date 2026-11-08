using Streamiz.Examples.Common;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.Mock;

namespace Streamiz.Examples.Serdes.Tests;

internal static class TestDrivers
{
    /// <summary>
    /// A TopologyTestDriver running the example's topology with the example's configuration.
    /// <paramref name="adjust"/> changes the configuration for the test (e.g. a mock Schema Registry).
    /// </summary>
    public static TopologyTestDriver For(IExample example, Action<StreamConfig> adjust = null)
    {
        var builder = new StreamBuilder();
        example.BuildTopology(builder);
        var config = ExampleRunner.CreateConfig(example);
        adjust?.Invoke(config);
        return new TopologyTestDriver(builder.Build(), config);
    }
}
