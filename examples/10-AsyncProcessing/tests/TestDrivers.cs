using Streamiz.Examples.Common;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.Mock;

namespace Streamiz.Examples.AsyncProcessing.Tests;

internal static class TestDrivers
{
    /// <summary>
    /// A TopologyTestDriver running the example's topology with the example's configuration.
    /// In SYNC_TASK mode the driver waits for the async calls after each PipeInput, so outputs can be read right away.
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
