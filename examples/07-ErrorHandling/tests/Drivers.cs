using System.Text;
using Confluent.Kafka;
using Streamiz.Examples.Common;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.Mock;

namespace Streamiz.Examples.ErrorHandling.Tests;

public static class Drivers
{
    /// <summary>A TopologyTestDriver running the example with its configuration.</summary>
    public static TopologyTestDriver For(IExample example, TopologyTestDriver.Mode mode = TopologyTestDriver.Mode.SYNC_TASK)
    {
        var builder = new StreamBuilder();
        example.BuildTopology(builder);
        return new TopologyTestDriver(builder.Build(), ExampleRunner.CreateConfig(example), mode);
    }

    public static string Header(Headers headers, string key) => Encoding.UTF8.GetString(headers.GetLastBytes(key));
}
