using Streamiz.Examples.Common;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.Mock;

namespace Streamiz.Examples.ProcessorApi.Tests;

/// <summary>
/// Runs an example's topology in a TopologyTestDriver, with the configuration the example would use against Kafka.
/// </summary>
internal static class ExampleDriver
{
    /// <summary>A fixed point in time, so the record timestamps of the tests are easy to read.</summary>
    public static readonly DateTime T0 = new(2026, 1, 1, 12, 0, 0, DateTimeKind.Utc);

    public static TopologyTestDriver Create(IExample example)
    {
        var builder = new StreamBuilder();
        example.BuildTopology(builder);
        return new TopologyTestDriver(builder.Build(), ExampleRunner.CreateConfig(example));
    }

    /// <summary>Reads every record written to the topic so far, as (key, value) pairs.</summary>
    public static List<(string Key, string Value)> ReadAll(this TestOutputTopic<string, string> topic)
        => topic.ReadKeyValueList().Select(r => (r.Message.Key, r.Message.Value)).ToList();
}
