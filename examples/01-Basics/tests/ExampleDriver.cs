using Streamiz.Examples.Common;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.Mock;

namespace Streamiz.Examples.Basics.Tests;

/// <summary>
/// Runs an example's topology with the TopologyTestDriver: no Kafka needed, records are processed as soon
/// as they're piped in.
/// </summary>
internal static class ExampleDriver
{
    /// <summary>A fixed point in time, so that timestamps in the tests are readable.</summary>
    public static readonly DateTime T0 = new(2026, 1, 1, 8, 0, 0, DateTimeKind.Utc);

    public static long Ms(DateTime dateTime) => new DateTimeOffset(dateTime).ToUnixTimeMilliseconds();

    public static TopologyTestDriver Create(IExample example)
    {
        var builder = new StreamBuilder();
        example.BuildTopology(builder);
        return new TopologyTestDriver(builder.Build(), ExampleRunner.CreateConfig(example));
    }
}
