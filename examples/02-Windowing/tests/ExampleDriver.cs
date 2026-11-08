using Confluent.Kafka;
using Streamiz.Examples.Common;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.State;

namespace Streamiz.Examples.Windowing.Tests;

/// <summary>
/// Runs an example's topology with the TopologyTestDriver: no Kafka needed, records are processed as soon
/// as they're piped in.
/// </summary>
internal static class ExampleDriver
{
    /// <summary>A fixed point in time (08:00 UTC), so that the windows in the tests are readable.</summary>
    public static readonly DateTime T0 = new(2026, 1, 1, 8, 0, 0, DateTimeKind.Utc);

    public static long Ms(DateTime dateTime) => new DateTimeOffset(dateTime).ToUnixTimeMilliseconds();

    /// <summary>Milliseconds of a time relative to T0.</summary>
    public static long At(TimeSpan offset) => Ms(T0 + offset);

    public static TopologyTestDriver Create(IExample example)
    {
        var builder = new StreamBuilder();
        example.BuildTopology(builder);
        return new TopologyTestDriver(builder.Build(), ExampleRunner.CreateConfig(example));
    }

    /// <summary>A windowed record as (key, window start, window end, value), with times relative to T0.</summary>
    public static (string Key, TimeSpan Start, TimeSpan End, V Value) Read<V>(ConsumeResult<Windowed<string>, V> record)
        => (record.Message.Key.Key,
            TimeSpan.FromMilliseconds(record.Message.Key.Window.StartMs - Ms(T0)),
            TimeSpan.FromMilliseconds(record.Message.Key.Window.EndMs - Ms(T0)),
            record.Message.Value);

    public static TimeSpan Sec(double seconds) => TimeSpan.FromSeconds(seconds);
    public static TimeSpan Min(double minutes) => TimeSpan.FromMinutes(minutes);
}
