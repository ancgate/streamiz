using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;

namespace Streamiz.Examples.Testing.Tests;

internal static class Drivers
{
    /// <summary>
    /// Builds a topology and starts a TopologyTestDriver on it. A unique application id per test keeps the tests
    /// independent (state directories and in-memory topics are per application).
    /// </summary>
    public static TopologyTestDriver Create(Action<StreamBuilder> build, Action<StreamConfig> configure = null,
        TopologyTestDriver.Mode mode = TopologyTestDriver.Mode.SYNC_TASK)
    {
        var builder = new StreamBuilder();
        build(builder);

        var config = new StreamConfig<StringSerDes, StringSerDes>
        {
            ApplicationId = $"test-{Guid.NewGuid():N}"
        };
        configure?.Invoke(config);

        return new TopologyTestDriver(builder.Build(), config, mode);
    }

    /// <summary>A fixed point in time: tests with timestamps must not depend on the current time.</summary>
    public static readonly DateTime Start = new(2026, 1, 1, 10, 0, 0, DateTimeKind.Utc);
}
