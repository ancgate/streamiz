using Streamiz.Examples.Common;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.Mock;

namespace Streamiz.Examples.StateAndQueries.Tests;

/// <summary>Builds a TopologyTestDriver for an example, with its configuration and a temporary state directory.</summary>
public sealed class ExampleDriver : IDisposable
{
    private readonly string stateDir;

    public ExampleDriver(IExample example, Action<StreamBuilder> buildTopology = null)
    {
        var builder = new StreamBuilder();
        (buildTopology ?? example.BuildTopology)(builder);

        Config = ExampleRunner.CreateConfig(example);
        // a fresh state directory per test: RocksDB stores keep their files between runs otherwise
        stateDir = Path.Combine(Path.GetTempPath(), "streamiz-examples-tests", Guid.NewGuid().ToString());
        Config.StateDir = stateDir;

        Driver = new TopologyTestDriver(builder.Build(), Config);
    }

    public StreamConfig Config { get; }
    public TopologyTestDriver Driver { get; }

    public void Dispose()
    {
        Driver.Dispose();
        if (Directory.Exists(stateDir))
            Directory.Delete(stateDir, true);
    }
}
