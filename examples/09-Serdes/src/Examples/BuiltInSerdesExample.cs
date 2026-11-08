using Streamiz.Examples.Common;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;

namespace Streamiz.Examples.Serdes.Examples;

/// <summary>
/// Built-in serdes given per operation: the topic's types don't have to match the default serdes of the configuration.
/// </summary>
public class BuiltInSerdesExample : IExample
{
    public string Name => "built-in-serdes";

    public string Description => "Reads double temperatures and writes rounded int alerts, with serdes given per operation.";

    public IEnumerable<string> InputTopics => new[] { "temperatures" };

    public void BuildTopology(StreamBuilder builder)
    {
        // key: sensor id (string), value: temperature in °C (double, 8 bytes).
        // The serdes are given as type parameters, so the default serdes of the configuration (string) don't matter here.
        builder.Stream<string, double, StringSerDes, DoubleSerDes>("temperatures")
            .Filter((_, celsius, _) => celsius >= 30.0)
            // the value type changes: double -> int
            .MapValues((celsius, _) => (int) Math.Round(celsius))
            // so the sink needs the Int32 serdes (4 bytes, big-endian like Java's IntegerSerializer)
            .To<StringSerDes, Int32SerDes>("heat-alerts");
    }

    public void Configure(StreamConfig config)
    {
        // nothing to change: the defaults (string key and value) aren't used by this topology
    }
}
