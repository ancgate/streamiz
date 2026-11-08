using Streamiz.Kafka.Net;

namespace Streamiz.Examples.Common;

/// <summary>
/// One runnable example: a topology plus the configuration it needs.
/// Every example group (console app) lists its examples, and its test project checks each topology
/// with the TopologyTestDriver, so the examples stay correct when the library changes.
/// </summary>
public interface IExample
{
    /// <summary>Name used on the command line, e.g. <c>dotnet run -- word-count</c>.</summary>
    string Name { get; }

    /// <summary>One sentence describing what the example shows.</summary>
    string Description { get; }

    /// <summary>Topics to create before running the example against a Kafka cluster.</summary>
    IEnumerable<string> InputTopics { get; }

    /// <summary>Builds the topology.</summary>
    void BuildTopology(StreamBuilder builder);

    /// <summary>Applies the settings this example needs (serdes, guarantees, handlers...).</summary>
    void Configure(StreamConfig config);
}
