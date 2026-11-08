using Streamiz.Examples.Common;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Examples.Basics;

/// <summary>
/// A KTable reads a topic as a changelog: the latest value of each key is the current state, and a record
/// with a null value (a tombstone) deletes the key. Filter and MapValues apply to the table itself.
/// </summary>
public class TableBasics : IExample
{
    public const string InputTopic = "user-countries";
    public const string OutputTopic = "user-countries-known";
    public const string StoreName = "user-countries-store";

    public string Name => "table-basics";
    public string Description => "A KTable of users' countries, with Filter, MapValues, updates and deletes.";
    public IEnumerable<string> InputTopics => new[] { InputTopic };

    public void BuildTopology(StreamBuilder builder)
    {
        // Table reads the topic and keeps the latest value of each key in a state store (queryable by its name).
        builder.Table(InputTopic, new StringSerDes(), new StringSerDes(), InMemory.As<string, string>(StoreName))
            // Filter on a table: a key whose new value doesn't match is removed from the result,
            // so downstream it's a delete (a null value), not just a skipped record.
            .Filter((user, country, _) => country != "unknown")
            // MapValues on a table: here normalize the country code.
            .MapValues((country, _) => country?.ToUpperInvariant())
            // ToStream emits every change of the table, including deletes (null values).
            .ToStream()
            .To(OutputTopic, new StringSerDes(), new StringSerDes());
    }

    public void Configure(StreamConfig config)
    {
    }
}
