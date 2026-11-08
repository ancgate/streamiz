using Streamiz.Examples.Common;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Examples.Serdes.Examples;

/// <summary>
/// Default serdes in the configuration, and serdes for a state store and an output topic whose types differ from them.
/// </summary>
public class MaterializedSerdesExample : IExample
{
    public string Name => "materialized-serdes";

    public string Description => "Counts page views per user: default string serdes for the input, Int64 serdes for the store and the output.";

    public IEnumerable<string> InputTopics => new[] { "page-views" };

    public void BuildTopology(StreamBuilder builder)
    {
        // no serdes given: the defaults of the configuration are used (StreamConfig<StringSerDes, StringSerDes>)
        builder.Stream<string, string>("page-views") // key: user, value: page
            .GroupByKey()
            // the store holds long values: its serdes are given by Materialized
            .Count(InMemory.As<string, long, StringSerDes, Int64SerDes>("views-per-user-store"))
            .ToStream()
            // and the output topic too
            .To<StringSerDes, Int64SerDes>("views-per-user");
    }

    public void Configure(StreamConfig config)
    {
        // the defaults are already string/string (ExampleRunner creates a StreamConfig<StringSerDes, StringSerDes>);
        // you can also set them explicitly:
        config.DefaultKeySerDes = new StringSerDes();
        config.DefaultValueSerDes = new StringSerDes();
    }
}
