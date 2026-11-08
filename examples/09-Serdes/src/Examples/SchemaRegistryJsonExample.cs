using Streamiz.Examples.Common;
using Streamiz.Examples.Serdes.Model;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SchemaRegistry.SerDes.Json;
using Streamiz.Kafka.Net.SerDes;

namespace Streamiz.Examples.Serdes.Examples;

/// <summary>
/// JSON Schema with Schema Registry: the serdes registers the schema generated from the C# class
/// and writes the Confluent wire format (magic byte + schema id + JSON).
/// </summary>
public class SchemaRegistryJsonExample : IExample
{
    public string Name => "schema-registry-json";

    public string Description => "Reads JSON Schema orders through Schema Registry and keeps the EUR ones.";

    public IEnumerable<string> InputTopics => new[] { "orders-json" };

    public void BuildTopology(StreamBuilder builder)
    {
        builder.Stream<string, Order, StringSerDes, SchemaJsonSerDes<Order>>("orders-json")
            .Filter((_, order, _) => order.Currency == "EUR")
            .To<StringSerDes, SchemaJsonSerDes<Order>>("orders-json-eur");
    }

    public void Configure(StreamConfig config)
    {
        // the Schema Registry serdes read their settings from the stream configuration
        config.SchemaRegistryUrl = SchemaRegistry.Url;
        config.AutoRegisterSchemas = true;
    }
}
