using Streamiz.Examples.Common;
using Streamiz.Examples.Serdes.Model;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SchemaRegistry.SerDes.Avro;
using Streamiz.Kafka.Net.SerDes;

namespace Streamiz.Examples.Serdes.Examples;

/// <summary>
/// Avro with Schema Registry, with a specific record class (<see cref="Payment"/>).
/// </summary>
public class SchemaRegistryAvroExample : IExample
{
    public string Name => "schema-registry-avro";

    public string Description => "Reads Avro payments through Schema Registry and routes the failed ones to their own topic.";

    public IEnumerable<string> InputTopics => new[] { "payments-avro" };

    public void BuildTopology(StreamBuilder builder)
    {
        builder.Stream<string, Payment, StringSerDes, SchemaAvroSerDes<Payment>>("payments-avro")
            .Filter((_, payment, _) => payment.status == "FAILED")
            .To<StringSerDes, SchemaAvroSerDes<Payment>>("payments-failed");
    }

    public void Configure(StreamConfig config)
    {
        config.SchemaRegistryUrl = SchemaRegistry.Url;
        config.AutoRegisterSchemas = true;
    }
}
