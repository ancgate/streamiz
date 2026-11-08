using Streamiz.Examples.Common;
using Streamiz.Examples.Serdes.Model.Proto;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SchemaRegistry.SerDes.Protobuf;
using Streamiz.Kafka.Net.SerDes;

namespace Streamiz.Examples.Serdes.Examples;

/// <summary>
/// Protobuf with Schema Registry. <see cref="Customer"/> is generated at build time from Protos/customer.proto
/// by the Grpc.Tools package (see the csproj).
/// </summary>
public class SchemaRegistryProtobufExample : IExample
{
    public string Name => "schema-registry-protobuf";

    public string Description => "Reads Protobuf customers through Schema Registry and re-keys the VIP ones by country.";

    public IEnumerable<string> InputTopics => new[] { "customers-proto" };

    public void BuildTopology(StreamBuilder builder)
    {
        builder.Stream<string, Customer, StringSerDes, SchemaProtobufSerDes<Customer>>("customers-proto")
            .Filter((_, customer, _) => customer.Vip)
            .SelectKey((_, customer, _) => customer.Country)
            .To<StringSerDes, SchemaProtobufSerDes<Customer>>("vip-customers-by-country");
    }

    public void Configure(StreamConfig config)
    {
        config.SchemaRegistryUrl = SchemaRegistry.Url;
        config.AutoRegisterSchemas = true;
    }
}
