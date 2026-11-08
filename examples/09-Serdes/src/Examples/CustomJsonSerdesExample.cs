using Streamiz.Examples.Common;
using Streamiz.Examples.Serdes.Model;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;

namespace Streamiz.Examples.Serdes.Examples;

/// <summary>
/// A custom serdes (<see cref="SystemTextJsonSerDes{T}"/>) for a C# class. Streamiz also ships a Newtonsoft-based
/// <see cref="Streamiz.Kafka.Net.SerDes.JsonSerDes{T}"/>; this one shows how to write your own.
/// </summary>
public class CustomJsonSerdesExample : IExample
{
    public string Name => "custom-json-serdes";

    public string Description => "Reads JSON orders with a custom System.Text.Json serdes and keeps the large ones.";

    public IEnumerable<string> InputTopics => new[] { "orders" };

    public void BuildTopology(StreamBuilder builder)
    {
        // serdes instances can also be passed directly
        builder.Stream("orders", new StringSerDes(), new SystemTextJsonSerDes<Order>())
            .Filter((_, order, _) => order.Amount >= 100)
            .To("large-orders", new StringSerDes(), new SystemTextJsonSerDes<Order>());
    }

    public void Configure(StreamConfig config)
    {
    }
}
