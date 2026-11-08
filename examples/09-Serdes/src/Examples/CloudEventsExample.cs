using System.Text.Json;
using CloudNative.CloudEvents;
using CloudNative.CloudEvents.SystemTextJson;
using Streamiz.Examples.Common;
using Streamiz.Examples.Serdes.Model;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.SerDes.CloudEvents;

namespace Streamiz.Examples.Serdes.Examples;

/// <summary>
/// CloudEvents (Kafka protocol binding). In binary mode the value is the JSON payload and the CloudEvents
/// attributes (id, type, source...) are written as <c>ce_*</c> headers.
/// </summary>
public class CloudEventsExample : IExample
{
    public string Name => "cloud-events";

    public string Description => "Reads orders as CloudEvents and republishes the large ones with the order id as event id.";

    public IEnumerable<string> InputTopics => new[] { "orders-cloudevents" };

    public void BuildTopology(StreamBuilder builder)
    {
        builder.Stream("orders-cloudevents", new StringSerDes(), CreateSerDes())
            .Filter((_, order, _) => order.Amount >= 100)
            .To("large-orders-cloudevents", new StringSerDes(), CreateSerDes());
    }

    /// <summary>
    /// JSON payload, binary content mode, and the event id taken from the order (the default is a new Guid per record).
    /// </summary>
    public static CloudEventSerDes<Order> CreateSerDes() => new(
        new JsonEventFormatter(),
        cloudEvent => JsonSerializer.Deserialize<Order>(cloudEvent.Data!.ToString()!),
        ContentMode.Binary,
        order => order.Id);

    public void Configure(StreamConfig config)
    {
    }
}
