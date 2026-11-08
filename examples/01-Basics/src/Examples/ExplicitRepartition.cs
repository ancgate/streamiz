using Streamiz.Examples.Common;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;

namespace Streamiz.Examples.Basics;

/// <summary>
/// Repartition writes a stream to an internal topic and reads it back, so that records with the same key
/// end up in the same partition. GroupBy and joins do it automatically after a key change; Repartition does
/// it explicitly, and lets you choose the topic name, the number of partitions and the serdes.
/// </summary>
public class ExplicitRepartition : IExample
{
    public const string InputTopic = "orders-by-id";
    public const string OutputTopic = "orders-by-customer";
    public const string RepartitionName = "orders-by-customer";

    private readonly int partitions;

    /// <param name="partitions">Number of partitions of the repartition topic.</param>
    public ExplicitRepartition(int partitions = 4)
    {
        this.partitions = partitions;
    }

    public string Name => "explicit-repartition";
    public string Description => "Re-key orders by customer and repartition them explicitly with Repartitioned.";
    public IEnumerable<string> InputTopics => new[] { InputTopic };

    public void BuildTopology(StreamBuilder builder)
    {
        builder.Stream(InputTopic, new StringSerDes(), new JsonSerDes<Order>())
            .SelectKey((orderId, order, _) => order.Customer)
            // The internal topic is named "<application id>-orders-by-customer-repartition" and created with
            // the given number of partitions (4 by default).
            .Repartition(Repartitioned<string, Order>
                .As(RepartitionName)
                .WithKeySerdes(new StringSerDes())
                .WithValueSerdes(new JsonSerDes<Order>())
                .WithNumberOfPartitions(partitions))
            .MapValues((order, _) => $"{order.Quantity} x {order.Product}")
            .To(OutputTopic, new StringSerDes(), new StringSerDes());
    }

    public void Configure(StreamConfig config)
    {
    }
}
