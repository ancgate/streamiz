using Streamiz.Examples.Common;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;

namespace Streamiz.Examples.Basics;

/// <summary>
/// Stateless record-by-record operations: Filter, FilterNot, Peek, SelectKey and MapValues.
/// Orders keyed by order id become order totals keyed by customer.
/// </summary>
public class FilterAndTransform : IExample
{
    public const string InputTopic = "orders";
    public const string OutputTopic = "order-totals-by-customer";

    public string Name => "filter-and-transform";
    public string Description => "Filter, FilterNot, Peek, SelectKey and MapValues on a stream of orders.";
    public IEnumerable<string> InputTopics => new[] { InputTopic };

    public void BuildTopology(StreamBuilder builder)
    {
        // Read the orders: key = order id, value = Order as JSON.
        builder.Stream(InputTopic, new StringSerDes(), new JsonSerDes<Order>())
            // Filter keeps the records for which the predicate is true: drop empty orders.
            .Filter((orderId, order, _) => order.Quantity > 0)
            // FilterNot drops the records for which the predicate is true: drop the test customer.
            .FilterNot((orderId, order, _) => order.Customer == "test")
            // Peek runs a side effect (here a log line) without changing the stream.
            .Peek((orderId, order, _) => Console.WriteLine($"order {orderId}: {order.Quantity} x {order.Product}"))
            // SelectKey changes the key: the order is now keyed by customer, the value is unchanged.
            // Changing the key marks the stream for repartitioning if it's grouped or joined later.
            .SelectKey((orderId, order, _) => order.Customer)
            // MapValues changes the value only, which never requires a repartition: keep the order total.
            .MapValues((order, _) => order.Total())
            // Write the result with the serdes of the new types.
            .To(OutputTopic, new StringSerDes(), new DoubleSerDes());
    }

    public void Configure(StreamConfig config)
    {
    }
}
