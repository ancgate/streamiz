using Streamiz.Examples.Common;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Examples.Basics;

/// <summary>
/// Aggregate and Reduce on a grouped stream: the total spent by each customer, and their largest order.
/// </summary>
public class TotalPerCustomer : IExample
{
    public const string InputTopic = "customer-orders";
    public const string TotalsTopic = "customer-totals";
    public const string LargestOrderTopic = "customer-largest-order";
    public const string TotalsStore = "customer-totals-store";
    public const string LargestOrderStore = "customer-largest-order-store";

    public string Name => "total-per-customer";
    public string Description => "Aggregate (total spent) and Reduce (largest order) per customer.";
    public IEnumerable<string> InputTopics => new[] { InputTopic };

    public void BuildTopology(StreamBuilder builder)
    {
        // The orders are already keyed by customer: GroupByKey groups them without any repartition.
        var ordersByCustomer = builder
            .Stream(InputTopic, new StringSerDes(), new JsonSerDes<Order>())
            .GroupByKey();

        // Aggregate builds a result of another type than the records: an initial value (0), and a function
        // which adds each order to the current total. The result type needs its own serdes for the store.
        ordersByCustomer
            .Aggregate(
                () => 0.0,
                (customer, order, total) => total + order.Total(),
                InMemory.As<string, double>(TotalsStore).WithValueSerdes(new DoubleSerDes()))
            .ToStream()
            .To(TotalsTopic, new StringSerDes(), new DoubleSerDes());

        // Reduce combines two values of the same type: keep the largest order seen so far.
        ordersByCustomer
            .Reduce(
                (largest, order) => order.Total() > largest.Total() ? order : largest,
                InMemory.As<string, Order>(LargestOrderStore).WithValueSerdes(new JsonSerDes<Order>()))
            .ToStream()
            .To(LargestOrderTopic, new StringSerDes(), new JsonSerDes<Order>());
    }

    public void Configure(StreamConfig config)
    {
    }
}
