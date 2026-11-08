using Streamiz.Examples.Common;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;

namespace Streamiz.Examples.Joins.Examples;

/// <summary>
/// Stream-stream inner join: an order is confirmed as paid when its payment arrives within 10 minutes.
/// </summary>
public class OrdersPaymentsJoinExample : IExample
{
    public const string OrdersTopic = "orders";
    public const string PaymentsTopic = "payments";
    public const string OutputTopic = "orders-paid";

    public string Name => "orders-payments-join";

    public string Description => "Stream-stream inner join: match each order with its payment received within 10 minutes.";

    public IEnumerable<string> InputTopics => new[] { OrdersTopic, PaymentsTopic };

    public void BuildTopology(StreamBuilder builder)
    {
        // Both streams are keyed by order id (orders: amount, payments: payment method).
        // A stream-stream join requires co-partitioned topics: same key, same number of partitions.
        var orders = builder.Stream<string, string>(OrdersTopic);
        var payments = builder.Stream<string, string>(PaymentsTopic);

        orders.Join(
                payments,
                // Called for each pair of records with the same key whose timestamps are at most 10 minutes apart.
                (amount, method) => $"amount={amount} paid-by={method}",
                // The join window: records of both sides are kept 10 minutes (in two window stores) to find matches.
                JoinWindowOptions.Of(TimeSpan.FromMinutes(10)),
                // Serdes of the key and of both values, used for the window stores.
                StreamJoinProps.With<string, string, string>(new StringSerDes(), new StringSerDes(), new StringSerDes()))
            .To(OutputTopic);
    }

    public void Configure(StreamConfig config)
    {
    }
}

/// <summary>
/// Stream-stream left join: every order produces a result, "pending" until its payment arrives.
/// </summary>
public class OrdersPaymentsLeftJoinExample : IExample
{
    public const string OrdersTopic = "orders";
    public const string PaymentsTopic = "payments";
    public const string OutputTopic = "orders-payment-status";

    public string Name => "orders-payments-left-join";

    public string Description => "Stream-stream left join: each order gets a status, 'pending' until its payment arrives within 10 minutes.";

    public IEnumerable<string> InputTopics => new[] { OrdersTopic, PaymentsTopic };

    public void BuildTopology(StreamBuilder builder)
    {
        var orders = builder.Stream<string, string>(OrdersTopic);
        var payments = builder.Stream<string, string>(PaymentsTopic);

        // LeftJoin: an order is emitted right away with a null payment when no payment matches yet,
        // and emitted again when a payment arrives within the window.
        // Note: Streamiz emits the non-matched result eagerly (Kafka Streams 3.x waits for the window to close).
        orders.LeftJoin(
                payments,
                (amount, method) => method == null
                    ? $"amount={amount} payment=pending"
                    : $"amount={amount} paid-by={method}",
                JoinWindowOptions.Of(TimeSpan.FromMinutes(10)))
            .To(OutputTopic);
    }

    public void Configure(StreamConfig config)
    {
    }
}

/// <summary>
/// Stream-stream outer join: orders without payment and payments without order are both reported.
/// </summary>
public class OrdersPaymentsOuterJoinExample : IExample
{
    public const string OrdersTopic = "orders";
    public const string PaymentsTopic = "payments";
    public const string OutputTopic = "orders-reconciliation";

    public string Name => "orders-payments-outer-join";

    public string Description => "Stream-stream outer join: reconcile orders and payments, reporting the ones missing on either side.";

    public IEnumerable<string> InputTopics => new[] { OrdersTopic, PaymentsTopic };

    public void BuildTopology(StreamBuilder builder)
    {
        var orders = builder.Stream<string, string>(OrdersTopic);
        var payments = builder.Stream<string, string>(PaymentsTopic);

        // OuterJoin: either side can be null in the joiner.
        orders.OuterJoin(
                payments,
                (amount, method) => (amount, method) switch
                {
                    (null, _) => $"payment by {method} without order",
                    (_, null) => $"order of {amount} without payment",
                    _ => $"order of {amount} paid by {method}"
                },
                JoinWindowOptions.Of(TimeSpan.FromMinutes(10)))
            .To(OutputTopic);
    }

    public void Configure(StreamConfig config)
    {
    }
}
