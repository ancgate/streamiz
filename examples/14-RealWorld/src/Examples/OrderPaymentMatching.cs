using Streamiz.Examples.Common;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.Processors.Public;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Stream;

namespace Streamiz.Examples.RealWorld.Examples;

/// <summary>
/// Order-payment matching (dataset: ecommerce). Orders paid within 30 minutes are confirmed with a stream-stream
/// join. A stateful transformer reports the orders still unpaid 30 minutes after they were placed, and the
/// payments that arrive after that deadline.
/// </summary>
public class OrderPaymentMatching : IExample
{
    public const string OrdersTopic = "ecommerce.orders";
    public const string PaymentsTopic = "ecommerce.payments";
    public const string PaidTopic = "ecommerce.orders-paid";
    public const string UnpaidTopic = "ecommerce.orders-unpaid";
    public const string LatePaymentsTopic = "ecommerce.payments-late";
    public const string StateStore = "order-payment-state";
    public static readonly TimeSpan PaymentDeadline = TimeSpan.FromMinutes(30);
    /// <summary>Unpaid orders are forgotten a day after the order (a payment after that is ignored).</summary>
    public static readonly TimeSpan ForgetAfter = TimeSpan.FromDays(1);

    public string Name => "order-payment-matching";

    public string Description => "Match orders with payments within 30 minutes (stream-stream join), report unpaid orders and late payments.";

    public IEnumerable<string> InputTopics => new[] { OrdersTopic, PaymentsTopic };

    public void BuildTopology(StreamBuilder builder)
    {
        // both topics are keyed by order id, so they're co-partitioned: no repartition needed
        var orders = builder.Stream(OrdersTopic, new StringSerDes(), new CamelJsonSerDes<Order>());
        var payments = builder.Stream(PaymentsTopic, new StringSerDes(), new CamelJsonSerDes<Payment>());

        // 1. paid orders: a payment at most 30 minutes after (or before) its order
        orders.Join(payments,
                (order, payment) => new PaidOrder(order.OrderId, order.CustomerId, payment.Amount, payment.Method,
                    Math.Round((payment.PaidAt - order.OrderedAt).TotalMinutes, 1)),
                JoinWindowOptions.Of(PaymentDeadline),
                StreamJoinProps.With<string, Order, Payment>(new StringSerDes(), new CamelJsonSerDes<Order>(), new CamelJsonSerDes<Payment>()))
            .To(PaidTopic, new StringSerDes(), new CamelJsonSerDes<PaidOrder>());

        // 2. what a join can't tell: which orders were NOT paid in time, and which payments came too late.
        // Orders and payments go through one stateful transformer that remembers the pending orders.
        var alerts = orders.MapValues((order, _) => new OrderEvent(order, null))
            .Merge(payments.MapValues((payment, _) => new OrderEvent(null, payment)))
            .Transform(TransformerBuilder.New<string, OrderEvent, string, PaymentAlert>()
                .Transformer<PaymentDeadlineTracker>()
                .StateStore(Stores.KeyValueStoreBuilder(
                    Stores.InMemoryKeyValueStore(StateStore), new StringSerDes(), new CamelJsonSerDes<PendingOrder>()))
                .Build());

        var branches = alerts.Branch(
            (_, alert, _) => alert.Type == PaymentAlert.Unpaid,
            (_, alert, _) => alert.Type == PaymentAlert.LatePayment);
        branches[0].To(UnpaidTopic, new StringSerDes(), new CamelJsonSerDes<PaymentAlert>());
        branches[1].To(LatePaymentsTopic, new StringSerDes(), new CamelJsonSerDes<PaymentAlert>());
    }

    public void Configure(StreamConfig config)
    {
    }
}

public record PaidOrder(string OrderId, string CustomerId, decimal Amount, string Method, double MinutesToPay);

/// <summary>An order or a payment (exactly one is set).</summary>
public record OrderEvent(Order Order, Payment Payment);

/// <summary>State of an order waiting for its payment; Reported once it has been reported as unpaid.</summary>
public record PendingOrder(Order Order, bool Reported);

public record PaymentAlert(string Type, string OrderId, string CustomerId, DateTime OrderedAt, DateTime? PaidAt, decimal? Amount)
{
    public const string Unpaid = "unpaid";
    public const string LatePayment = "late-payment";
}

/// <summary>
/// Keeps each order until its payment arrives. Every minute of stream time, the orders older than 30 minutes are
/// reported as unpaid (once). A payment more than 30 minutes after its order is reported as late.
/// Decisions use the event times (orderedAt, paidAt), so the result doesn't depend on when punctuations run.
/// </summary>
public class PaymentDeadlineTracker : ITransformer<string, OrderEvent, string, PaymentAlert>
{
    private ProcessorContext<string, PaymentAlert> context;
    private IKeyValueStore<string, PendingOrder> pending;

    public void Init(ProcessorContext<string, PaymentAlert> context)
    {
        this.context = context;
        pending = (IKeyValueStore<string, PendingOrder>)context.GetStateStore(OrderPaymentMatching.StateStore);
        // STREAM_TIME: driven by the timestamps of the records, so it's deterministic (and works when replaying data)
        context.Schedule(TimeSpan.FromMinutes(1), PunctuationType.STREAM_TIME, ReportUnpaidOrders);
    }

    public Record<string, PaymentAlert> Process(Record<string, OrderEvent> record)
    {
        if (record.Value.Order != null)
        {
            pending.Put(record.Key, new PendingOrder(record.Value.Order, false));
            return null;
        }

        var payment = record.Value.Payment;
        var state = pending.Get(payment.OrderId);
        if (state == null)
            return null; // payment of an unknown order (or a duplicate): ignored here

        pending.Delete(payment.OrderId);
        if (payment.PaidAt - state.Order.OrderedAt <= OrderPaymentMatching.PaymentDeadline)
            return null; // paid in time: the join confirms it

        // late: report it unpaid first if the punctuation didn't do it yet, then the late payment
        if (!state.Reported)
            context.Forward(state.Order.OrderId, UnpaidAlert(state.Order));
        return Record<string, PaymentAlert>.Create(payment.OrderId,
            new PaymentAlert(PaymentAlert.LatePayment, payment.OrderId, state.Order.CustomerId, state.Order.OrderedAt,
                payment.PaidAt, payment.Amount));
    }

    private void ReportUnpaidOrders(long streamTime)
    {
        var now = DateTimeOffset.FromUnixTimeMilliseconds(streamTime).UtcDateTime;
        var deadline = now - OrderPaymentMatching.PaymentDeadline;
        foreach (var entry in pending.All().ToList())
        {
            var order = entry.Value.Order;
            var reported = entry.Value.Reported;
            if (!reported && order.OrderedAt < deadline)
            {
                context.Forward(entry.Key, UnpaidAlert(order));
                reported = true;
            }

            if (order.OrderedAt < now - OrderPaymentMatching.ForgetAfter)
                pending.Delete(entry.Key); // a payment this late is ignored: the store doesn't grow forever
            else if (reported != entry.Value.Reported)
                pending.Put(entry.Key, entry.Value with { Reported = true }); // kept to recognize a late payment
        }
    }

    private static PaymentAlert UnpaidAlert(Order order)
        => new(PaymentAlert.Unpaid, order.OrderId, order.CustomerId, order.OrderedAt, null, null);

    public void Close()
    {
    }
}
