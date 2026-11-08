using NUnit.Framework;
using Streamiz.Examples.DataGenerator;
using Streamiz.Examples.RealWorld.Examples;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;

namespace Streamiz.Examples.RealWorld.Tests;

/// <summary>5000 orders and 4737 payments: paid in time, paid late, never paid.</summary>
public class OrderPaymentMatchingTests
{
    private static readonly OrderPaymentMatching Example = new();
    private TopologyTestDriver driver;
    private Dictionary<string, Order> orders;
    private Dictionary<string, Payment> payments;

    [OneTimeSetUp]
    public void Run()
    {
        var records = Scenario.Load("ecommerce").Where(r => Example.InputTopics.Contains(r.Topic)).ToList();
        orders = records.Where(r => r.Topic == OrderPaymentMatching.OrdersTopic).Select(r => r.As<Order>()).ToDictionary(o => o.OrderId);
        payments = records.Where(r => r.Topic == OrderPaymentMatching.PaymentsTopic).Select(r => r.As<Payment>()).ToDictionary(p => p.OrderId);

        driver = Scenario.Create(Example);
        Scenario.Pipe(driver, records);

        // Unpaid orders are reported by a stream-time punctuation: stream time only moves with new records.
        // At the end of the data, one more order an hour later moves it past the deadline of the last orders.
        var end = records.Max(r => r.Timestamp).AddHours(1);
        Scenario.Pipe(driver, new[]
        {
            new DataRecord(OrderPaymentMatching.OrdersTopic, "O-END",
                $$"""{"orderId":"O-END","customerId":"C0001","productId":"P0001","quantity":1,"orderedAt":"{{end:yyyy-MM-ddTHH:mm:ssZ}}"}""", end)
        });
    }

    [OneTimeTearDown]
    public void Dispose() => driver.Dispose();

    private bool PaidInTime(Order order)
        => payments.TryGetValue(order.OrderId, out var p) && p.PaidAt - order.OrderedAt <= OrderPaymentMatching.PaymentDeadline;

    [Test]
    public void OrdersPaidWithin30MinutesAreMatched()
    {
        var paid = Scenario.ReadAll(driver, OrderPaymentMatching.PaidTopic, new StringSerDes(), new CamelJsonSerDes<PaidOrder>())
            .Select(r => r.Message.Value).ToList();

        var expected = orders.Values.Where(PaidInTime).Select(o => o.OrderId).ToList();
        CollectionAssert.AreEquivalent(expected, paid.Select(p => p.OrderId));
        Assert.AreEqual(Scenario.Facts("ecommerce").GetProperty("paymentsWithin30Minutes").GetInt32(), paid.Count); // 4495
        Assert.That(paid.Select(p => p.MinutesToPay), Is.All.InRange(0, 30));
        foreach (var p in paid)
            Assert.AreEqual(payments[p.OrderId].Amount, p.Amount);
    }

    [Test]
    public void OrdersNotPaidWithin30MinutesAreReportedOnce()
    {
        var unpaid = Scenario.ReadAll(driver, OrderPaymentMatching.UnpaidTopic, new StringSerDes(), new CamelJsonSerDes<PaymentAlert>())
            .Select(r => r.Message.Value).ToList();

        var expected = orders.Values.Where(o => !PaidInTime(o)).Select(o => o.OrderId).ToList();
        CollectionAssert.AreEquivalent(expected, unpaid.Select(a => a.OrderId)); // each one exactly once

        // documented facts: 263 never paid + 242 paid late
        var facts = Scenario.Facts("ecommerce");
        Assert.AreEqual(facts.GetProperty("unpaidOrders").GetInt32() + facts.GetProperty("latePayments").GetInt32(), unpaid.Count);
    }

    [Test]
    public void LatePaymentsAreReported()
    {
        var late = Scenario.ReadAll(driver, OrderPaymentMatching.LatePaymentsTopic, new StringSerDes(), new CamelJsonSerDes<PaymentAlert>())
            .Select(r => r.Message.Value).ToList();

        var expected = payments.Values.Where(p => p.PaidAt - orders[p.OrderId].OrderedAt > OrderPaymentMatching.PaymentDeadline)
            .Select(p => p.OrderId).ToList();
        CollectionAssert.AreEquivalent(expected, late.Select(a => a.OrderId));
        Assert.AreEqual(Scenario.Facts("ecommerce").GetProperty("latePayments").GetInt32(), late.Count); // 242
        foreach (var alert in late)
            Assert.AreEqual(payments[alert.OrderId].PaidAt, alert.PaidAt);
    }

    [Test]
    public void ReportedOrdersAreForgottenAfterADay()
    {
        var store = driver.GetKeyValueStore<string, PendingOrder>(OrderPaymentMatching.StateStore);
        var end = store.Get("O-END").Order.OrderedAt;

        // what's left: orders of the last day not paid in time (kept to recognize a late payment), and the last order
        var expected = orders.Values.Where(o => !payments.ContainsKey(o.OrderId) && o.OrderedAt >= end - OrderPaymentMatching.ForgetAfter)
            .Select(o => o.OrderId).Append("O-END");
        CollectionAssert.AreEquivalent(expected, store.All().Select(kv => kv.Key));
    }
}
