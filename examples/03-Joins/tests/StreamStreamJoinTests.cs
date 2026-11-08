using NUnit.Framework;
using Streamiz.Examples.Joins.Examples;
using static Streamiz.Examples.Joins.Tests.ExampleDriver;

namespace Streamiz.Examples.Joins.Tests;

public class OrdersPaymentsJoinTests
{
    [Test]
    public void OrderIsPaidWhenThePaymentArrivesWithinTenMinutes()
    {
        using var driver = Create(new OrdersPaymentsJoinExample());
        var orders = driver.CreateInputTopic<string, string>(OrdersPaymentsJoinExample.OrdersTopic);
        var payments = driver.CreateInputTopic<string, string>(OrdersPaymentsJoinExample.PaymentsTopic);
        var output = driver.CreateOutputTopic<string, string>(OrdersPaymentsJoinExample.OutputTopic);

        orders.PipeInput("o1", "120.50", T0);
        payments.PipeInput("o1", "card", T0.AddMinutes(5));      // 5 minutes later: joined

        orders.PipeInput("o2", "35.00", T0);
        payments.PipeInput("o2", "paypal", T0.AddMinutes(15));   // 15 minutes later: outside the window

        CollectionAssert.AreEqual(new[]
        {
            ("o1", "amount=120.50 paid-by=card")
        }, output.ReadAll());
    }
}

public class OrdersPaymentsLeftJoinTests
{
    [Test]
    public void EveryOrderGetsAStatusAndIsUpdatedWhenPaid()
    {
        using var driver = Create(new OrdersPaymentsLeftJoinExample());
        var orders = driver.CreateInputTopic<string, string>(OrdersPaymentsLeftJoinExample.OrdersTopic);
        var payments = driver.CreateInputTopic<string, string>(OrdersPaymentsLeftJoinExample.PaymentsTopic);
        var output = driver.CreateOutputTopic<string, string>(OrdersPaymentsLeftJoinExample.OutputTopic);

        orders.PipeInput("o1", "120.50", T0);
        orders.PipeInput("o2", "35.00", T0);
        payments.PipeInput("o1", "card", T0.AddMinutes(5));

        CollectionAssert.AreEqual(new[]
        {
            ("o1", "amount=120.50 payment=pending"), // emitted as soon as the order arrives
            ("o2", "amount=35.00 payment=pending"),
            ("o1", "amount=120.50 paid-by=card")     // emitted again when its payment arrives
        }, output.ReadAll());
    }
}

public class OrdersPaymentsOuterJoinTests
{
    [Test]
    public void OrdersAndPaymentsWithoutMatchAreReported()
    {
        using var driver = Create(new OrdersPaymentsOuterJoinExample());
        var orders = driver.CreateInputTopic<string, string>(OrdersPaymentsOuterJoinExample.OrdersTopic);
        var payments = driver.CreateInputTopic<string, string>(OrdersPaymentsOuterJoinExample.PaymentsTopic);
        var output = driver.CreateOutputTopic<string, string>(OrdersPaymentsOuterJoinExample.OutputTopic);

        orders.PipeInput("o1", "120.50", T0);
        payments.PipeInput("o1", "card", T0.AddMinutes(5));
        payments.PipeInput("o3", "paypal", T0.AddMinutes(6));  // no order o3

        CollectionAssert.AreEqual(new[]
        {
            ("o1", "order of 120.50 without payment"),
            ("o1", "order of 120.50 paid by card"),
            ("o3", "payment by paypal without order")
        }, output.ReadAll());
    }
}
