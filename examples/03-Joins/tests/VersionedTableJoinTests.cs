using NUnit.Framework;
using Streamiz.Examples.Joins.Examples;
using static Streamiz.Examples.Joins.Tests.ExampleDriver;

namespace Streamiz.Examples.Joins.Tests;

public class PriceAsOfOrderTimeTests
{
    [Test]
    public void OrdersUseThePriceValidAtTheirTimestamp()
    {
        using var driver = Create(new PriceAsOfOrderTimeExample());
        var prices = driver.CreateInputTopic<string, string>(PriceAsOfOrderTimeExample.PricesTopic);
        var orders = driver.CreateInputTopic<string, string>(PriceAsOfOrderTimeExample.OrdersTopic);
        var output = driver.CreateOutputTopic<string, string>(PriceAsOfOrderTimeExample.OutputTopic);

        prices.PipeInput("p1", "10.00", T0);
        prices.PipeInput("p1", "12.00", T0.AddSeconds(60));   // price change at T0 + 60s

        orders.PipeInput("p1", "o1;2", T0.AddSeconds(30));    // placed before the change, processed after it
        orders.PipeInput("p1", "o2;1", T0.AddSeconds(90));    // placed after the change

        CollectionAssert.AreEqual(new[]
        {
            ("p1", "o1: 2 x 10.00"),
            ("p1", "o2: 1 x 12.00")
        }, output.ReadAll());
    }
}

public class PriceJoinWithGraceTests
{
    [Test]
    public void ALatePriceUpdateIsUsedForOrdersStillInTheGracePeriod()
    {
        using var driver = Create(new PriceJoinWithGraceExample());
        var prices = driver.CreateInputTopic<string, string>(PriceJoinWithGraceExample.PricesTopic);
        var orders = driver.CreateInputTopic<string, string>(PriceJoinWithGraceExample.OrdersTopic);
        var output = driver.CreateOutputTopic<string, string>(PriceJoinWithGraceExample.OutputTopic);

        prices.PipeInput("p1", "10.00", T0);
        orders.PipeInput("p1", "o1;2", T0.AddSeconds(60));    // buffered: stream time 60s, deadline 0s
        Assert.IsEmpty(output.ReadAll());

        prices.PipeInput("p1", "11.00", T0.AddSeconds(30));   // late price update, valid from 30s

        orders.PipeInput("p1", "o2;1", T0.AddSeconds(150));   // stream time 150s: o1 (60s) is past the 90s deadline

        CollectionAssert.AreEqual(new[]
        {
            ("p1", "o1: 2 x 11.00")   // the late update is used: without grace, o1 would have been priced 10.00
        }, output.ReadAll());
    }
}

public class VersionedTableOutOfOrderTests
{
    [Test]
    public void AnOutOfOrderUpdateIsEmittedButNotJoined()
    {
        using var driver = Create(new VersionedTableOutOfOrderExample());
        var prices = driver.CreateInputTopic<string, string>(VersionedTableOutOfOrderExample.PricesTopic);
        var names = driver.CreateInputTopic<string, string>(VersionedTableOutOfOrderExample.ProductNamesTopic);
        var priceChanges = driver.CreateOutputTopic<string, string>(VersionedTableOutOfOrderExample.PriceChangesTopic);
        var productPrices = driver.CreateOutputTopic<string, string>(VersionedTableOutOfOrderExample.ProductPricesTopic);

        names.PipeInput("p1", "Laptop", T0);
        prices.PipeInput("p1", "999", T0.AddMinutes(10));
        prices.PipeInput("p1", "899", T0.AddMinutes(5));  // older than the latest price: out of order

        CollectionAssert.AreEqual(new[]
        {
            ("p1", "999"),
            ("p1", "899")   // ToStream() emits the out-of-order update too
        }, priceChanges.ReadAll());

        CollectionAssert.AreEqual(new[]
        {
            ("p1", "Laptop: 999")   // the join ignores it: the latest price is still 999
        }, productPrices.ReadAll());
    }
}
