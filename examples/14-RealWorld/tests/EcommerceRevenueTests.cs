using NUnit.Framework;
using Streamiz.Examples.RealWorld.Examples;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;

namespace Streamiz.Examples.RealWorld.Tests;

/// <summary>The whole ecommerce dataset (500 customers, 200 products and their changes, 5000 orders) through the topology.</summary>
public class EcommerceRevenueTests
{
    private static readonly EcommerceRevenue Example = new();
    private TopologyTestDriver driver;
    private List<Product> productUpdates;
    private List<Customer> customerUpdates;
    private List<Order> orders;

    [OneTimeSetUp]
    public void Run()
    {
        var records = Scenario.Load("ecommerce").Where(r => Example.InputTopics.Contains(r.Topic)).ToList();
        productUpdates = records.Where(r => r.Topic == EcommerceRevenue.ProductsTopic).Select(r => r.As<Product>()).ToList();
        customerUpdates = records.Where(r => r.Topic == EcommerceRevenue.CustomersTopic).Select(r => r.As<Customer>()).ToList();
        orders = records.Where(r => r.Topic == EcommerceRevenue.OrdersTopic).Select(r => r.As<Order>()).ToList();

        driver = Scenario.Create(Example);
        Scenario.Pipe(driver, records);
    }

    [OneTimeTearDown]
    public void Dispose() => driver.Dispose();

    /// <summary>The independent computation: the latest version with a time at or before the order's.</summary>
    private Product PriceAt(string productId, DateTime at)
        => productUpdates.Where(p => p.ProductId == productId && p.UpdatedAt <= at).MaxBy(p => p.UpdatedAt);

    private Customer CustomerAt(string customerId, DateTime at)
        => customerUpdates.Where(c => c.CustomerId == customerId && c.UpdatedAt <= at).MaxBy(c => c.UpdatedAt);

    private List<EnrichedOrder> Enriched()
        => Scenario.ReadAll(driver, EcommerceRevenue.EnrichedOrdersTopic, new StringSerDes(), new CamelJsonSerDes<EnrichedOrder>())
            .Select(r => r.Message.Value).ToList();

    [Test]
    public void EveryOrderIsPricedAtTheTimeOfTheOrder()
    {
        var enriched = Enriched().ToDictionary(o => o.OrderId);

        Assert.AreEqual(orders.Count, enriched.Count);
        foreach (var order in orders)
        {
            var result = enriched[order.OrderId];
            var product = PriceAt(order.ProductId, order.OrderedAt);
            var customer = CustomerAt(order.CustomerId, order.OrderedAt);
            Assert.AreEqual(product.Price, result.UnitPrice, order.OrderId);
            Assert.AreEqual(product.Price * order.Quantity, result.Amount, order.OrderId);
            Assert.AreEqual(product.Category, result.Category, order.OrderId);
            Assert.AreEqual(customer.Country, result.Country, order.OrderId);
            Assert.AreEqual(customer.Tier, result.Tier, order.OrderId);
        }
    }

    [Test]
    public void ThePriceCutOfTheFeaturedProductAppliesFromItsTimeOn()
    {
        // documented facts: P0007 costs 193.02 until 2026-01-06T12:00Z, then 144.76; 41 orders before, 47 after
        var featured = Enriched().Where(o => o.ProductId == "P0007").ToList();
        var changedAt = new DateTime(2026, 1, 6, 12, 0, 0, DateTimeKind.Utc);

        var before = featured.Where(o => o.OrderedAt < changedAt).ToList();
        var after = featured.Where(o => o.OrderedAt >= changedAt).ToList();
        Assert.AreEqual(41, before.Count);
        Assert.AreEqual(47, after.Count);
        Assert.That(before.Select(o => o.UnitPrice), Is.All.EqualTo(193.02m));
        Assert.That(after.Select(o => o.UnitPrice), Is.All.EqualTo(144.76m));
    }

    [Test]
    public void RevenuePerCategoryAndHourMatchesTheOrders()
    {
        var expected = orders
            .GroupBy(o => (PriceAt(o.ProductId, o.OrderedAt).Category, Scenario.Floor(o.OrderedAt, EcommerceRevenue.Hour)))
            .ToDictionary(g => g.Key, g => new Revenue(g.Sum(o => PriceAt(o.ProductId, o.OrderedAt).Price * o.Quantity), g.Count()));

        var actual = Scenario.FinalPerWindow(Scenario.ReadAll(driver, EcommerceRevenue.RevenueByCategoryTopic,
            new TimeWindowedSerDes<string>(new StringSerDes(), (long)EcommerceRevenue.Hour.TotalMilliseconds), new CamelJsonSerDes<Revenue>()));

        Scenario.AssertSame(expected, actual);

        // the busiest hour of the day (documented fact: 18:00 UTC)
        var busiestHour = actual.GroupBy(w => w.Key.Start.Hour).MaxBy(g => g.Sum(w => w.Value.Orders))!.Key;
        Assert.AreEqual(Scenario.Facts("ecommerce").GetProperty("busiestHourUtc").GetInt32(), busiestHour);
    }

    [Test]
    public void RevenuePerCustomerIsQueryable()
    {
        var store = driver.GetKeyValueStore<string, Revenue>(EcommerceRevenue.RevenuePerCustomerStore);
        var expected = orders.GroupBy(o => o.CustomerId)
            .ToDictionary(g => g.Key, g => new Revenue(g.Sum(o => PriceAt(o.ProductId, o.OrderedAt).Price * o.Quantity), g.Count()));

        var actual = store.All().ToDictionary(kv => kv.Key, kv => kv.Value);
        Scenario.AssertSame(expected, actual);

        // the documented top customers by *paid* amount: ordered amounts include the unpaid orders, so they're at least as high
        foreach (var top in Scenario.Facts("ecommerce").GetProperty("topCustomersByPaidAmount").EnumerateArray())
            Assert.GreaterOrEqual(store.Get(top.GetProperty("customerId").GetString()).Amount, top.GetProperty("revenue").GetDecimal());
    }
}
