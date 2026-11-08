using Streamiz.Examples.Common;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Examples.RealWorld.Examples;

/// <summary>
/// E-commerce revenue (dataset: ecommerce). Each order is priced with the product price that was valid when it was
/// placed (versioned products table), enriched with its customer (global table), then aggregated: revenue per
/// category per hour, and revenue per customer.
/// </summary>
public class EcommerceRevenue : IExample
{
    public const string OrdersTopic = "ecommerce.orders";
    public const string ProductsTopic = "ecommerce.products";
    public const string CustomersTopic = "ecommerce.customers";
    public const string EnrichedOrdersTopic = "ecommerce.orders-enriched";
    public const string RevenueByCategoryTopic = "ecommerce.revenue-by-category-hourly";
    public const string RevenuePerCustomerStore = "revenue-per-customer";
    public const string RevenueByCategoryStore = "revenue-by-category-hourly";
    public static readonly TimeSpan Hour = TimeSpan.FromHours(1);

    public string Name => "ecommerce-revenue";

    public string Description => "Orders priced as of their time (versioned table), enriched with customers, revenue per category per hour and per customer.";

    public IEnumerable<string> InputTopics => new[] { OrdersTopic, ProductsTopic, CustomersTopic };

    public void BuildTopology(StreamBuilder builder)
    {
        // Products: a versioned table keeps 7 days of price history per product, so an order is priced
        // with the price valid at the order's time, even if a newer price was received before the order.
        // (the serdes are given explicitly: Streamiz doesn't read the topic with the serdes of the Materialized)
        var products = builder.Table(ProductsTopic, new StringSerDes(), new CamelJsonSerDes<Product>(),
            InMemoryVersioned.As<string, Product, StringSerDes, CamelJsonSerDes<Product>>("products-versioned", TimeSpan.FromDays(7)));

        // Customers: small reference data, replicated on every instance (global table): no repartition needed to join.
        var customers = builder.GlobalTable(CustomersTopic, new StringSerDes(), new CamelJsonSerDes<Customer>(),
            InMemory.As<string, Customer>("customers-store"));

        var enriched = builder.Stream(OrdersTopic, new StringSerDes(), new CamelJsonSerDes<Order>())
            // orders are keyed by order id: re-key them by product id to join with the products table
            // (Streamiz repartitions them through an internal topic)
            .SelectKey((_, order, _) => order.ProductId)
            .Join(products,
                (order, product) => new EnrichedOrder(order.OrderId, order.CustomerId, order.ProductId, product.Category,
                    order.Quantity, product.Price, product.Price * order.Quantity, order.OrderedAt),
                new StreamTableJoinProps<string, Order, Product>(
                    new StringSerDes(), new CamelJsonSerDes<Order>(), new CamelJsonSerDes<Product>()))
            // the customer id comes from the value: a global table can be joined on any field
            .Join(customers,
                (_, order) => order.CustomerId,
                (order, customer) => order with { Country = customer.Country, Tier = customer.Tier });

        // 1. the enriched orders, keyed by order id again
        enriched
            .SelectKey((_, order, _) => order.OrderId)
            .To(EnrichedOrdersTopic, new StringSerDes(), new CamelJsonSerDes<EnrichedOrder>());

        // 2. revenue per category per hour (tumbling windows of 1 hour, on the order's time)
        enriched
            .GroupBy<string, StringSerDes, CamelJsonSerDes<EnrichedOrder>>((_, order, _) => order.Category)
            .WindowedBy(TumblingWindowOptions.Of(Hour))
            .Aggregate(() => Revenue.Zero, (_, order, revenue) => revenue.Add(order.Amount),
                InMemoryWindows.As<string, Revenue>(RevenueByCategoryStore).WithValueSerdes(new CamelJsonSerDes<Revenue>()))
            .ToStream()
            .To(RevenueByCategoryTopic, new TimeWindowedSerDes<string>(new StringSerDes(), (long)Hour.TotalMilliseconds),
                new CamelJsonSerDes<Revenue>());

        // 3. revenue per customer, kept in a queryable store (e.g. for a "top customers" API)
        enriched
            .GroupBy<string, StringSerDes, CamelJsonSerDes<EnrichedOrder>>((_, order, _) => order.CustomerId)
            .Aggregate(() => Revenue.Zero, (_, order, revenue) => revenue.Add(order.Amount),
                InMemory.As<string, Revenue>(RevenuePerCustomerStore).WithValueSerdes(new CamelJsonSerDes<Revenue>()));
    }

    public void Configure(StreamConfig config)
    {
    }
}
