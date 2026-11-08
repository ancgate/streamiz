using Streamiz.Examples.Common;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Examples.Joins.Examples;

/// <summary>
/// Foreign-key table join: each order row is joined with the product it references.
/// </summary>
public class OrdersProductsForeignKeyJoinExample : IExample
{
    public const string OrdersTopic = "orders-table";
    public const string ProductsTopic = "products";
    public const string OutputTopic = "orders-enriched";

    public string Name => "orders-products-fk-join";

    public string Description => "Foreign-key table join: join each order (keyed by order id) with the product its value references.";

    public IEnumerable<string> InputTopics => new[] { OrdersTopic, ProductsTopic };

    public void BuildTopology(StreamBuilder builder)
    {
        // Orders: order id -> "productId;quantity". Products: product id -> product name.
        // The two tables have different keys, so a regular table-table join isn't possible.
        var orders = builder.Table(OrdersTopic, InMemory.As<string, string>("orders-store"));
        var products = builder.Table(ProductsTopic, InMemory.As<string, string>("products-store"));

        orders.Join(
                products,
                // Foreign key extractor: the product id stored in the order value.
                order => order.Split(';')[0],
                // Joiner: order value + product value. The result is keyed by order id.
                (order, productName) => $"{order.Split(';')[1]} x {productName}")
            // The result updates when an order changes (even to another product)
            // and when a product changes (all the orders referencing it).
            .ToStream()
            .To(OutputTopic);
    }

    public void Configure(StreamConfig config)
    {
    }
}

/// <summary>
/// Foreign-key table left join: orders referencing an unknown product are still emitted.
/// </summary>
public class OrdersProductsForeignKeyLeftJoinExample : IExample
{
    public const string OrdersTopic = "orders-table";
    public const string ProductsTopic = "products";
    public const string OutputTopic = "orders-enriched";

    public string Name => "orders-products-fk-left-join";

    public string Description => "Foreign-key table left join: orders referencing a product that doesn't exist yet are emitted too.";

    public IEnumerable<string> InputTopics => new[] { OrdersTopic, ProductsTopic };

    public void BuildTopology(StreamBuilder builder)
    {
        var orders = builder.Table(OrdersTopic, InMemory.As<string, string>("orders-store"));
        var products = builder.Table(ProductsTopic, InMemory.As<string, string>("products-store"));

        // LeftJoin: the joiner gets a null product when the referenced product isn't known (yet).
        orders.LeftJoin(
                products,
                order => order.Split(';')[0],
                (order, productName) => $"{order.Split(';')[1]} x {productName ?? "unknown product"}")
            .ToStream()
            .To(OutputTopic);
    }

    public void Configure(StreamConfig config)
    {
    }
}
