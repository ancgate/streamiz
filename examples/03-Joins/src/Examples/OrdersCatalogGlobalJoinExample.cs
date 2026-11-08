using System.Globalization;
using Streamiz.Examples.Common;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Examples.Joins.Examples;

/// <summary>
/// Stream-global table join: enrich orders with the product catalog, looked up by a field of the order.
/// </summary>
public class OrdersCatalogGlobalJoinExample : IExample
{
    public const string CatalogTopic = "product-catalog";
    public const string OrdersTopic = "orders";
    public const string OutputTopic = "orders-with-product";

    public string Name => "orders-catalog-global-join";

    public string Description => "Stream-global table join: enrich orders with the product catalog, looked up by the order's product id.";

    public IEnumerable<string> InputTopics => new[] { CatalogTopic, OrdersTopic };

    public void BuildTopology(StreamBuilder builder)
    {
        // A global table is fully replicated on every instance (all partitions of the topic), so the
        // stream doesn't need to be co-partitioned with it, nor keyed by the lookup key.
        // Catalog: product id -> "name;price".
        var catalog = builder.GlobalTable(CatalogTopic, InMemory.As<string, string>("product-catalog-store"));

        // Orders are keyed by order id, value "productId;quantity".
        builder.Stream<string, string>(OrdersTopic)
            .Join(catalog,
                // Key mapper: which global table key to look up for this order.
                (orderId, order) => order.Split(';')[0],
                // Joiner: order value + catalog value.
                (order, product) =>
                {
                    var quantity = int.Parse(order.Split(';')[1]);
                    var parts = product.Split(';');
                    var price = decimal.Parse(parts[1], CultureInfo.InvariantCulture);
                    return $"{quantity} x {parts[0]} = {(quantity * price).ToString("0.00", CultureInfo.InvariantCulture)}";
                })
            .To(OutputTopic);
    }

    public void Configure(StreamConfig config)
    {
    }
}
