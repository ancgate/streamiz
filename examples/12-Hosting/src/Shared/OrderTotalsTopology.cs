using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Examples.Hosting;

/// <summary>
/// The topology, registered in dependency injection: its dependencies (here the price catalog) are injected,
/// and it can be unit tested without the host.
/// </summary>
public class OrderTotalsTopology
{
    public const string InputTopic = "hosted-orders";
    public const string OutputTopic = "order-totals";
    public const string StoreName = "order-totals-store";

    private readonly IPriceCatalog catalog;

    public OrderTotalsTopology(IPriceCatalog catalog) => this.catalog = catalog;

    public Topology Build()
    {
        var builder = new StreamBuilder();

        // key: customer, value: "sku:quantity"
        builder.Stream<string, string>(InputTopic)
            .MapValues((line, _) =>
            {
                var parts = line.Split(':');
                return catalog.PriceOf(parts[0]) * int.Parse(parts[1]);
            })
            .GroupByKey()
            // total spent per customer, queryable from the web API (see the WebApi project)
            .Aggregate(() => 0.0, (_, amount, total) => total + amount,
                InMemory.As<string, double, StringSerDes, DoubleSerDes>(StoreName))
            .ToStream()
            .To<StringSerDes, DoubleSerDes>(OutputTopic);

        return builder.Build();
    }
}
