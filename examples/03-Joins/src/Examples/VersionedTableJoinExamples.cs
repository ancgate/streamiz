using Streamiz.Examples.Common;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Examples.Joins.Examples;

/// <summary>
/// Stream-table join on a versioned table: each order gets the price that was valid at the order's time,
/// even when a newer price was received before the order.
/// </summary>
public class PriceAsOfOrderTimeExample : IExample
{
    public const string PricesTopic = "prices";
    public const string OrdersTopic = "orders-by-product";
    public const string OutputTopic = "priced-orders";

    public string Name => "price-as-of-order-time";

    public string Description => "Versioned table join: price each order with the price valid at the order's timestamp, not the latest one.";

    public IEnumerable<string> InputTopics => new[] { PricesTopic, OrdersTopic };

    public void BuildTopology(StreamBuilder builder)
    {
        // A versioned store keeps the history of each key for 1 day (the history retention),
        // so it can answer "what was the price of this product at time t?".
        var prices = builder.Table(PricesTopic, InMemoryVersioned.As<string, string>("prices-store", TimeSpan.FromDays(1)));

        // Orders are keyed by product id, value "orderId;quantity".
        // With a versioned table, the join reads the price as of the order's timestamp (KIP-914):
        // an order placed before a price change keeps the old price, even if it's processed after the change.
        builder.Stream<string, string>(OrdersTopic)
            .Join(prices, (order, price) =>
            {
                var parts = order.Split(';');
                return $"{parts[0]}: {parts[1]} x {price}";
            })
            .To(OutputTopic);
    }

    public void Configure(StreamConfig config)
    {
    }
}

/// <summary>
/// Stream-table join with a grace period: orders wait up to 1 minute (of stream time) before being joined,
/// so price updates that arrive late (out of order) are still taken into account.
/// </summary>
public class PriceJoinWithGraceExample : IExample
{
    public const string PricesTopic = "prices";
    public const string OrdersTopic = "orders-by-product";
    public const string OutputTopic = "priced-orders";

    public string Name => "price-join-with-grace";

    public string Description => "Stream-table join with a grace period (KIP-923): delay orders 1 minute so late price updates are used.";

    public IEnumerable<string> InputTopics => new[] { PricesTopic, OrdersTopic };

    public void BuildTopology(StreamBuilder builder)
    {
        // A grace period requires a versioned table, with a history retention at least as long as the grace period.
        var prices = builder.Table(PricesTopic, InMemoryVersioned.As<string, string>("prices-store", TimeSpan.FromDays(1)));

        // The join props carry the serdes (used by the buffer of waiting orders) and the grace period.
        var joinProps = new StreamTableJoinProps<string, string, string>(
                new StringSerDes(), new StringSerDes(), new StringSerDes())
            .WithGracePeriod(TimeSpan.FromMinutes(1));

        // Each order is buffered until the stream time (the highest order timestamp seen) is 1 minute past it,
        // then joined with the price valid at the order's timestamp. A price update that arrives late,
        // but within the grace period, is therefore used. Orders older than the grace period are joined right away.
        builder.Stream<string, string>(OrdersTopic)
            .Join(prices, (order, price) =>
            {
                var parts = order.Split(';');
                return $"{parts[0]}: {parts[1]} x {price}";
            }, joinProps)
            .To(OutputTopic);
    }

    public void Configure(StreamConfig config)
    {
    }
}

/// <summary>
/// Out-of-order updates of a versioned table: they are kept in the history and emitted by ToStream(),
/// but joins ignore them, because they aren't the latest value of their key.
/// </summary>
public class VersionedTableOutOfOrderExample : IExample
{
    public const string PricesTopic = "prices";
    public const string ProductNamesTopic = "product-names";
    public const string PriceChangesTopic = "price-changes";
    public const string ProductPricesTopic = "product-prices";

    public string Name => "versioned-table-out-of-order";

    public string Description => "Versioned table: an out-of-order price update reaches ToStream() but doesn't change a table-table join.";

    public IEnumerable<string> InputTopics => new[] { PricesTopic, ProductNamesTopic };

    public void BuildTopology(StreamBuilder builder)
    {
        var prices = builder.Table(PricesTopic, InMemoryVersioned.As<string, string>("prices-store", TimeSpan.FromDays(1)));
        var names = builder.Table(ProductNamesTopic, InMemory.As<string, string>("product-names-store"));

        // Every update of the versioned table is emitted, including the out-of-order ones
        // (an update older than the latest value of its key, stored in the history).
        prices.ToStream().To(PriceChangesTopic);

        // A table-table join only applies the latest value of each key:
        // an out-of-order update doesn't change the joined result.
        prices.Join(names, (price, name) => $"{name}: {price}")
            .ToStream()
            .To(ProductPricesTopic);
    }

    public void Configure(StreamConfig config)
    {
    }
}
