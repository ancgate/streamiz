using Streamiz.Examples.Common;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;

namespace Streamiz.Examples.Serdes.Examples;

/// <summary>
/// <see cref="NullableInt64SerDes"/>: a <c>long?</c> value where null is a real Kafka null (a tombstone).
/// <see cref="Int64SerDes"/> can't represent null: it would write 8 zero bytes.
/// </summary>
public class NullableSerdesExample : IExample
{
    public string Name => "nullable-serdes";

    public string Description => "Splits stock levels (long?, null = discontinued) into restock requests and discontinued products.";

    public IEnumerable<string> InputTopics => new[] { "stock-levels" };

    public void BuildTopology(StreamBuilder builder)
    {
        // key: product SKU, value: units in stock, or null when the product is discontinued
        var stock = builder.Stream<string, long?, StringSerDes, NullableInt64SerDes>("stock-levels");

        // low stock -> restock request (the value is never null here, so the plain Int64 serdes is enough)
        stock.Filter((_, units, _) => units is < 5)
            .MapValues((units, _) => 20L - units.Value)
            .To<StringSerDes, Int64SerDes>("restock-requests");

        // discontinued -> forwarded with a null value, which NullableInt64SerDes writes as a Kafka tombstone
        stock.Filter((_, units, _) => units == null)
            .To<StringSerDes, NullableInt64SerDes>("discontinued-products");
    }

    public void Configure(StreamConfig config)
    {
    }
}
