using Streamiz.Examples.Common;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;

namespace Streamiz.Examples.Basics;

/// <summary>
/// FlatMap turns one record into zero, one or several records, with new keys and values:
/// an order with several lines becomes one record per product.
/// </summary>
public class FlatMapOrderLines : IExample
{
    public const string InputTopic = "multi-line-orders";
    public const string OutputTopic = "product-demand";

    public string Name => "flatmap-order-lines";
    public string Description => "FlatMap one order with several lines into one record per product.";
    public IEnumerable<string> InputTopics => new[] { InputTopic };

    public void BuildTopology(StreamBuilder builder)
    {
        builder.Stream(InputTopic, new StringSerDes(), new JsonSerDes<MultiLineOrder>())
            // FlatMap returns any number of key/value pairs for each input record:
            // here one pair (product, quantity) per line. An order without lines produces nothing.
            .FlatMap((orderId, order, _) => order.Lines
                .Select(line => KeyValuePair.Create(line.Product, line.Quantity)))
            .To(OutputTopic, new StringSerDes(), new Int32SerDes());
    }

    public void Configure(StreamConfig config)
    {
    }
}
