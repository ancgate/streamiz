using Streamiz.Examples.Common;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Examples.Windowing;

/// <summary>
/// Session Aggregate: an aggregator adds each record to its session, and a merger combines two sessions when
/// a record bridges them. Here: the items and total of a cart per shopping session.
/// </summary>
public class SessionCartTotal : IExample
{
    public const string InputTopic = "cart-additions";
    public const string OutputTopic = "cart-per-session";
    public const string StoreName = "cart-sessions-store";
    public static readonly TimeSpan InactivityGap = TimeSpan.FromMinutes(10);

    public string Name => "session-cart-total";
    public string Description => "Items and total of each user's cart per shopping session (session Aggregate with a merger).";
    public IEnumerable<string> InputTopics => new[] { InputTopic };

    public void BuildTopology(StreamBuilder builder)
    {
        // Cart additions: key = user, value = the item added.
        builder.Stream(InputTopic, new StringSerDes(), new JsonSerDes<CartItem>())
            .GroupByKey()
            .WindowedBy(SessionWindowOptions.Of(InactivityGap))
            .Aggregate(
                // initializer: an empty cart for a new session
                () => new CartSummary(),
                // aggregator: add the item to the session's cart
                (user, item, cart) => new CartSummary
                {
                    Items = cart.Items.Append(item.Product).ToList(),
                    Total = cart.Total + item.Price
                },
                // merger: when two sessions become one, combine their carts
                (user, cart1, cart2) => new CartSummary
                {
                    Items = cart1.Items.Concat(cart2.Items).ToList(),
                    Total = cart1.Total + cart2.Total
                },
                InMemorySessions.As<string, CartSummary>(StoreName).WithValueSerdes(new JsonSerDes<CartSummary>()))
            .ToStream()
            // A merged session's old sessions are emitted with a null cart (tombstones): use a reference type
            // for the aggregate so that tombstones stay null.
            .To(OutputTopic, new SessionWindowedSerDes<string>(new StringSerDes()), new JsonSerDes<CartSummary>());
    }

    public void Configure(StreamConfig config)
    {
    }
}
