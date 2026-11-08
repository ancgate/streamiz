using Streamiz.Examples.Common;
using Streamiz.Kafka.Net;

namespace Streamiz.Examples.ProcessingGuarantees.Examples;

/// <summary>
/// At-most-once: the offsets are committed right after each poll, before the records are processed, and the
/// producer doesn't retry. A record is never processed twice, but the records being processed when the
/// application crashes are lost. For outputs where a duplicate is worse than a gap: push notifications,
/// SMS, sampled metrics.
/// </summary>
public class AtMostOnceExample : IExample
{
    public string Name => "at-most-once";
    public string Description => "Never sends a notification twice; one can be lost on a crash.";
    public IEnumerable<string> InputTopics => new[] { "order-shipped" };

    public void BuildTopology(StreamBuilder builder)
    {
        builder
            // key: customer id, value: order id
            .Stream<string, string>("order-shipped")
            .MapValues((customer, order, _) => $"Your order {order} is on its way!")
            // a notification service sends each record of this topic to the customer's phone
            .To("push-notifications");
    }

    public void Configure(StreamConfig config)
    {
        // also disables idempotence and sets MessageSendMaxRetries = 0 (a producer retry could write twice)
        config.Guarantee = ProcessingGuarantee.AT_MOST_ONCE;
    }
}
