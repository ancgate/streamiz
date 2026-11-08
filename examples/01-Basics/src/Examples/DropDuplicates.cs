using Streamiz.Examples.Common;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Examples.Basics;

/// <summary>
/// DropDuplicate removes the records of a key that are equal to a record of the same key seen shortly before
/// or after: for example a payment sent twice by a client that retried.
/// </summary>
public class DropDuplicates : IExample
{
    public const string InputTopic = "payment-requests";
    public const string OutputTopic = "payment-requests-deduplicated";
    public const string StoreName = "payment-dedup-store";

    public string Name => "drop-duplicates";
    public string Description => "Drop payments resent with the same id and content within a few minutes.";
    public IEnumerable<string> InputTopics => new[] { InputTopic };

    public void BuildTopology(StreamBuilder builder)
    {
        builder.Stream(InputTopic, new StringSerDes(), new JsonSerDes<Payment>())
            // Records are compared within the interval, centered on the record: with 10 minutes, a payment is
            // a duplicate if the same payment id was seen less than 5 minutes before (or after) it, and the
            // comparer says both values are equal. The seen records are kept in a window store.
            .DropDuplicate(
                (paymentId, previous, current) => previous.Amount == current.Amount && previous.Method == current.Method,
                TimeSpan.FromMinutes(10),
                InMemoryWindows.As<string, Payment>(StoreName).WithValueSerdes(new JsonSerDes<Payment>()))
            .To(OutputTopic, new StringSerDes(), new JsonSerDes<Payment>());
    }

    public void Configure(StreamConfig config)
    {
    }
}
