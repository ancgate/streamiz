using Streamiz.Examples.Common;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.Processors.Public;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;

namespace Streamiz.Examples.ProcessorApi.Examples;

/// <summary>
/// A stateful transformer with its own state store: drop events already seen in the last 10 minutes.
/// </summary>
public class DeduplicateEventsExample : IExample
{
    public const string EventsTopic = "payment-events";
    public const string OutputTopic = "payment-events-unique";
    public const string StoreName = "seen-events";

    public string Name => "deduplicate-events";

    public string Description => "Stateful transformer with a state store: drop events whose id was already seen in the last 10 minutes.";

    public IEnumerable<string> InputTopics => new[] { EventsTopic };

    public void BuildTopology(StreamBuilder builder)
    {
        builder.Stream<string, string>(EventsTopic)
            .Transform(TransformerBuilder.New<string, string, string, string>()
                .Transformer<DeduplicationTransformer>()
                // The store is created with the transformer and connected to it. It is backed by a changelog topic,
                // so the "already seen" ids survive a restart or a rebalance.
                .StateStore(Stores.KeyValueStoreBuilder(
                    Stores.InMemoryKeyValueStore(StoreName),
                    new StringSerDes(),
                    new Int64SerDes()))
                .Build())
            .To(OutputTopic);
    }

    public void Configure(StreamConfig config)
    {
    }
}

/// <summary>
/// Events are keyed by event id. The store keeps, for each id, the timestamp of the last time it was forwarded.
/// </summary>
public class DeduplicationTransformer : ITransformer<string, string, string, string>
{
    private static readonly long WindowMs = (long)TimeSpan.FromMinutes(10).TotalMilliseconds;
    private IKeyValueStore<string, long> seen;

    public void Init(ProcessorContext<string, string> context)
    {
        // The store is looked up by name: it must be connected to this transformer (see StateStore(...)).
        seen = (IKeyValueStore<string, long>)context.GetStateStore(DeduplicateEventsExample.StoreName);
    }

    public Record<string, string> Process(Record<string, string> record)
    {
        long timestamp = record.Timestamp.UnixTimestampMs;
        long lastSeen = seen.Get(record.Key); // 0 when the id was never seen

        if (lastSeen != 0 && timestamp - lastSeen < WindowMs)
            return null; // returning null drops the record

        seen.Put(record.Key, timestamp);
        return record;
    }

    public void Close()
    {
    }
}
