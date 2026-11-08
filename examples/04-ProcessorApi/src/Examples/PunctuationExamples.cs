using Streamiz.Examples.Common;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.Processors.Public;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;

namespace Streamiz.Examples.ProcessorApi.Examples;

/// <summary>
/// Stream-time punctuation: count readings per sensor and emit the counts every minute of event time.
/// </summary>
public class CountPerMinuteExample : IExample
{
    public const string ReadingsTopic = "sensor-readings";
    public const string OutputTopic = "readings-per-minute";
    public const string StoreName = "minute-counts";

    public string Name => "count-per-minute";

    public string Description => "Stream-time punctuation: count the readings of each sensor and emit the counts every minute of event time.";

    public IEnumerable<string> InputTopics => new[] { ReadingsTopic };

    public void BuildTopology(StreamBuilder builder)
    {
        builder.Stream<string, string>(ReadingsTopic)
            .Transform(TransformerBuilder.New<string, string, string, string>()
                .Transformer<MinuteCountTransformer>()
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
/// Counts readings in a store and, on each stream-time punctuation, forwards and resets the counts.
/// </summary>
public class MinuteCountTransformer : ITransformer<string, string, string, string>
{
    private IKeyValueStore<string, long> counts;
    private ProcessorContext<string, string> context;

    public void Init(ProcessorContext<string, string> context)
    {
        this.context = context;
        counts = (IKeyValueStore<string, long>)context.GetStateStore(CountPerMinuteExample.StoreName);

        // STREAM_TIME: the punctuation is driven by the record timestamps, not by the clock.
        // It runs when the stream time (the highest timestamp seen) has moved forward by one minute since the last run.
        // No new records means no punctuation.
        context.Schedule(TimeSpan.FromMinutes(1), PunctuationType.STREAM_TIME, EmitCounts);
    }

    public Record<string, string> Process(Record<string, string> record)
    {
        counts.Put(record.Key, counts.Get(record.Key) + 1);
        return null; // nothing is emitted per record: the counts are emitted by the punctuation
    }

    private void EmitCounts(long timestamp)
    {
        // The punctuator receives the current stream time. Records forwarded here get that timestamp.
        foreach (var count in counts.All().ToList())
        {
            context.Forward(count.Key, count.Value.ToString());
            counts.Delete(count.Key);
        }
    }

    public void Close()
    {
    }
}

/// <summary>
/// Wall-clock punctuation: alert when a sensor has been silent for 30 seconds, even if no record arrives.
/// </summary>
public class SilentSensorAlertExample : IExample
{
    public const string HeartbeatsTopic = "sensor-heartbeats";
    public const string OutputTopic = "sensor-alerts";
    public const string StoreName = "last-heartbeat";

    public string Name => "silent-sensor-alert";

    public string Description => "Wall-clock punctuation: every 10 seconds, alert on sensors that sent no heartbeat for 30 seconds.";

    public IEnumerable<string> InputTopics => new[] { HeartbeatsTopic };

    public void BuildTopology(StreamBuilder builder)
    {
        builder.Stream<string, string>(HeartbeatsTopic)
            .Transform(TransformerBuilder.New<string, string, string, string>()
                .Transformer<SilentSensorTransformer>()
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
/// Remembers when each sensor was last heard of (wall-clock time), and checks every 10 seconds of wall-clock time.
/// </summary>
public class SilentSensorTransformer : ITransformer<string, string, string, string>
{
    private static readonly long SilenceMs = (long)TimeSpan.FromSeconds(30).TotalMilliseconds;
    private IKeyValueStore<string, long> lastHeartbeat;
    private ProcessorContext<string, string> context;

    public void Init(ProcessorContext<string, string> context)
    {
        this.context = context;
        lastHeartbeat = (IKeyValueStore<string, long>)context.GetStateStore(SilentSensorAlertExample.StoreName);

        // PROCESSING_TIME: driven by the wall clock, so it runs even when no record arrives.
        context.Schedule(TimeSpan.FromSeconds(10), PunctuationType.PROCESSING_TIME, CheckSilentSensors);
    }

    public Record<string, string> Process(Record<string, string> record)
    {
        // context.WallClockTime is the task's clock (the TopologyTestDriver can move it forward in tests).
        lastHeartbeat.Put(record.Key, context.WallClockTime);
        return null;
    }

    private void CheckSilentSensors(long now)
    {
        foreach (var sensor in lastHeartbeat.All().ToList())
        {
            if (now - sensor.Value >= SilenceMs)
            {
                context.Forward(sensor.Key, $"silent for {(now - sensor.Value) / 1000}s");
                lastHeartbeat.Delete(sensor.Key); // alert once, until the sensor is heard of again
            }
        }
    }

    public void Close()
    {
    }
}
