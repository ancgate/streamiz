using Confluent.Kafka;
using Streamiz.Examples.Common;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.SerDes;

namespace Streamiz.Examples.Basics;

/// <summary>
/// A timestamp extractor gives each record its "event time", read here from the payload: the time a sensor
/// took a measure, not the time Kafka received it. Windows, joins and suppress all work on this time.
/// </summary>
public class EventTimeExtractor : IExample
{
    public const string InputTopic = "sensor-readings";
    public const string OutputTopic = "sensor-readings-by-event-time";

    public string Name => "event-time-extractor";
    public string Description => "Use the time a measure was taken (from the payload) as the record timestamp.";
    public IEnumerable<string> InputTopics => new[] { InputTopic };

    /// <summary>
    /// Reads the timestamp from the deserialized value. If the value can't be read, it falls back to the
    /// partition time (the highest timestamp seen on the partition), so the record isn't dropped.
    /// A negative timestamp would drop the record.
    /// </summary>
    public class MeasuredAtExtractor : ITimestampExtractor
    {
        public long Extract(ConsumeResult<object, object> record, long partitionTime)
            => record.Message.Value is SensorReading reading && reading.MeasuredAt > 0
                ? reading.MeasuredAt
                : partitionTime;
    }

    public void BuildTopology(StreamBuilder builder)
    {
        // The extractor is given for this source only. config.DefaultTimestampExtractor sets it for all sources.
        builder.Stream(InputTopic, new StringSerDes(), new JsonSerDes<SensorReading>(), new MeasuredAtExtractor())
            // The records written to the output topic carry the extracted timestamp.
            .To(OutputTopic, new StringSerDes(), new JsonSerDes<SensorReading>());
    }

    public void Configure(StreamConfig config)
    {
    }
}
