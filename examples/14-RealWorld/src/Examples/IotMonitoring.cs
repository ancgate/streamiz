using Streamiz.Examples.Common;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.Processors.Public;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Examples.RealWorld.Examples;

/// <summary>
/// IoT averages (dataset: iot): a 10-minute moving average of the temperature of each device, updated every minute
/// (hopping windows), with a 5-minute grace period for readings that arrive late or out of order.
/// </summary>
public class IotAverages : IExample
{
    public const string ReadingsTopic = "iot.readings";
    public const string AveragesTopic = "iot.temperature-avg-10min";
    public const string Store = "temperature-10min";
    public static readonly TimeSpan WindowSize = TimeSpan.FromMinutes(10);
    public static readonly TimeSpan Advance = TimeSpan.FromMinutes(1);
    public static readonly TimeSpan Grace = TimeSpan.FromMinutes(5);

    public string Name => "iot-averages";

    public string Description => "10-minute moving average temperature per device (hopping windows) with a 5-minute grace period.";

    public IEnumerable<string> InputTopics => new[] { ReadingsTopic };

    public void BuildTopology(StreamBuilder builder)
    {
        builder.Stream(ReadingsTopic, new StringSerDes(), new CamelJsonSerDes<Reading>())
            .GroupByKey()
            // windows [t, t+10min) every minute; a reading still counts if it arrives within 5 minutes
            // (of stream time) after its window's end, later ones are dropped (dropped-records metric)
            .WindowedBy(HoppingWindowOptions.Of(WindowSize, Advance, Grace))
            .Aggregate(() => SumCount.Zero, (_, reading, acc) => acc.Add(reading.Temperature),
                InMemoryWindows.As<string, SumCount>(Store).WithValueSerdes(new CamelJsonSerDes<SumCount>()))
            .MapValues((acc, _) => Math.Round(acc.Average(), 2))
            .ToStream()
            .To(AveragesTopic, new TimeWindowedSerDes<string>(new StringSerDes(), (long)WindowSize.TotalMilliseconds), new DoubleSerDes());
    }

    public void Configure(StreamConfig config)
    {
    }
}

/// <summary>
/// IoT alerts (dataset: iot): a reading far from the device's recent average (anomaly), and devices that stop
/// reporting for 5 minutes (offline) or start again (back online).
/// </summary>
public class IotAlerts : IExample
{
    public const string ReadingsTopic = "iot.readings";
    public const string AlertsTopic = "iot.alerts";
    public const string LastSeenStore = "device-last-seen";
    public static readonly TimeSpan OfflineAfter = TimeSpan.FromMinutes(5);

    public string Name => "iot-alerts";

    public string Description => "Anomalies (deviation from a moving average) and offline / back-online devices (stream-time punctuation).";

    public IEnumerable<string> InputTopics => new[] { ReadingsTopic };

    public void BuildTopology(StreamBuilder builder)
    {
        var readings = builder.Stream(ReadingsTopic, new StringSerDes(), new CamelJsonSerDes<Reading>());

        // anomalies: each device keeps an exponential moving average; a reading more than 8°C away raises an alert
        var anomalies = readings
            .GroupByKey()
            .Aggregate(() => new DeviceStats(), (_, reading, stats) => stats.Next(reading),
                InMemory.As<string, DeviceStats>("device-stats").WithValueSerdes(new CamelJsonSerDes<DeviceStats>()))
            .ToStream()
            .Filter((_, stats, _) => stats.LastAlert != null)
            .MapValues((stats, _) => stats.LastAlert);

        // offline detection: remember when each device was last heard of; a stream-time punctuation checks every minute
        var connectivity = readings.Transform(TransformerBuilder.New<string, Reading, string, DeviceAlert>()
            .Transformer<OfflineDetector>()
            .StateStore(Stores.KeyValueStoreBuilder(Stores.InMemoryKeyValueStore(LastSeenStore),
                new StringSerDes(), new CamelJsonSerDes<DeviceStatus>()))
            .Build());

        anomalies.Merge(connectivity).To(AlertsTopic, new StringSerDes(), new CamelJsonSerDes<DeviceAlert>());
    }

    public void Configure(StreamConfig config)
    {
    }
}

public record DeviceAlert(string Type, string DeviceId, string Detail, DateTime At)
{
    public const string Anomaly = "anomaly";
    public const string Offline = "offline";
    public const string BackOnline = "back-online";
}

/// <summary>Exponential moving average of a device's temperature.</summary>
public class DeviceStats
{
    public const double Alpha = 0.2;
    public const double Threshold = 8.0;
    public const int Warmup = 5;

    public double Average { get; set; }
    public long Count { get; set; }
    public DeviceAlert LastAlert { get; set; }

    public DeviceStats Next(Reading reading)
    {
        if (Count >= Warmup && Math.Abs(reading.Temperature - Average) > Threshold)
            // an anomalous value doesn't update the average, so a spike doesn't hide the next one
            return new DeviceStats
            {
                Average = Average,
                Count = Count,
                LastAlert = new DeviceAlert(DeviceAlert.Anomaly, reading.DeviceId,
                    $"{reading.Temperature}°C, average {Math.Round(Average, 2)}°C", reading.MeasuredAt)
            };

        return new DeviceStats
        {
            Average = Count == 0 ? reading.Temperature : Alpha * reading.Temperature + (1 - Alpha) * Average,
            Count = Count + 1
        };
    }
}

public record DeviceStatus(long LastSeen, bool Offline);

/// <summary>
/// Tracks the latest measurement time of each device. Every minute of stream time (the highest measurement time
/// seen across devices), a device not heard of for 5 minutes is reported offline once; its next reading reports it back online.
/// </summary>
public class OfflineDetector : ITransformer<string, Reading, string, DeviceAlert>
{
    private ProcessorContext<string, DeviceAlert> context;
    private IKeyValueStore<string, DeviceStatus> lastSeen;

    public void Init(ProcessorContext<string, DeviceAlert> context)
    {
        this.context = context;
        lastSeen = (IKeyValueStore<string, DeviceStatus>)context.GetStateStore(IotAlerts.LastSeenStore);
        context.Schedule(TimeSpan.FromMinutes(1), PunctuationType.STREAM_TIME, CheckDevices);
    }

    public Record<string, DeviceAlert> Process(Record<string, Reading> record)
    {
        var measuredAt = new DateTimeOffset(record.Value.MeasuredAt).ToUnixTimeMilliseconds();
        var status = lastSeen.Get(record.Key);
        lastSeen.Put(record.Key, new DeviceStatus(Math.Max(measuredAt, status?.LastSeen ?? 0), false));

        return status is { Offline: true }
            ? Record<string, DeviceAlert>.Create(record.Key,
                new DeviceAlert(DeviceAlert.BackOnline, record.Key, "reporting again", record.Value.MeasuredAt))
            : null;
    }

    private void CheckDevices(long streamTime)
    {
        foreach (var device in lastSeen.All().ToList())
        {
            if (device.Value.Offline || streamTime - device.Value.LastSeen <= IotAlerts.OfflineAfter.TotalMilliseconds)
                continue;
            var last = DateTimeOffset.FromUnixTimeMilliseconds(device.Value.LastSeen).UtcDateTime;
            context.Forward(device.Key, new DeviceAlert(DeviceAlert.Offline, device.Key, $"no reading since {last:HH:mm:ss}",
                DateTimeOffset.FromUnixTimeMilliseconds(streamTime).UtcDateTime));
            lastSeen.Put(device.Key, device.Value with { Offline = true });
        }
    }

    public void Close()
    {
    }
}
