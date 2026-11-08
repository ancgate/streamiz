using Streamiz.Examples.Common;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Examples.Windowing;

/// <summary>
/// Hopping windows: fixed-size windows that overlap. A 5-minute window starting every minute gives a moving
/// average: each reading counts in 5 windows.
/// </summary>
public class HoppingAverageTemperature : IExample
{
    public const string InputTopic = "temperatures";
    public const string OutputTopic = "temperature-5min-average";
    public const string StoreName = "temperature-average-store";
    public static readonly TimeSpan WindowSize = TimeSpan.FromMinutes(5);
    public static readonly TimeSpan Advance = TimeSpan.FromMinutes(1);

    public string Name => "hopping-average-temperature";
    public string Description => "Moving 5-minute average temperature per sensor, updated every minute (hopping windows).";
    public IEnumerable<string> InputTopics => new[] { InputTopic };

    public void BuildTopology(StreamBuilder builder)
    {
        // Readings: key = sensor id, value = temperature.
        builder.Stream(InputTopic, new StringSerDes(), new DoubleSerDes())
            .GroupByKey()
            // 5-minute windows starting every minute: [08:00, 08:05), [08:01, 08:06)...
            .WindowedBy(HoppingWindowOptions.Of(WindowSize, Advance))
            // An average can't be updated from the previous average alone: aggregate a sum and a count.
            .Aggregate(
                () => new SumCount(),
                (sensor, temperature, acc) => new SumCount { Sum = acc.Sum + temperature, Count = acc.Count + 1 },
                InMemoryWindows.As<string, SumCount>(StoreName).WithValueSerdes(new JsonSerDes<SumCount>()))
            // Then turn each (sum, count) into the average.
            .MapValues((acc, _) => acc.Average())
            .ToStream()
            .To(OutputTopic,
                new TimeWindowedSerDes<string>(new StringSerDes(), (long)WindowSize.TotalMilliseconds),
                new DoubleSerDes());
    }

    public void Configure(StreamConfig config)
    {
    }
}
