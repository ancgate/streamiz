using System.Runtime.CompilerServices;
using System.Text.Json;
using Confluent.Kafka;
using NUnit.Framework;
using Streamiz.Examples.Common;
using Streamiz.Examples.DataGenerator;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Stream;

namespace Streamiz.Examples.RealWorld.Tests;

/// <summary>
/// Replays a dataset of examples/data through an example's topology with the TopologyTestDriver.
/// The records are read with the generator's loader: same keys, timestamps (event time) and order as `datagen load`.
/// </summary>
internal static class Scenario
{
    public static string DataDir => Path.Combine(TestContext.CurrentContext.TestDirectory, "data");

    /// <summary>The records of a dataset, in arrival order.</summary>
    public static List<DataRecord> Load(string dataset) => Loader.Read(DataDir, Catalog.Find(dataset));

    /// <summary>The facts.json of a dataset: values computed by the generator, documented in examples/data/README.md.</summary>
    public static JsonElement Facts(string dataset)
    {
        using var doc = JsonDocument.Parse(File.ReadAllText(Path.Combine(DataDir, dataset, "facts.json")));
        return doc.RootElement.Clone();
    }

    public static TopologyTestDriver Create(IExample example)
    {
        var builder = new StreamBuilder();
        example.BuildTopology(builder);
        return new TopologyTestDriver(builder.Build(), ExampleRunner.CreateConfig(example));
    }

    /// <summary>Pipes the raw JSON records (string key and value, event time as timestamp) to their topics.</summary>
    public static void Pipe(TopologyTestDriver driver, IEnumerable<DataRecord> records)
    {
        var topics = Topics.GetOrCreateValue(driver);
        foreach (var record in records)
        {
            if (!topics.TryGetValue("in:" + record.Topic, out var topic))
                topics["in:" + record.Topic] = topic = driver.CreateInputTopic(record.Topic, new StringSerDes(), new StringSerDes());
            ((TestInputTopic<string, string>)topic).PipeInput(record.Key, record.Value, record.Timestamp);
        }
    }

    // a driver creates a topic once, and the records of an output topic can be read once: both are kept per driver
    private static readonly ConditionalWeakTable<TopologyTestDriver, Dictionary<string, object>> Topics = new();

    public static T As<T>(this DataRecord record) where T : class
        => JsonSerializer.Deserialize<T>(record.Value, CamelJsonSerDes<T>.Options);

    /// <summary>All the records of an output topic (read once per driver, then cached).</summary>
    public static List<ConsumeResult<K, V>> ReadAll<K, V>(TopologyTestDriver driver, string topic, ISerDes<K> keySerdes, ISerDes<V> valueSerdes)
    {
        var topics = Topics.GetOrCreateValue(driver);
        if (!topics.TryGetValue("out:" + topic, out var records))
            topics["out:" + topic] = records = driver.CreateOutputTopic(topic, TimeSpan.FromSeconds(1), keySerdes, valueSerdes)
                .ReadKeyValueList().ToList();
        return (List<ConsumeResult<K, V>>)records;
    }

    /// <summary>
    /// The final result of each window: an aggregation emits every update, the last one per (key, window) is the result.
    /// </summary>
    public static Dictionary<(string Key, DateTime Start), V> FinalPerWindow<V>(IEnumerable<ConsumeResult<Windowed<string>, V>> records)
    {
        var result = new Dictionary<(string, DateTime), V>();
        foreach (var r in records)
            result[(r.Message.Key.Key, Utc(r.Message.Key.Window.StartMs))] = r.Message.Value;
        return result;
    }

    /// <summary>
    /// The content of a window store (an interactive query): the current result of each window still retained.
    /// Cheaper than reading a big output topic with the test driver, whose reads get slower as the topic grows.
    /// </summary>
    public static Dictionary<(string Key, DateTime Start), V> WindowStore<V>(TopologyTestDriver driver, string store)
    {
        var result = new Dictionary<(string, DateTime), V>();
        using var all = driver.GetWindowStore<string, V>(store).All();
        while (all.MoveNext())
            result[(all.Current!.Value.Key.Key, Utc(all.Current.Value.Key.Window.StartMs))] = all.Current.Value.Value;
        return result;
    }

    /// <summary>Same keys, same values (CollectionAssert.AreEquivalent does that too, but in quadratic time).</summary>
    public static void AssertSame<TKey, TValue>(IReadOnlyDictionary<TKey, TValue> expected, IReadOnlyDictionary<TKey, TValue> actual)
    {
        var missing = expected.Keys.Where(k => !actual.ContainsKey(k)).Take(5).ToList();
        var unexpected = actual.Keys.Where(k => !expected.ContainsKey(k)).Take(5).ToList();
        Assert.IsEmpty(missing, "missing results");
        Assert.IsEmpty(unexpected, "unexpected results");
        foreach (var (key, value) in expected)
            Assert.AreEqual(value, actual[key], key!.ToString());
    }

    public static long Ms(DateTime dateTime) => new DateTimeOffset(dateTime, TimeSpan.Zero).ToUnixTimeMilliseconds();

    public static DateTime Utc(long ms) => DateTimeOffset.FromUnixTimeMilliseconds(ms).UtcDateTime;

    public static DateTime Utc(string iso) => DateTime.Parse(iso, null, System.Globalization.DateTimeStyles.AdjustToUniversal);

    /// <summary>Start of the window of <paramref name="size"/> (aligned on the epoch) containing the time.</summary>
    public static DateTime Floor(DateTime time, TimeSpan size) => new(time.Ticks - time.Ticks % size.Ticks, DateTimeKind.Utc);
}
