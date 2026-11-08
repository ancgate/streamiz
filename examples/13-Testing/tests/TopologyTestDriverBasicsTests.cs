using System.Text;
using Confluent.Kafka;
using NUnit.Framework;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.Crosscutting;

namespace Streamiz.Examples.Testing.Tests;

/// <summary>
/// The basics: input and output topics, keys, values, headers and timestamps.
/// In the default SYNC_TASK mode, each PipeInput is processed before it returns: outputs can be read right away.
/// </summary>
public class TopologyTestDriverBasicsTests
{
    [Test]
    public void PipeOneRecordAndReadIt()
    {
        using var driver = Drivers.Create(OrderRouting.Build);
        // serdes: the defaults of the configuration (string), or given to CreateInputTopic/CreateOutputTopic
        var orders = driver.CreateInputTopic<string, string>(OrderRouting.Input);
        var standard = driver.CreateOutputTopic<string, string>(OrderRouting.Standard);
        var express = driver.CreateOutputTopic<string, string>(OrderRouting.Express);

        orders.PipeInput("o-1", "2 coffees");

        var record = standard.ReadKeyValue();
        Assert.AreEqual("o-1", record.Message.Key);
        Assert.AreEqual("2 COFFEES", record.Message.Value);
        // everything was read, and nothing went to the other topic
        // (IsEmpty and QueueSize only work in ASYNC_CLUSTER_IN_MEMORY mode: read the list instead)
        CollectionAssert.IsEmpty(standard.ReadValueList());
        CollectionAssert.IsEmpty(express.ReadValueList());
    }

    [Test]
    public void HeadersAreInputsToo()
    {
        using var driver = Drivers.Create(OrderRouting.Build);
        var orders = driver.CreateInputTopic<string, string>(OrderRouting.Input);
        var express = driver.CreateOutputTopic<string, string>(OrderRouting.Express);

        var headers = new Headers { { "priority", Encoding.UTF8.GetBytes("high") } };
        orders.PipeInput("o-2", "1 croissant", headers);

        var record = express.ReadKeyValue();
        Assert.AreEqual("1 CROISSANT", record.Message.Value);
        // headers travel with the record
        Assert.AreEqual("high", Encoding.UTF8.GetString(record.Message.Headers.GetLastBytes("priority")));
    }

    [Test]
    public void TimestampsAreKept()
    {
        using var driver = Drivers.Create(OrderRouting.Build);
        var orders = driver.CreateInputTopic<string, string>(OrderRouting.Input);
        var standard = driver.CreateOutputTopic<string, string>(OrderRouting.Standard);

        // without a timestamp, PipeInput uses the current time; give one to test time-based logic
        orders.PipeInput("o-3", "tea", Drivers.Start);

        Assert.AreEqual(Drivers.Start.GetMilliseconds(), standard.ReadKeyValue().Message.Timestamp.UnixTimestampMs);
    }

    [Test]
    public void PipeABatchAndReadEverything()
    {
        using var driver = Drivers.Create(OrderRouting.Build);
        var orders = driver.CreateInputTopic<string, string>(OrderRouting.Input);
        var standard = driver.CreateOutputTopic<string, string>(OrderRouting.Standard);

        orders.PipeInputs(new[]
        {
            KeyValuePair.Create("o-1", "tea"),
            KeyValuePair.Create("o-2", "cake"),
            KeyValuePair.Create("o-1", "water")
        });

        // ReadValueList: values only; ReadKeyValueList: full records; ReadKeyValuesToMap: the last value per key
        CollectionAssert.AreEqual(new[] { "TEA", "CAKE", "WATER" }, standard.ReadValueList().ToList());

        orders.PipeInput("o-2", "pie");
        orders.PipeInput("o-2", "tart");
        var latest = standard.ReadKeyValuesToMap();
        Assert.AreEqual("TART", latest["o-2"]);
    }
}
