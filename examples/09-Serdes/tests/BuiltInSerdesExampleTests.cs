using NUnit.Framework;
using Streamiz.Examples.Serdes.Examples;
using Streamiz.Kafka.Net.SerDes;

namespace Streamiz.Examples.Serdes.Tests;

public class BuiltInSerdesExampleTests
{
    [Test]
    public void HotTemperaturesAreRoundedToIntegers()
    {
        using var driver = TestDrivers.For(new BuiltInSerdesExample());
        var temperatures = driver.CreateInputTopic<string, double, StringSerDes, DoubleSerDes>("temperatures");
        var alerts = driver.CreateOutputTopic<string, int, StringSerDes, Int32SerDes>("heat-alerts");

        temperatures.PipeInput("sensor-1", 21.4);
        temperatures.PipeInput("sensor-2", 31.6);
        temperatures.PipeInput("sensor-1", 30.0);

        var results = alerts.ReadKeyValueList().Select(r => (r.Message.Key, r.Message.Value)).ToList();
        CollectionAssert.AreEqual(new[] { ("sensor-2", 32), ("sensor-1", 30) }, results);
    }
}
