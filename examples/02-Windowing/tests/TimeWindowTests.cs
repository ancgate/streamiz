using NUnit.Framework;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Stream;
using static Streamiz.Examples.Windowing.Tests.ExampleDriver;

namespace Streamiz.Examples.Windowing.Tests;

public class TumblingClicksPerMinuteTests
{
    [Test]
    public void EachClickUpdatesTheCountOfItsMinute()
    {
        using var driver = Create(new TumblingClicksPerMinute());
        var clicks = driver.CreateInputTopic<string, string>(TumblingClicksPerMinute.InputTopic);
        var counts = driver.CreateOutputTopic(TumblingClicksPerMinute.OutputTopic, TimeSpan.FromSeconds(1),
            new TimeWindowedSerDes<string>(new StringSerDes(), 60_000), new Int64SerDes());

        clicks.PipeInput("home", "alice", T0 + Sec(10));
        clicks.PipeInput("about", "bob", T0 + Sec(20));
        clicks.PipeInput("home", "carol", T0 + Sec(50));
        clicks.PipeInput("home", "alice", T0 + Sec(70)); // next minute

        CollectionAssert.AreEqual(
            new[]
            {
                ("home", Min(0), Min(1), 1L),
                ("about", Min(0), Min(1), 1L),
                ("home", Min(0), Min(1), 2L),
                ("home", Min(1), Min(2), 1L),
            },
            counts.ReadKeyValueList().Select(Read).ToArray());
    }
}

public class HoppingAverageTemperatureTests
{
    [Test]
    public void EachReadingCountsInFiveOverlappingWindows()
    {
        using var driver = Create(new HoppingAverageTemperature());
        var temperatures = driver.CreateInputTopic(HoppingAverageTemperature.InputTopic, new StringSerDes(), new DoubleSerDes());
        var averages = driver.CreateOutputTopic(HoppingAverageTemperature.OutputTopic, TimeSpan.FromSeconds(1),
            new TimeWindowedSerDes<string>(new StringSerDes(), 5 * 60_000), new DoubleSerDes());

        temperatures.PipeInput("s1", 20.0, T0);
        temperatures.PipeInput("s1", 22.0, T0 + Sec(30));

        var records = averages.ReadKeyValueList().Select(Read).ToList();

        // 08:00 is in the windows starting at 07:56, 07:57, 07:58, 07:59 and 08:00 (5 minutes, every minute)
        var lastAveragePerWindow = records
            .GroupBy(r => r.Start)
            .ToDictionary(g => g.Key, g => g.Last().Value);
        CollectionAssert.AreEquivalent(new[] { Min(-4), Min(-3), Min(-2), Min(-1), Min(0) }, lastAveragePerWindow.Keys);
        // each window first had 20, then the average of 20 and 22
        Assert.That(lastAveragePerWindow.Values, Is.All.EqualTo(21.0));
        Assert.AreEqual(10, records.Count);
    }
}

public class SlidingFailedLoginsTests
{
    [Test]
    public void ThreeFailedLoginsWithinTenSecondsRaiseOneAlert()
    {
        using var driver = Create(new SlidingFailedLogins());
        var logins = driver.CreateInputTopic<string, string>(SlidingFailedLogins.InputTopic);
        var alerts = driver.CreateOutputTopic(SlidingFailedLogins.OutputTopic, TimeSpan.FromSeconds(1),
            new TimeWindowedSerDes<string>(new StringSerDes(), 10_000), new Int64SerDes());

        logins.PipeInput("alice", "10.0.0.1", T0);
        logins.PipeInput("bob", "10.0.0.9", T0 + Sec(1));
        logins.PipeInput("alice", "10.0.0.1", T0 + Sec(3));
        logins.PipeInput("alice", "10.0.0.2", T0 + Sec(6));  // 3 failures in [07:59:56, 08:00:06]
        logins.PipeInput("alice", "10.0.0.2", T0 + Sec(30)); // too far from the others

        // The window holding the 3 failures ends at the third one and starts 10 seconds earlier.
        // A tumbling window of 10 seconds starting at 08:00:00 would also catch them, but not a burst
        // at 08:00:08, 08:00:11 and 08:00:14: sliding windows have no fixed boundaries.
        CollectionAssert.AreEqual(
            new[] { ("alice", Sec(-4), Sec(6), 3L) },
            alerts.ReadKeyValueList().Select(Read).ToArray());
    }

    [Test]
    public void ABurstAcrossAWholeTenSecondsBoundaryIsDetected()
    {
        using var driver = Create(new SlidingFailedLogins());
        var logins = driver.CreateInputTopic<string, string>(SlidingFailedLogins.InputTopic);
        var alerts = driver.CreateOutputTopic(SlidingFailedLogins.OutputTopic, TimeSpan.FromSeconds(1),
            new TimeWindowedSerDes<string>(new StringSerDes(), 10_000), new Int64SerDes());

        logins.PipeInput("alice", "10.0.0.1", T0 + Sec(8));
        logins.PipeInput("alice", "10.0.0.1", T0 + Sec(11));
        logins.PipeInput("alice", "10.0.0.1", T0 + Sec(14));

        CollectionAssert.AreEqual(
            new[] { ("alice", Sec(4), Sec(14), 3L) },
            alerts.ReadKeyValueList().Select(Read).ToArray());
    }
}

public class LateRecordsAndGraceTests
{
    [Test]
    public void ALateRecordIsCountedWithinTheGracePeriodAndDroppedAfterIt()
    {
        using var driver = Create(new LateRecordsAndGrace());
        var orders = driver.CreateInputTopic<string, string>(LateRecordsAndGrace.InputTopic);
        var counts = driver.CreateOutputTopic(LateRecordsAndGrace.OutputTopic, TimeSpan.FromSeconds(1),
            new TimeWindowedSerDes<string>(new StringSerDes(), 60_000), new Int64SerDes());

        orders.PipeInput("shop", "o1", T0 + Sec(10));  // window [08:00, 08:01)
        orders.PipeInput("shop", "o2", T0 + Sec(70));  // stream time 08:01:10
        orders.PipeInput("shop", "o3", T0 + Sec(50));  // late, but 08:01:10 - 30s grace = 08:00:40 < 08:01: accepted
        orders.PipeInput("shop", "o4", T0 + Sec(120)); // stream time 08:02:00: the window [08:00, 08:01) is closed
        orders.PipeInput("shop", "o5", T0 + Sec(40));  // too late: dropped

        CollectionAssert.AreEqual(
            new[]
            {
                ("shop", Min(0), Min(1), 1L),
                ("shop", Min(1), Min(2), 1L),
                ("shop", Min(0), Min(1), 2L),
                ("shop", Min(2), Min(3), 1L),
            },
            counts.ReadKeyValueList().Select(Read).ToArray());
    }
}

public class ReadWindowedResultsTests
{
    [Test]
    public void WindowedKeysAreReadBackWithTheSameSerdes()
    {
        using var driver = Create(new ReadWindowedResults());
        var windowedCounts = driver.CreateInputTopic(ReadWindowedResults.InputTopic,
            new TimeWindowedSerDes<string>(new StringSerDes(), 60_000), new Int64SerDes());
        var readable = driver.CreateOutputTopic(ReadWindowedResults.OutputTopic, TimeSpan.FromSeconds(1),
            new StringSerDes(), new StringSerDes());

        windowedCounts.PipeInput(new Windowed<string>("home", new TimeWindow(Ms(T0), Ms(T0) + 60_000)), 3L);

        var record = readable.ReadKeyValue();
        Assert.AreEqual("home", record.Message.Key);
        Assert.AreEqual("3 clicks from 08:00 to 08:01", record.Message.Value);
    }
}
