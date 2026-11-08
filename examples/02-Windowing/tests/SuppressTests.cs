using NUnit.Framework;
using Streamiz.Kafka.Net.SerDes;
using static Streamiz.Examples.Windowing.Tests.ExampleDriver;

namespace Streamiz.Examples.Windowing.Tests;

public class TumblingFinalResultsTests
{
    [Test]
    public void OnlyTheFinalCountIsEmittedWhenTheWindowCloses()
    {
        using var driver = Create(new TumblingFinalResults());
        var clicks = driver.CreateInputTopic<string, string>(TumblingFinalResults.InputTopic);
        var finalCounts = driver.CreateOutputTopic(TumblingFinalResults.OutputTopic, TimeSpan.FromSeconds(1),
            new TimeWindowedSerDes<string>(new StringSerDes(), 60_000), new Int64SerDes());

        clicks.PipeInput("home", "alice", T0 + Sec(10));
        clicks.PipeInput("home", "bob", T0 + Sec(20));
        clicks.PipeInput("about", "carol", T0 + Sec(30));
        clicks.PipeInput("home", "dave", T0 + Sec(50));
        clicks.PipeInput("home", "erin", T0 + Sec(80)); // stream time 08:01:20 < 08:01:30 (end + grace): still open
        Assert.IsEmpty(finalCounts.ReadKeyValueList());

        clicks.PipeInput("home", "frank", T0 + Sec(91)); // stream time 08:01:31: [08:00, 08:01) is closed

        CollectionAssert.AreEquivalent(
            new[] { ("home", Min(0), Min(1), 3L), ("about", Min(0), Min(1), 1L) },
            finalCounts.ReadKeyValueList().Select(Read).ToArray());
    }
}

public class SessionFinalResultsTests
{
    [Test]
    public void AVisitIsEmittedOnceWhenItsOver()
    {
        using var driver = Create(new SessionFinalResults());
        var views = driver.CreateInputTopic<string, string>(SessionFinalResults.InputTopic);
        var visits = driver.CreateOutputTopic(SessionFinalResults.OutputTopic, TimeSpan.FromSeconds(1),
            new SessionWindowedSerDes<string>(new StringSerDes()), new NullableInt64SerDes());

        views.PipeInput("alice", "/home", T0);
        views.PipeInput("alice", "/products", T0 + Min(2)); // merged: no tombstone is emitted
        views.PipeInput("bob", "/home", T0 + Min(6));       // stream time 08:06: alice's visit could still grow
        Assert.IsEmpty(visits.ReadKeyValueList());

        views.PipeInput("carol", "/home", T0 + Min(8));     // 08:08 > 08:02 + 5 minutes: alice's visit is over

        CollectionAssert.AreEqual(
            new (string, TimeSpan, TimeSpan, long?)[] { ("alice", Min(0), Min(2), 2L) },
            visits.ReadKeyValueList().Select(Read).ToArray());
    }
}

public class RateLimitStreamTimeTests
{
    [Test]
    public void TheLatestPriceIsEmittedOnceAMinuteOfStreamTimeHasPassed()
    {
        using var driver = Create(new RateLimitStreamTime());
        var prices = driver.CreateInputTopic(RateLimitStreamTime.InputTopic, new StringSerDes(), new DoubleSerDes());
        var limited = driver.CreateOutputTopic(RateLimitStreamTime.OutputTopic, TimeSpan.FromSeconds(1),
            new StringSerDes(), new DoubleSerDes());

        prices.PipeInput("ACME", 10.0, T0);           // starts ACME's minute
        prices.PipeInput("ACME", 11.0, T0 + Sec(10));
        prices.PipeInput("ACME", 12.0, T0 + Sec(30));
        prices.PipeInput("XYZ", 5.0, T0 + Sec(40));   // starts XYZ's minute
        Assert.IsEmpty(limited.ReadKeyValueList());

        prices.PipeInput("ACME", 13.0, T0 + Sec(70)); // stream time 08:01:10: ACME's minute is over

        // only the latest ACME price; XYZ's minute ends at 08:01:40
        CollectionAssert.AreEqual(
            new[] { ("ACME", 13.0) },
            limited.ReadKeyValueList().Select(r => (r.Message.Key, r.Message.Value)).ToArray());
    }
}

public class RateLimitWallClockTests
{
    [Test]
    public void TheLatestPriceIsEmittedAfterAMinuteOfWallClockTimeWithoutNewRecords()
    {
        using var driver = Create(new RateLimitWallClock());
        var prices = driver.CreateInputTopic(RateLimitWallClock.InputTopic, new StringSerDes(), new DoubleSerDes());
        var limited = driver.CreateOutputTopic(RateLimitWallClock.OutputTopic, TimeSpan.FromSeconds(1),
            new StringSerDes(), new DoubleSerDes());

        prices.PipeInput("ACME", 10.0);
        prices.PipeInput("ACME", 11.0);
        driver.AdvanceWallClockTime(TimeSpan.FromSeconds(30));
        Assert.IsEmpty(limited.ReadKeyValueList());

        // no new record: the wall clock alone emits the latest price
        driver.AdvanceWallClockTime(TimeSpan.FromSeconds(31));
        CollectionAssert.AreEqual(
            new[] { ("ACME", 11.0) },
            limited.ReadKeyValueList().Select(r => (r.Message.Key, r.Message.Value)).ToArray());
    }
}
