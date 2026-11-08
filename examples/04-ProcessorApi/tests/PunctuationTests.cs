using NUnit.Framework;
using Streamiz.Examples.ProcessorApi.Examples;
using static Streamiz.Examples.ProcessorApi.Tests.ExampleDriver;

namespace Streamiz.Examples.ProcessorApi.Tests;

public class CountPerMinuteTests
{
    [Test]
    public void CountsAreEmittedWhenTheStreamTimeMovesByAMinute()
    {
        using var driver = Create(new CountPerMinuteExample());
        var readings = driver.CreateInputTopic<string, string>(CountPerMinuteExample.ReadingsTopic);
        var output = driver.CreateOutputTopic<string, string>(CountPerMinuteExample.OutputTopic);

        // The first punctuation runs as soon as the stream time is known, right after the first record.
        readings.PipeInput("sensor-a", "21.5", T0);
        CollectionAssert.AreEqual(new[] { ("sensor-a", "1") }, output.ReadAll());

        // Then nothing is emitted until the stream time is one minute past that first punctuation.
        readings.PipeInput("sensor-a", "21.7", T0.AddSeconds(10));
        readings.PipeInput("sensor-b", "19.0", T0.AddSeconds(20));
        readings.PipeInput("sensor-a", "21.6", T0.AddSeconds(30));
        Assert.IsEmpty(output.ReadAll());

        // A record at T0 + 65s moves the stream time past T0 + 60s: the counts are emitted and reset.
        // The record itself is processed (counted) before the punctuation runs.
        readings.PipeInput("sensor-a", "21.8", T0.AddSeconds(65));
        CollectionAssert.AreEqual(new[]
        {
            ("sensor-a", "3"),
            ("sensor-b", "1")
        }, output.ReadAll());
    }
}

public class SilentSensorAlertTests
{
    [Test]
    public void SensorsSilentForThirtySecondsAreReportedOnce()
    {
        using var driver = Create(new SilentSensorAlertExample());
        var heartbeats = driver.CreateInputTopic<string, string>(SilentSensorAlertExample.HeartbeatsTopic);
        var output = driver.CreateOutputTopic<string, string>(SilentSensorAlertExample.OutputTopic);

        heartbeats.PipeInput("sensor-1", "ok");
        heartbeats.PipeInput("sensor-2", "ok");

        // The wall clock of the driver only moves when the test moves it: no record is needed for the punctuation.
        driver.AdvanceWallClockTime(TimeSpan.FromSeconds(20));
        Assert.IsEmpty(output.ReadAll());

        heartbeats.PipeInput("sensor-2", "ok");  // sensor-2 is still alive, sensor-1 isn't

        // Like Kafka's TopologyTestDriver, each AdvanceWallClockTime moves the clock by exactly the given
        // duration and runs a due punctuation once, at the new time (missed 10s ticks aren't replayed).
        // At +40s, sensor-1 (last heard of at 0s) has been silent for 40s.
        driver.AdvanceWallClockTime(TimeSpan.FromSeconds(20));
        CollectionAssert.AreEqual(new[] { ("sensor-1", "silent for 40s") }, output.ReadAll());

        // Reported once: nothing more for sensor-1 until it sends a heartbeat again.
        // At +70s, sensor-2 (last heard of at +20s) has been silent for 50s.
        driver.AdvanceWallClockTime(TimeSpan.FromSeconds(30));
        CollectionAssert.AreEqual(new[] { ("sensor-2", "silent for 50s") }, output.ReadAll());
    }
}
