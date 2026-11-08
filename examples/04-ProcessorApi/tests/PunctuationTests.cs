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

        // Note: when a punctuation runs, the driver moves its clock a little further than asked
        // (a tenth of the advance), so the exact number of seconds in the alerts isn't asserted.
        driver.AdvanceWallClockTime(TimeSpan.FromSeconds(20));
        var alerts = output.ReadAll();
        Assert.AreEqual(1, alerts.Count);
        Assert.AreEqual("sensor-1", alerts[0].Key);
        AssertSilentForAtLeast30Seconds(alerts[0].Value);

        // Reported once: nothing more for sensor-1 until it sends a heartbeat again.
        driver.AdvanceWallClockTime(TimeSpan.FromSeconds(30));
        alerts = output.ReadAll();
        Assert.AreEqual(1, alerts.Count);
        Assert.AreEqual("sensor-2", alerts[0].Key);
        AssertSilentForAtLeast30Seconds(alerts[0].Value);
    }

    private static void AssertSilentForAtLeast30Seconds(string alert)
    {
        StringAssert.StartsWith("silent for ", alert);
        var seconds = int.Parse(alert["silent for ".Length..].TrimEnd('s'));
        Assert.GreaterOrEqual(seconds, 30);
    }
}
