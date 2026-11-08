using NUnit.Framework;
using Streamiz.Examples.RealWorld.Examples;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;

namespace Streamiz.Examples.RealWorld.Tests;

/// <summary>4 hours of readings of 50 devices, in arrival order: out-of-order and late readings, spikes and silences.</summary>
public class IotMonitoringTests
{
    private static readonly DateTime LateFrom = new(2026, 1, 5, 11, 0, 0, DateTimeKind.Utc);

    /// <summary>
    /// Independently: replay the readings in arrival order; a reading counts in a window if, when it arrives, the window
    /// isn't closed yet: window end + grace > stream time (the highest measurement time seen so far).
    /// </summary>
    private static Dictionary<(string, DateTime), SumCount> ExpectedWindows(IEnumerable<Reading> readings)
    {
        var expected = new Dictionary<(string, DateTime), SumCount>();
        var streamTime = DateTime.MinValue;
        foreach (var reading in readings)
        {
            streamTime = reading.MeasuredAt > streamTime ? reading.MeasuredAt : streamTime;
            for (var start = Scenario.Floor(reading.MeasuredAt, IotAverages.Advance);
                 start > reading.MeasuredAt - IotAverages.WindowSize;
                 start -= IotAverages.Advance)
            {
                if (start + IotAverages.WindowSize + IotAverages.Grace <= streamTime)
                    continue; // dropped: too late
                var key = (reading.DeviceId, start);
                expected[key] = (expected.TryGetValue(key, out var acc) ? acc : SumCount.Zero).Add(reading.Temperature);
            }
        }

        return expected;
    }

    [Test]
    public void MovingAveragesCountReadingsThatArriveWithinTheGracePeriod()
    {
        var records = Scenario.Load("iot").Where(r => r.Topic == IotAverages.ReadingsTopic).ToList();
        using var driver = Scenario.Create(new IotAverages());
        Scenario.Pipe(driver, records);

        // all the windows of all the devices, as an interactive query of the window store would return them
        var actual = Scenario.WindowStore<SumCount>(driver, IotAverages.Store);
        var expected = ExpectedWindows(records.Select(r => r.As<Reading>()));
        Scenario.AssertSame(expected, actual);

        // documented fact: D42's readings of 11:00-11:09 arrive 15 minutes late, after the grace period of all their
        // windows: the window [11:00, 11:10) has no result for D42, while it has one for the other devices
        Assert.IsFalse(actual.ContainsKey(("D42", LateFrom)));
        Assert.IsTrue(actual.ContainsKey(("D41", LateFrom)));
        Assert.IsTrue(actual.ContainsKey(("D42", LateFrom.AddMinutes(10))));
        Assert.AreEqual(9, actual[("D42", LateFrom.AddMinutes(-9))].Count); // 10:51:42 to 10:59:42, not 11:00:42
    }

    [Test]
    public void MovingAveragesTopicOfASliceOfDevices()
    {
        // the output topic, on 3 devices (the TopologyTestDriver reads big output topics slowly)
        var devices = new[] { "D41", "D42", "D43" };
        var records = Scenario.Load("iot").Where(r => r.Topic == IotAverages.ReadingsTopic && devices.Contains(r.Key)).ToList();
        using var driver = Scenario.Create(new IotAverages());
        Scenario.Pipe(driver, records);

        var actual = Scenario.FinalPerWindow(Scenario.ReadAll(driver, IotAverages.AveragesTopic,
            new TimeWindowedSerDes<string>(new StringSerDes(), (long)IotAverages.WindowSize.TotalMilliseconds), new DoubleSerDes()));
        var expected = ExpectedWindows(records.Select(r => r.As<Reading>()))
            .ToDictionary(w => w.Key, w => Math.Round(w.Value.Average(), 2));

        Scenario.AssertSame(expected, actual);
        Assert.IsFalse(actual.ContainsKey(("D42", LateFrom)));
    }

    [Test]
    public void AnomaliesAndOfflineDevicesAreReported()
    {
        var records = Scenario.Load("iot").Where(r => r.Topic == IotAlerts.ReadingsTopic).ToList();
        using var driver = Scenario.Create(new IotAlerts());
        Scenario.Pipe(driver, records);

        var alerts = Scenario.ReadAll(driver, IotAlerts.AlertsTopic, new StringSerDes(), new CamelJsonSerDes<DeviceAlert>())
            .Select(r => r.Message.Value).ToList();
        var facts = Scenario.Facts("iot");

        // the documented spikes: D07 at 09:30, 09:31 and 09:32 (+15°C), D23 at 10:30 (-12°C)
        var spikes = facts.GetProperty("spikes").EnumerateArray()
            .Select(s => (s.GetProperty("deviceId").GetString(), Scenario.Utc(s.GetProperty("measuredAt").GetString())));
        CollectionAssert.AreEquivalent(spikes, alerts.Where(a => a.Type == DeviceAlert.Anomaly).Select(a => (a.DeviceId, a.At)));

        // offline once: the silent devices (D13 for good, D31 for 40 minutes) and D42, whose late readings leave an
        // 11-minute gap in what arrives; back online: D31 and D42
        var silences = facts.GetProperty("silences").EnumerateArray().ToList();
        var lateDevice = facts.GetProperty("lateReadings").GetProperty("deviceId").GetString();
        CollectionAssert.AreEquivalent(
            silences.Select(s => s.GetProperty("deviceId").GetString()).Append(lateDevice),
            alerts.Where(a => a.Type == DeviceAlert.Offline).Select(a => a.DeviceId));
        CollectionAssert.AreEquivalent(
            silences.Where(s => s.TryGetProperty("silentUntil", out _)).Select(s => s.GetProperty("deviceId").GetString()).Append(lateDevice),
            alerts.Where(a => a.Type == DeviceAlert.BackOnline).Select(a => a.DeviceId));

        // D31 is silent from 09:00 to 09:40: reported offline between 09:05 and 09:07 (punctuation every minute of stream time)
        var d31 = alerts.Single(a => a.Type == DeviceAlert.Offline && a.DeviceId == "D31");
        Assert.That(d31.At, Is.InRange(new DateTime(2026, 1, 5, 9, 5, 0, DateTimeKind.Utc), new DateTime(2026, 1, 5, 9, 7, 0, DateTimeKind.Utc)));
    }
}
