using NUnit.Framework;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.SerDes;

namespace Streamiz.Examples.Testing.Tests;

/// <summary>
/// Time. Two clocks matter:
/// - stream time: the largest record timestamp seen so far. It moves only when records arrive, so drive it with
///   the timestamps you give to PipeInput (windows, grace periods, suppress until window close, joins).
/// - wall-clock time: the time of the machine. In the driver it's simulated and moves only with AdvanceWallClockTime
///   (processing-time punctuators, suppress until a wall-clock time limit).
/// </summary>
public class TimeTests
{
    [Test]
    public void AWindowIsEmittedOnceStreamTimePassesItsEnd()
    {
        using var driver = Drivers.Create(ClicksPerMinute.Build);
        var clicks = driver.CreateInputTopic<string, string>(ClicksPerMinute.Input);
        var perMinute = driver.CreateOutputTopic(ClicksPerMinute.Output, TimeSpan.FromSeconds(1),
            ClicksPerMinute.WindowedKeySerDes, new Int64SerDes());

        clicks.PipeInput("home", "c1", Drivers.Start.AddSeconds(5));
        clicks.PipeInput("home", "c2", Drivers.Start.AddSeconds(40));
        // stream time is 10:00:40: the [10:00, 10:01) window is still open, nothing is emitted
        CollectionAssert.IsEmpty(perMinute.ReadKeyValueList());

        // any record after 10:01 closes it (here a click on another page)
        clicks.PipeInput("pricing", "c3", Drivers.Start.AddSeconds(61));

        var record = perMinute.ReadKeyValue();
        Assert.AreEqual("home", record.Message.Key.Key);
        Assert.AreEqual(Drivers.Start.GetMilliseconds(), record.Message.Key.Window.StartMs);
        Assert.AreEqual(2, record.Message.Value);
        CollectionAssert.IsEmpty(perMinute.ReadKeyValueList());
    }

    [Test]
    public void OutOfOrderRecordsAreJoinedAsOfTheirTimestamp()
    {
        using var driver = Drivers.Create(PriceAtOrderTime.Build);
        var prices = driver.CreateInputTopic<string, string>(PriceAtOrderTime.Prices);
        var orders = driver.CreateInputTopic<string, string>(PriceAtOrderTime.Orders);
        var priced = driver.CreateOutputTopic<string, string>(PriceAtOrderTime.Output);

        prices.PipeInput("coffee", "4.00", Drivers.Start);
        prices.PipeInput("coffee", "4.50", Drivers.Start.AddHours(1).AddMinutes(-30));

        // an order placed at 10:15 that arrives late: it gets the price valid at 10:15
        orders.PipeInput("coffee", "2", Drivers.Start.AddMinutes(15));
        orders.PipeInput("coffee", "1", Drivers.Start.AddMinutes(45));

        CollectionAssert.AreEqual(new[] { "2 x 4.00", "1 x 4.50" }, priced.ReadValueList().ToList());
    }

    [Test]
    public void WallClockTimeIsAdvancedExplicitly()
    {
        using var driver = Drivers.Create(AlertDebouncer.Build);
        var alerts = driver.CreateInputTopic<string, string>(AlertDebouncer.Input);
        var debounced = driver.CreateOutputTopic<string, string>(AlertDebouncer.Output);

        alerts.PipeInput("disk-full", "90%");
        alerts.PipeInput("disk-full", "95%");
        driver.AdvanceWallClockTime(TimeSpan.FromSeconds(10));
        CollectionAssert.IsEmpty(debounced.ReadValueList());

        // 30 s after the first alert: one record, with the latest value
        driver.AdvanceWallClockTime(TimeSpan.FromSeconds(25));
        CollectionAssert.AreEqual(new[] { "95%" }, debounced.ReadValueList().ToList());
    }
}
