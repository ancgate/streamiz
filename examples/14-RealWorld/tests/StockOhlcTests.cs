using NUnit.Framework;
using Streamiz.Examples.RealWorld.Examples;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;

namespace Streamiz.Examples.RealWorld.Tests;

/// <summary>A trading day of 10 symbols (12017 ticks), with a crash of ACME and a jump of GLBX.</summary>
public class StockOhlcTests
{
    private TopologyTestDriver driver;
    private List<Tick> ticks;

    [OneTimeSetUp]
    public void Run()
    {
        var records = Scenario.Load("stocks");
        ticks = records.Select(r => r.As<Tick>()).ToList();

        driver = Scenario.Create(new StockOhlc());
        Scenario.Pipe(driver, records);
    }

    [OneTimeTearDown]
    public void Dispose() => driver.Dispose();

    // the results are read from the window stores (interactive queries): the TopologyTestDriver reads big output
    // topics slowly; the output topics hold the same values (see the IoT tests for an output topic check)
    private Dictionary<(string Key, DateTime Start), Candle> Candles() => Scenario.WindowStore<Candle>(driver, StockOhlc.CandlesStore);

    [Test]
    public void OneMinuteCandles()
    {
        var expected = ticks.GroupBy(t => (t.Symbol, Scenario.Floor(t.Timestamp, StockOhlc.CandleSize)))
            .ToDictionary(g => g.Key, g => new Candle(g.First().Price, g.Max(t => t.Price), g.Min(t => t.Price), g.Last().Price,
                g.Sum(t => (long)t.Volume), g.Count()));

        Scenario.AssertSame(expected, Candles());
    }

    [Test]
    public void TheCandlesOfTheDayMatchTheDocumentedOpenHighLowClose()
    {
        var candles = Candles();
        foreach (var symbol in Scenario.Facts("stocks").GetProperty("symbols").EnumerateArray())
        {
            var name = symbol.GetProperty("symbol").GetString();
            var day = candles.Where(c => c.Key.Key == name).OrderBy(c => c.Key.Start).Select(c => c.Value).ToList();
            Assert.AreEqual(symbol.GetProperty("open").GetDecimal(), day[0].Open, name);
            Assert.AreEqual(symbol.GetProperty("close").GetDecimal(), day[^1].Close, name);
            Assert.AreEqual(symbol.GetProperty("high").GetDecimal(), day.Max(c => c.High), name);
            Assert.AreEqual(symbol.GetProperty("low").GetDecimal(), day.Min(c => c.Low), name);
            Assert.AreEqual(symbol.GetProperty("ticks").GetInt32(), day.Sum(c => c.Trades), name);
        }
    }

    [Test]
    public void FiveMinuteMovingAverage()
    {
        var size = TimeSpan.FromMinutes(5);
        var advance = TimeSpan.FromMinutes(1);
        var expected = new Dictionary<(string, DateTime), SumCount>();
        foreach (var tick in ticks)
            for (var start = Scenario.Floor(tick.Timestamp, advance); start > tick.Timestamp - size; start -= advance)
                expected[(tick.Symbol, start)] = (expected.TryGetValue((tick.Symbol, start), out var acc) ? acc : SumCount.Zero).Add((double)tick.Price);

        Scenario.AssertSame(expected, Scenario.WindowStore<SumCount>(driver, StockOhlc.MovingAverageStore));
    }

    [Test]
    public void PriceAlertsForTheCrashAndTheJumpOnly()
    {
        var alerts = Scenario.ReadAll(driver, StockOhlc.AlertsTopic, new StringSerDes(), new CamelJsonSerDes<PriceAlert>())
            .Select(r => r.Message.Value).ToList();
        var facts = Scenario.Facts("stocks");

        Assert.AreEqual(2, alerts.Count, string.Join(", ", alerts));

        // ACME loses 8% between 16:00 and 16:02: the alert comes when it's 5% below the 5-minute high
        var crash = facts.GetProperty("crash");
        var drop = alerts.Single(a => a.Type == PriceAlert.Drop);
        Assert.AreEqual(crash.GetProperty("symbol").GetString(), drop.Symbol);
        Assert.That(drop.At, Is.InRange(Scenario.Utc(crash.GetProperty("from").GetString()), Scenario.Utc(crash.GetProperty("to").GetString())));
        Assert.LessOrEqual(drop.To, drop.From * 0.95m);

        // GLBX gains 6% at 18:30: alert on the first tick after the jump
        var jump = facts.GetProperty("jump");
        var up = alerts.Single(a => a.Type == PriceAlert.Jump);
        Assert.AreEqual(jump.GetProperty("symbol").GetString(), up.Symbol);
        Assert.That(up.At, Is.InRange(Scenario.Utc(jump.GetProperty("at").GetString()), Scenario.Utc(jump.GetProperty("at").GetString()).AddMinutes(1)));
        Assert.GreaterOrEqual(up.To, up.From * 1.05m);
    }
}
