using Streamiz.Examples.Common;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Examples.RealWorld.Examples;

/// <summary>
/// Market data (dataset: stocks): 1-minute candles (open, high, low, close, volume) per symbol, a 5-minute
/// moving average updated every minute, and alerts when a price moves more than 5% within 5 minutes.
/// </summary>
public class StockOhlc : IExample
{
    public const string TicksTopic = "stocks.ticks";
    public const string CandlesTopic = "stocks.ohlc-1min";
    public const string MovingAverageTopic = "stocks.sma-5min";
    public const string AlertsTopic = "stocks.alerts";
    public const string CandlesStore = "candles";
    public const string MovingAverageStore = "sma-5min";
    public static readonly TimeSpan CandleSize = TimeSpan.FromMinutes(1);

    public string Name => "stock-ohlc";

    public string Description => "1-minute OHLC candles, 5-minute moving average and 5% price-move alerts per symbol.";

    public IEnumerable<string> InputTopics => new[] { TicksTopic };

    public void BuildTopology(StreamBuilder builder)
    {
        // ticks are keyed by symbol
        var ticks = builder.Stream(TicksTopic, new StringSerDes(), new CamelJsonSerDes<Tick>());

        // candles: tumbling 1-minute windows; ticks arrive in time order, so the first tick opens the candle
        ticks.GroupByKey()
            .WindowedBy(TumblingWindowOptions.Of(CandleSize))
            .Aggregate(() => Candle.Empty, (_, tick, candle) => candle.Add(tick),
                InMemoryWindows.As<string, Candle>(CandlesStore).WithValueSerdes(new CamelJsonSerDes<Candle>()))
            .ToStream()
            .To(CandlesTopic, new TimeWindowedSerDes<string>(new StringSerDes(), (long)CandleSize.TotalMilliseconds),
                new CamelJsonSerDes<Candle>());

        // moving average: 5-minute windows every minute
        ticks.GroupByKey()
            .WindowedBy(HoppingWindowOptions.Of(TimeSpan.FromMinutes(5), TimeSpan.FromMinutes(1)))
            .Aggregate(() => SumCount.Zero, (_, tick, acc) => acc.Add((double)tick.Price),
                InMemoryWindows.As<string, SumCount>(MovingAverageStore).WithValueSerdes(new CamelJsonSerDes<SumCount>()))
            .MapValues((acc, _) => Math.Round(acc.Average(), 4))
            .ToStream()
            .To(MovingAverageTopic, new TimeWindowedSerDes<string>(new StringSerDes(), 300_000), new DoubleSerDes());

        // alerts: each symbol keeps its last 5 minutes of prices
        ticks.GroupByKey()
            .Aggregate(() => new PriceWatch(), (_, tick, watch) => watch.Next(tick),
                InMemory.As<string, PriceWatch>("price-watch").WithValueSerdes(new CamelJsonSerDes<PriceWatch>()))
            .ToStream()
            .Filter((_, watch, _) => watch.LastAlert != null)
            .MapValues((watch, _) => watch.LastAlert)
            .To(AlertsTopic, new StringSerDes(), new CamelJsonSerDes<PriceAlert>());
    }

    public void Configure(StreamConfig config)
    {
    }
}

public record Candle(decimal Open, decimal High, decimal Low, decimal Close, long Volume, int Trades)
{
    public static readonly Candle Empty = new(0, 0, 0, 0, 0, 0);

    public Candle Add(Tick tick) => Trades == 0
        ? new Candle(tick.Price, tick.Price, tick.Price, tick.Price, tick.Volume, 1)
        : new Candle(Open, Math.Max(High, tick.Price), Math.Min(Low, tick.Price), tick.Price, Volume + tick.Volume, Trades + 1);
}

public record PricePoint(DateTime At, decimal Price);

public record PriceAlert(string Symbol, string Type, decimal From, decimal To, DateTime At)
{
    public const string Drop = "drop";
    public const string Jump = "jump";
}

/// <summary>The prices of the last 5 minutes; an alert is raised once when the price moves more than 5% from them.</summary>
public class PriceWatch
{
    public static readonly TimeSpan Window = TimeSpan.FromMinutes(5);
    public const decimal Threshold = 0.05m;

    public List<PricePoint> Recent { get; set; } = new();
    public bool Alerting { get; set; }
    public PriceAlert LastAlert { get; set; }

    public PriceWatch Next(Tick tick)
    {
        var recent = Recent.Where(p => tick.Timestamp - p.At <= Window).ToList();
        PriceAlert alert = null;
        var alerting = false;
        if (recent.Count > 0)
        {
            var high = recent.Max(p => p.Price);
            var low = recent.Min(p => p.Price);
            if (tick.Price <= high * (1 - Threshold))
            {
                alerting = true;
                alert = Alerting ? null : new PriceAlert(tick.Symbol, PriceAlert.Drop, high, tick.Price, tick.Timestamp);
            }
            else if (tick.Price >= low * (1 + Threshold))
            {
                alerting = true;
                alert = Alerting ? null : new PriceAlert(tick.Symbol, PriceAlert.Jump, low, tick.Price, tick.Timestamp);
            }
        }

        recent.Add(new PricePoint(tick.Timestamp, tick.Price));
        return new PriceWatch { Recent = recent, Alerting = alerting, LastAlert = alert };
    }
}
