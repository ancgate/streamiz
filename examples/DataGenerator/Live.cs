using Confluent.Kafka;
using Streamiz.Examples.DataGenerator.Datasets;

namespace Streamiz.Examples.DataGenerator;

/// <summary>Options of the live mode: events per second, and the ratios of late, out-of-order and malformed records.</summary>
public record LiveOptions(double Rate, double Late, double OutOfOrder, double Bad, TimeSpan? Duration);

/// <summary>
/// Generates new events continuously, in the same schemas as the files, with the current time as event time.
/// A late record is 5-15 minutes old, an out-of-order record 30-90 seconds old, and a malformed record isn't valid JSON.
/// </summary>
public static class Live
{
    public static async Task RunAsync(string dataset, LiveOptions options, CancellationToken token)
    {
        var rng = new Rng(Environment.TickCount);
        var next = Generator(dataset, rng);
        using var producer = Kafka.CreateProducer();
        var interval = TimeSpan.FromSeconds(1.0 / options.Rate);
        var end = options.Duration.HasValue ? DateTime.UtcNow + options.Duration.Value : DateTime.MaxValue;
        long produced = 0, late = 0, outOfOrder = 0, bad = 0;

        Console.WriteLine($"Producing live '{dataset}' events: {options.Rate}/s, late {options.Late:P0}, " +
                          $"out-of-order {options.OutOfOrder:P0}, malformed {options.Bad:P0}. Ctrl+C to stop.");

        while (!token.IsCancellationRequested && DateTime.UtcNow < end)
        {
            var now = DateTime.UtcNow;
            DateTime eventTime;
            if (rng.Chance(options.Late))
            {
                eventTime = now - rng.Seconds(5 * 60, 15 * 60);
                late++;
            }
            else if (rng.Chance(options.OutOfOrder))
            {
                eventTime = now - rng.Seconds(30, 90);
                outOfOrder++;
            }
            else
                eventTime = now;

            foreach (var record in next(eventTime))
            {
                var value = record.Value;
                if (rng.Chance(options.Bad))
                {
                    value = value[..(value.Length / 2)]; // truncated JSON
                    bad++;
                }

                producer.Produce(record.Topic, new Message<string, string>
                {
                    Key = record.Key,
                    Value = value,
                    Timestamp = new Timestamp(record.Timestamp, TimestampType.CreateTime)
                });
                produced++;
            }

            if (produced % 1000 == 0)
                Console.WriteLine($"{produced} produced ({late} late, {outOfOrder} out of order, {bad} malformed)");

            try
            {
                await Task.Delay(interval, token);
            }
            catch (TaskCanceledException)
            {
            }
        }

        producer.Flush(TimeSpan.FromSeconds(10));
        Console.WriteLine($"{produced} produced ({late} late, {outOfOrder} out of order, {bad} malformed)");
    }

    private static Func<DateTime, IEnumerable<DataRecord>> Generator(string dataset, Rng rng)
    {
        switch (dataset.ToLowerInvariant())
        {
            case "iot":
            {
                return at =>
                {
                    var device = $"D{rng.Int(1, IotDataset.DeviceCount + 1):D2}";
                    var reading = new Reading(device, Math.Round(18 + rng.Gaussian() * 2, 2), Math.Round(rng.Double(30, 60), 1),
                        rng.Int(20, 100), at);
                    return new[] { new DataRecord("iot.readings", device, Json.Serialize(reading), at) };
                };
            }
            case "stocks":
            {
                var prices = StocksDataset.Symbols.ToDictionary(s => s.Symbol, s => (double)s.Price);
                return at =>
                {
                    var symbol = rng.Pick(StocksDataset.Symbols).Symbol;
                    prices[symbol] *= Math.Exp(rng.Gaussian() * 0.0005);
                    var tick = new Tick(symbol, Math.Round((decimal)prices[symbol], 2), rng.Int(1, 50) * 10, at);
                    return new[] { new DataRecord("stocks.ticks", symbol, Json.Serialize(tick), at) };
                };
            }
            case "banking":
            {
                var countries = new[] { "FR", "DE", "ES", "IT", "NL" };
                var id = 0;
                return at =>
                {
                    var account = $"A{rng.Int(1, BankingDataset.AccountCount + 1):D4}";
                    var tx = new Transaction($"L{++id:D8}", account, Math.Round((decimal)rng.Double(5, 200), 2), "EUR",
                        rng.Pick(countries), "LiveShop", "shopping", at);
                    return new[] { new DataRecord("banking.transactions", account, Json.Serialize(tx), at) };
                };
            }
            case "clickstream":
            {
                var pages = new[] { "/", "/category/books", "/product/P0001", "/product/P0042", "/cart", "/checkout", "/confirmation" };
                return at =>
                {
                    var user = $"U{rng.Int(1, ClickstreamDataset.Users + 1):D4}";
                    var view = new PageView(user, rng.Pick(pages), "Mozilla/5.0 (X11; Linux x86_64) Firefox/132.0", at);
                    return new[] { new DataRecord("clickstream.pageviews", user, Json.Serialize(view), at) };
                };
            }
            case "ecommerce":
            {
                var id = 0;
                return at =>
                {
                    var order = new Order($"L{++id:D8}", $"C{rng.Int(1, 501):D4}", $"P{rng.Int(1, 201):D4}", rng.Int(1, 4), at);
                    var records = new List<DataRecord> { new("ecommerce.orders", order.OrderId, Json.Serialize(order), at) };
                    if (rng.Chance(0.9))
                    {
                        // the payment is produced right away, with its own (later) event time
                        var paidAt = at + rng.Seconds(60, 600);
                        var payment = new Payment($"LP{id:D8}", order.OrderId, Math.Round((decimal)rng.Double(5, 500), 2), "card", paidAt);
                        records.Add(new DataRecord("ecommerce.payments", order.OrderId, Json.Serialize(payment), paidAt));
                    }

                    return records;
                };
            }
            default:
                throw new ArgumentException($"No live generator for '{dataset}' (iot, stocks, banking, clickstream, ecommerce)");
        }
    }
}
