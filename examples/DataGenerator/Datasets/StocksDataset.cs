namespace Streamiz.Examples.DataGenerator.Datasets;

/// <summary>
/// Trades of 10 fictional stocks during one session (2026-01-05, 14:30 to 21:00 UTC = 9:30 to 16:00 New York):
/// random walks with a known crash (ACME, -8% in 2 minutes) and a known jump (GLBX, +6%).
/// </summary>
public static class StocksDataset
{
    public static readonly DateTime Open = Json.Utc(2026, 1, 5, 14, 30);
    public static readonly DateTime Close = Json.Utc(2026, 1, 5, 21, 0);

    public static readonly (string Symbol, decimal Price)[] Symbols =
    {
        ("ACME", 124.50m), ("GLBX", 58.20m), ("INIT", 312.00m), ("NOVA", 23.75m), ("ORBT", 87.10m),
        ("PIXL", 145.60m), ("QNTM", 402.30m), ("RIVR", 36.90m), ("SOLR", 71.45m), ("VRTX", 219.80m)
    };

    public const string CrashSymbol = "ACME";
    public static readonly DateTime CrashStart = Json.Utc(2026, 1, 5, 16, 0);
    public static readonly TimeSpan CrashDuration = TimeSpan.FromMinutes(2);
    public const double CrashSize = 0.08;

    public const string JumpSymbol = "GLBX";
    public static readonly DateTime JumpAt = Json.Utc(2026, 1, 5, 18, 30);
    public const double JumpSize = 0.06;

    public static List<Tick> Generate(int seed)
    {
        var rng = new Rng(seed);
        var ticks = new List<Tick>();
        foreach (var (symbol, start) in Symbols)
        {
            var price = (double)start;
            var crashed = 0.0;
            var jumped = false;
            for (var t = Open.AddSeconds(rng.Int(0, 20)); t < Close; t = t.AddSeconds(rng.Int(10, 30)))
            {
                price *= Math.Exp(rng.Gaussian() * 0.0005);
                if (symbol == CrashSymbol && t >= CrashStart && t < CrashStart + CrashDuration)
                {
                    // linear drop over the 2 minutes
                    var target = CrashSize * (t - CrashStart).TotalSeconds / CrashDuration.TotalSeconds;
                    price *= (1 - target) / (1 - crashed);
                    crashed = target;
                }

                if (symbol == JumpSymbol && !jumped && t >= JumpAt)
                {
                    price *= 1 + JumpSize;
                    jumped = true;
                }

                ticks.Add(new Tick(symbol, Math.Round((decimal)price, 2), rng.Int(1, 50) * 10, t));
            }
        }

        return ticks.OrderBy(t => t.Timestamp).ThenBy(t => t.Symbol, StringComparer.Ordinal).ToList();
    }

    public static void Write(string dataDir, int seed)
    {
        var ticks = Generate(seed);
        var dir = Path.Combine(dataDir, "stocks");
        Json.WriteLines(Path.Combine(dir, "ticks.jsonl"), ticks);
        Json.WriteFacts(Path.Combine(dir, "facts.json"), new
        {
            seed,
            ticks = ticks.Count,
            symbols = Symbols.Select(s => new
            {
                symbol = s.Symbol,
                ticks = ticks.Count(t => t.Symbol == s.Symbol),
                open = ticks.First(t => t.Symbol == s.Symbol).Price,
                close = ticks.Last(t => t.Symbol == s.Symbol).Price,
                high = ticks.Where(t => t.Symbol == s.Symbol).Max(t => t.Price),
                low = ticks.Where(t => t.Symbol == s.Symbol).Min(t => t.Price)
            }),
            crash = new { symbol = CrashSymbol, from = CrashStart, to = CrashStart + CrashDuration, drop = CrashSize },
            jump = new { symbol = JumpSymbol, at = JumpAt, rise = JumpSize }
        });
    }
}
