namespace Streamiz.Examples.DataGenerator.Datasets;

/// <summary>
/// Card transactions of 300 accounts over 2 days (2026-01-05 and 2026-01-06, UTC), with 3 kinds of fraud injected
/// in 6 known accounts. Normal behaviour is constrained so that it can never trigger the detection rules
/// (see data/README.md): transactions of an account are at least 15 minutes apart, and amounts stay between
/// 0.3x and 3x the account's typical amount (at most 450 EUR).
/// </summary>
public static class BankingDataset
{
    public static readonly DateTime Start = Json.Utc(2026, 1, 5);
    public const int Days = 2;
    public const int AccountCount = 300;

    public static readonly (string AccountId, DateTime At)[] VelocityFraud =
    {
        ("A0017", Json.Utc(2026, 1, 5, 14)), ("A0042", Json.Utc(2026, 1, 6, 9, 30))
    };

    public static readonly (string AccountId, DateTime At, string ForeignCountry)[] MultiCountryFraud =
    {
        ("A0099", Json.Utc(2026, 1, 5, 20, 15), "BR"), ("A0150", Json.Utc(2026, 1, 6, 3, 40), "TH")
    };

    public static readonly (string AccountId, DateTime At)[] LargeAmountFraud =
    {
        ("A0203", Json.Utc(2026, 1, 6, 16, 5)), ("A0256", Json.Utc(2026, 1, 6, 21, 50))
    };

    private static readonly string[] Countries = { "FR", "DE", "ES", "IT", "NL", "BE", "PT" };

    private static readonly (string Category, string[] Merchants)[] Merchants =
    {
        ("groceries", new[] { "FreshMart", "GreenGrocer", "DailyBasket" }),
        ("restaurants", new[] { "Pasta Corner", "Sushi Bar", "Le Bistro", "Burger Lab" }),
        ("transport", new[] { "CityMetro", "RideNow", "FuelStation" }),
        ("shopping", new[] { "MegaStore", "BookNook", "TechHub", "StyleShop" }),
        ("entertainment", new[] { "CinemaMax", "StreamPlus", "ConcertHall" }),
    };

    public record Data(List<Account> Accounts, List<Transaction> Transactions);

    public static Data Generate(int seed)
    {
        var rng = new Rng(seed);
        var accounts = new List<Account>();
        var typical = new Dictionary<string, decimal>();
        for (var a = 1; a <= AccountCount; ++a)
        {
            var (first, last) = Names.Person(rng);
            var account = new Account($"A{a:D4}", $"{first} {last}", rng.Pick(Countries),
                Json.Utc(2020, 1, 1).AddDays(rng.Int(0, 2000)));
            accounts.Add(account);
            typical[account.AccountId] = Math.Round((decimal)rng.Double(20, 150), 2);
        }

        var fraudTimes = VelocityFraud.Select(f => (f.AccountId, f.At))
            .Concat(MultiCountryFraud.Select(f => (f.AccountId, f.At)))
            .ToList();

        var tx = new List<(DateTime At, string AccountId, decimal Amount, string Country, string Merchant, string Category)>();
        foreach (var account in accounts)
        {
            // 15-35 transactions, at least 15 minutes apart
            var times = Enumerable.Range(0, rng.Int(15, 36))
                .Select(_ => Start.AddSeconds(rng.Int(0, Days * 86400 - 3600)))
                .OrderBy(t => t)
                .ToList();
            var last = DateTime.MinValue;
            foreach (var planned in times)
            {
                var at = planned < last.AddMinutes(15) ? last.AddMinutes(15).AddSeconds(rng.Int(0, 300)) : planned;
                last = at;
                // no normal transaction around an injected fraud of the same account
                if (fraudTimes.Any(f => f.AccountId == account.AccountId && Math.Abs((f.At - at).TotalMinutes) < 30))
                    continue;

                var amount = Math.Round(typical[account.AccountId] * (decimal)Math.Clamp(Math.Exp(rng.Gaussian() * 0.5), 0.3, 3.0), 2);
                var country = rng.Chance(0.95) ? account.HomeCountry : rng.Pick(Countries);
                var (category, merchants) = rng.Pick(Merchants);
                tx.Add((at, account.AccountId, amount, country, rng.Pick(merchants), category));
            }
        }

        var homeOf = accounts.ToDictionary(a => a.AccountId, a => a.HomeCountry);

        // velocity: 12 small payments in less than 3 minutes
        foreach (var (accountId, at) in VelocityFraud)
        {
            var t = at;
            for (var i = 0; i < 12; ++i)
            {
                tx.Add((t, accountId, Math.Round((decimal)rng.Double(1, 5), 2), homeOf[accountId], "QuickPay Kiosk", "shopping"));
                t += rng.Seconds(10, 15);
            }
        }

        // impossible travel: a payment at home, then one in another continent 5 minutes later
        foreach (var (accountId, at, foreign) in MultiCountryFraud)
        {
            tx.Add((at, accountId, Math.Round(typical[accountId], 2), homeOf[accountId], "FreshMart", "groceries"));
            tx.Add((at.AddMinutes(5), accountId, Math.Round(typical[accountId] * 1.5m, 2), foreign, "Duty Free Online", "shopping"));
        }

        // unusual amount: 20 times the typical amount, on day 2 (the account has history by then)
        foreach (var (accountId, at) in LargeAmountFraud)
        {
            tx.RemoveAll(t => t.AccountId == accountId && Math.Abs((t.At - at).TotalMinutes) < 30);
            tx.Add((at, accountId, Math.Max(1200m, Math.Round(typical[accountId] * 20, 2)), homeOf[accountId], "LuxuryWatches", "shopping"));
        }

        var transactions = tx
            .OrderBy(t => t.At).ThenBy(t => t.AccountId, StringComparer.Ordinal)
            .Select((t, i) => new Transaction($"T{i + 1:D6}", t.AccountId, t.Amount, "EUR", t.Country, t.Merchant, t.Category, t.At))
            .ToList();
        return new Data(accounts, transactions);
    }

    public static void Write(string dataDir, int seed)
    {
        var data = Generate(seed);
        var dir = Path.Combine(dataDir, "banking");
        Json.WriteLines(Path.Combine(dir, "accounts.jsonl"), data.Accounts);
        for (var d = 0; d < Days; ++d)
        {
            var day = Start.AddDays(d);
            Json.WriteLines(Path.Combine(dir, $"transactions-{day:yyyy-MM-dd}.jsonl"),
                data.Transactions.Where(t => t.Timestamp >= day && t.Timestamp < day.AddDays(1)));
        }

        Json.WriteFacts(Path.Combine(dir, "facts.json"), new
        {
            seed,
            accounts = AccountCount,
            transactions = data.Transactions.Count,
            rules = new
            {
                velocity = "at least 10 transactions within 5 minutes",
                multiCountry = "transactions in 2 different countries within 10 minutes",
                largeAmount = "amount > 10x the account's average so far (after at least 5 transactions) and > 500 EUR"
            },
            velocityFraud = VelocityFraud.Select(f => new { accountId = f.AccountId, from = f.At }),
            multiCountryFraud = MultiCountryFraud.Select(f => new { accountId = f.AccountId, at = f.At, foreignCountry = f.ForeignCountry }),
            largeAmountFraud = LargeAmountFraud.Select(f => new
            {
                accountId = f.AccountId,
                at = f.At,
                amount = data.Transactions.Single(t => t.AccountId == f.AccountId && t.Timestamp == f.At).Amount
            }),
            fraudulentAccounts = VelocityFraud.Select(f => f.AccountId)
                .Concat(MultiCountryFraud.Select(f => f.AccountId))
                .Concat(LargeAmountFraud.Select(f => f.AccountId))
                .OrderBy(a => a)
        });
    }
}
