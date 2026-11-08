using Streamiz.Examples.DataGenerator.Datasets;

namespace Streamiz.Examples.DataGenerator;

/// <summary>Where a dataset file goes: its topic, the field used as the record key, and the event-time field.</summary>
public record FileSpec(string Pattern, string Topic, string KeyField, string TimestampField, bool Wrapped = false);

public record DatasetSpec(string Name, string Description, FileSpec[] Files);

/// <summary>The datasets of examples/data, and the topics they're loaded into.</summary>
public static class Catalog
{
    /// <summary>Seed of the generated files (each dataset uses Seed + its own offset).</summary>
    public const int Seed = 20260105;

    public static readonly DatasetSpec[] Datasets =
    {
        new("ecommerce", "Online shop: customers, products with price changes, orders, payments, shipments", new[]
        {
            new FileSpec("customers.jsonl", "ecommerce.customers", "customerId", "updatedAt"),
            new FileSpec("products.jsonl", "ecommerce.products", "productId", "updatedAt"),
            new FileSpec("orders.jsonl", "ecommerce.orders", "orderId", "orderedAt"),
            new FileSpec("payments.jsonl", "ecommerce.payments", "orderId", "paidAt"),
            new FileSpec("shipments.jsonl", "ecommerce.shipments", "orderId", "shippedAt"),
        }),
        new("clickstream", "Website page views: sessions, funnel, bots", new[]
        {
            new FileSpec("pageviews-*.jsonl", "clickstream.pageviews", "userId", "viewedAt"),
        }),
        new("iot", "Sensor readings every minute with anomalies, silences, late and out-of-order readings", new[]
        {
            new FileSpec("devices.jsonl", "iot.devices", "deviceId", "installedAt"),
            new FileSpec("readings-*.jsonl", "iot.readings", "deviceId", "measuredAt"),
        }),
        new("banking", "Card transactions with injected fraud", new[]
        {
            new FileSpec("accounts.jsonl", "banking.accounts", "accountId", "openedAt"),
            new FileSpec("transactions-*.jsonl", "banking.transactions", "accountId", "timestamp"),
        }),
        new("stocks", "Trades of 10 stocks during one session", new[]
        {
            new FileSpec("ticks.jsonl", "stocks.ticks", "symbol", "timestamp"),
        }),
        new("bad", "Malformed transactions for the error-handling demos", new[]
        {
            new FileSpec("transactions-malformed.jsonl", "banking.transactions", "key", "timestamp", Wrapped: true),
        }),
    };

    /// <summary>Output topics of the examples/14-RealWorld scenarios (created by `create-topics --with-outputs`).</summary>
    public static readonly string[] OutputTopics =
    {
        "ecommerce.orders-enriched", "ecommerce.revenue-by-category-hourly",
        "ecommerce.orders-paid", "ecommerce.orders-unpaid", "ecommerce.payments-late",
        "banking.fraud-alerts", "banking.transactions-dlq",
        "iot.temperature-avg-10min", "iot.alerts",
        "clickstream.sessions", "clickstream.pageviews-per-minute", "clickstream.bot-suspects",
        "stocks.ohlc-1min", "stocks.sma-5min", "stocks.alerts",
    };

    public static DatasetSpec Find(string name)
        => Datasets.FirstOrDefault(d => d.Name.Equals(name, StringComparison.OrdinalIgnoreCase))
           ?? throw new ArgumentException($"Unknown dataset '{name}'. Known: {string.Join(", ", Datasets.Select(d => d.Name))}");

    public static void GenerateAll(string dataDir)
    {
        EcommerceDataset.Write(dataDir, Seed + 1);
        ClickstreamDataset.Write(dataDir, Seed + 2);
        IotDataset.Write(dataDir, Seed + 3);
        BankingDataset.Write(dataDir, Seed + 4);
        StocksDataset.Write(dataDir, Seed + 5);
        BadDataset.Write(dataDir, Seed + 6);
    }

    /// <summary>Finds examples/data: the "data" folder next to the examples solution, from the current directory up.</summary>
    public static string FindDataDir()
    {
        for (var dir = new DirectoryInfo(Directory.GetCurrentDirectory()); dir != null; dir = dir.Parent)
        {
            if (File.Exists(Path.Combine(dir.FullName, "Streamiz.Examples.sln")))
                return Path.Combine(dir.FullName, "data");
            if (File.Exists(Path.Combine(dir.FullName, "examples", "Streamiz.Examples.sln")))
                return Path.Combine(dir.FullName, "examples", "data");
        }

        throw new DirectoryNotFoundException("examples/data not found: run from the repository, or pass --data <dir>");
    }
}
