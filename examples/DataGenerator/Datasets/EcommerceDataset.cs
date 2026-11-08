namespace Streamiz.Examples.DataGenerator.Datasets;

/// <summary>
/// An online shop over 3 days (2026-01-05 to 2026-01-07, UTC): customers, a product catalog with price changes,
/// orders with a daily rhythm, payments (most within minutes, some late, some never) and shipments.
/// </summary>
public static class EcommerceDataset
{
    public static readonly DateTime Start = Json.Utc(2026, 1, 5);
    public const int Days = 3;
    public const string FeaturedProduct = "P0007";
    public static readonly DateTime FeaturedPriceChange = Json.Utc(2026, 1, 6, 12);

    private static readonly string[] Countries = { "FR", "DE", "US", "GB", "ES", "IT" };
    private static readonly double[] CountryWeights = { 30, 20, 20, 15, 10, 5 };
    private static readonly string[] Tiers = { "bronze", "silver", "gold" };
    private static readonly double[] TierWeights = { 60, 30, 10 };

    private static readonly (string Category, decimal Min, decimal Max, string[] Nouns)[] Categories =
    {
        ("Electronics", 30, 900, new[] { "Headphones", "Speaker", "Monitor", "Keyboard", "Camera", "Charger" }),
        ("Books", 8, 45, new[] { "Novel", "Cookbook", "Atlas", "Biography", "Guide", "Anthology" }),
        ("Home", 10, 250, new[] { "Lamp", "Blanket", "Vase", "Kettle", "Rug", "Clock" }),
        ("Clothing", 12, 180, new[] { "Jacket", "Sweater", "Scarf", "Sneakers", "Shirt", "Hat" }),
        ("Sports", 15, 400, new[] { "Racket", "Yoga Mat", "Bike Helmet", "Ball", "Tent", "Backpack" }),
        ("Toys", 5, 120, new[] { "Puzzle", "Robot", "Board Game", "Kite", "Doll", "Train Set" }),
        ("Beauty", 6, 90, new[] { "Perfume", "Lotion", "Shampoo", "Brush Set", "Serum", "Soap" }),
        ("Grocery", 2, 40, new[] { "Coffee", "Tea", "Chocolate", "Olive Oil", "Honey", "Granola" }),
    };

    private static readonly string[] Adjectives =
        { "Classic", "Smart", "Eco", "Deluxe", "Compact", "Premium", "Urban", "Vintage", "Pro", "Bright" };

    // Orders per hour of the day: quiet nights, a lunch bump and an evening peak.
    private static readonly double[] HourWeights =
        { 1, 1, 1, 1, 1, 2, 3, 5, 7, 8, 8, 9, 10, 9, 8, 8, 9, 10, 11, 12, 11, 8, 5, 2 };

    private static readonly string[] PaymentMethods = { "card", "paypal", "applepay", "giftcard" };
    private static readonly double[] PaymentWeights = { 60, 25, 12, 3 };
    private static readonly string[] Carriers = { "UPS", "DHL", "FedEx", "LaPoste" };

    public record Data(List<Customer> Customers, List<Product> Products, List<Order> Orders,
        List<Payment> Payments, List<Shipment> Shipments);

    public static Data Generate(int seed)
    {
        var rng = new Rng(seed);

        // ---- customers: 500 profiles, then 25 tier upgrades during the 3 days
        var customers = new List<Customer>();
        for (var i = 1; i <= 500; ++i)
        {
            var (first, last) = Names.Person(rng);
            customers.Add(new Customer(
                $"C{i:D4}", $"{first} {last}", $"{first}.{last}{i}@example.com".ToLowerInvariant(),
                Countries[rng.Weighted(CountryWeights)], Tiers[rng.Weighted(TierWeights)],
                Start.AddDays(-1).AddSeconds(i * 10)));
        }

        var latestCustomer = customers.ToDictionary(c => c.CustomerId);
        var upgrades = new List<Customer>();
        foreach (var index in Enumerable.Range(0, 25).Select(_ => rng.Int(0, 500)).Distinct())
        {
            var customer = customers[index];
            if (customer.Tier == "gold")
                continue;
            var upgraded = customer with
            {
                Tier = customer.Tier == "bronze" ? "silver" : "gold",
                UpdatedAt = Start.AddSeconds(rng.Int(0, Days * 86400))
            };
            upgrades.Add(upgraded);
        }

        customers.AddRange(upgrades.OrderBy(c => c.UpdatedAt));

        // ---- products: 200, then about 40 price changes (and a known one for P0007)
        var products = new List<Product>();
        for (var i = 1; i <= 200; ++i)
        {
            var (category, min, max, nouns) = Categories[rng.Int(0, Categories.Length)];
            var price = Math.Round(min + (max - min) * (decimal)Math.Pow(rng.Double(), 2), 2);
            products.Add(new Product($"P{i:D4}", $"{rng.Pick(Adjectives)} {rng.Pick(nouns)}", category, price,
                Start.AddDays(-1).AddSeconds(i * 10)));
        }

        var history = products.ToDictionary(p => p.ProductId, p => new List<Product> { p });
        var changes = new List<Product>();
        for (var c = 0; c < 40; ++c)
        {
            var product = products[rng.Int(0, products.Count)];
            if (product.ProductId == FeaturedProduct)
                continue;
            var previous = history[product.ProductId].Last();
            var factor = rng.Chance(0.6) ? rng.Double(0.7, 0.9) : rng.Double(1.05, 1.25); // sales and increases
            var at = Start.AddSeconds(rng.Int(3600, Days * 86400 - 3600));
            if (at <= previous.UpdatedAt)
                continue;
            var change = previous with { Price = Math.Round(previous.Price * (decimal)factor, 2), UpdatedAt = at };
            history[product.ProductId].Add(change);
            changes.Add(change);
        }

        // the featured product goes on sale (-25%) on day 2 at noon
        var featured = history[FeaturedProduct].Last();
        var featuredChange = featured with { Price = Math.Round(featured.Price * 0.75m, 2), UpdatedAt = FeaturedPriceChange };
        history[FeaturedProduct].Add(featuredChange);
        changes.Add(featuredChange);
        products.AddRange(changes.OrderBy(p => p.UpdatedAt));

        decimal PriceAt(string productId, DateTime at)
            => history[productId].Where(p => p.UpdatedAt <= at).OrderBy(p => p.UpdatedAt).Last().Price;

        // ---- orders: 5000 over 3 days, more during the day, frequent customers and popular products
        var orderTimes = new List<DateTime>();
        for (var i = 0; i < 5000; ++i)
        {
            var day = rng.Int(0, Days);
            var hour = rng.Weighted(HourWeights);
            orderTimes.Add(Start.AddDays(day).AddHours(hour).AddSeconds(rng.Int(0, 3600)));
        }

        orderTimes.Sort();
        var orders = new List<Order>();
        for (var i = 0; i < orderTimes.Count; ++i)
        {
            var quantity = rng.Weighted(new double[] { 70, 20, 10 }) + 1;
            orders.Add(new Order($"O{i + 1:D6}", $"C{rng.Skewed(500, 1.6) + 1:D4}", $"P{rng.Skewed(200, 2.2) + 1:D4}",
                quantity, orderTimes[i]));
        }

        // ---- payments: 90% within 1-10 minutes, 5% late (35-120 minutes), 5% never
        var payments = new List<(Order Order, decimal Amount, string Method, DateTime PaidAt)>();
        foreach (var order in orders)
        {
            var r = rng.Double();
            if (r >= 0.95)
                continue;
            var delay = r < 0.90 ? rng.Seconds(60, 600) : rng.Seconds(35 * 60, 120 * 60);
            payments.Add((order, PriceAt(order.ProductId, order.OrderedAt) * order.Quantity,
                PaymentMethods[rng.Weighted(PaymentWeights)], order.OrderedAt + delay));
        }

        var paymentList = payments
            .OrderBy(p => p.PaidAt).ThenBy(p => p.Order.OrderId)
            .Select((p, i) => new Payment($"PAY{i + 1:D6}", p.Order.OrderId, p.Amount, p.Method, p.PaidAt))
            .ToList();

        // ---- shipments: 80% of the paid orders, 2-24 hours after the payment
        var shipments = new List<Shipment>();
        foreach (var payment in paymentList)
        {
            if (!rng.Chance(0.8))
                continue;
            var shipped = payment.PaidAt + rng.Seconds(2 * 3600, 24 * 3600);
            shipments.Add(new Shipment(payment.OrderId, rng.Pick(Carriers), shipped, shipped + rng.Seconds(86400, 3 * 86400)));
        }

        shipments.Sort((a, b) => a.ShippedAt.CompareTo(b.ShippedAt));
        return new Data(customers, products, orders, paymentList, shipments);
    }

    public static void Write(string dataDir, int seed)
    {
        var data = Generate(seed);
        var dir = Path.Combine(dataDir, "ecommerce");
        Json.WriteLines(Path.Combine(dir, "customers.jsonl"), data.Customers);
        Json.WriteLines(Path.Combine(dir, "products.jsonl"), data.Products);
        Json.WriteLines(Path.Combine(dir, "orders.jsonl"), data.Orders);
        Json.WriteLines(Path.Combine(dir, "payments.jsonl"), data.Payments);
        Json.WriteLines(Path.Combine(dir, "shipments.jsonl"), data.Shipments);

        var orders = data.Orders.ToDictionary(o => o.OrderId);
        var paid = data.Payments.Where(p => p.PaidAt - orders[p.OrderId].OrderedAt <= TimeSpan.FromMinutes(30)).ToList();
        var featuredHistory = data.Products.Where(p => p.ProductId == FeaturedProduct).ToList();
        var revenueByCustomer = data.Payments
            .GroupBy(p => orders[p.OrderId].CustomerId)
            .Select(g => new { customerId = g.Key, revenue = g.Sum(p => p.Amount) })
            .OrderByDescending(x => x.revenue).ThenBy(x => x.customerId)
            .ToList();

        Json.WriteFacts(Path.Combine(dir, "facts.json"), new
        {
            seed,
            customers = 500,
            customerUpdates = data.Customers.Count - 500,
            products = 200,
            priceChanges = data.Products.Count - 200,
            orders = data.Orders.Count,
            payments = data.Payments.Count,
            paymentsWithin30Minutes = paid.Count,
            latePayments = data.Payments.Count - paid.Count,
            unpaidOrders = data.Orders.Count - data.Payments.Count,
            shipments = data.Shipments.Count,
            featuredProduct = new
            {
                productId = FeaturedProduct,
                priceBefore = featuredHistory[^2].Price,
                priceAfter = featuredHistory[^1].Price,
                changedAt = FeaturedPriceChange,
                ordersBefore = data.Orders.Count(o => o.ProductId == FeaturedProduct && o.OrderedAt < FeaturedPriceChange),
                ordersAfter = data.Orders.Count(o => o.ProductId == FeaturedProduct && o.OrderedAt >= FeaturedPriceChange)
            },
            busiestHourUtc = data.Orders.GroupBy(o => o.OrderedAt.Hour).OrderByDescending(g => g.Count()).First().Key,
            topCustomersByPaidAmount = revenueByCustomer.Take(5)
        });
    }
}
