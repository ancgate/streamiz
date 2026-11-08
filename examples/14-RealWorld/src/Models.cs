namespace Streamiz.Examples.RealWorld;

// Input records: the schemas of the files in examples/data (see examples/data/README.md).

public record Customer(string CustomerId, string Name, string Email, string Country, string Tier, DateTime UpdatedAt);

public record Product(string ProductId, string Name, string Category, decimal Price, DateTime UpdatedAt);

public record Order(string OrderId, string CustomerId, string ProductId, int Quantity, DateTime OrderedAt);

public record Payment(string PaymentId, string OrderId, decimal Amount, string Method, DateTime PaidAt);

public record PageView(string UserId, string Page, string UserAgent, DateTime ViewedAt);

public record Reading(string DeviceId, double Temperature, double Humidity, int Battery, DateTime MeasuredAt);

public record Transaction(string TransactionId, string AccountId, decimal Amount, string Currency, string Country,
    string Merchant, string Category, DateTime Timestamp);

public record Tick(string Symbol, decimal Price, int Volume, DateTime Timestamp);

// Results and state of the scenarios.

/// <summary>An order with its product (price at the order's time) and its customer.</summary>
public record EnrichedOrder(string OrderId, string CustomerId, string ProductId, string Category, int Quantity,
    decimal UnitPrice, decimal Amount, DateTime OrderedAt, string Country = null, string Tier = null);

/// <summary>Revenue and number of orders.</summary>
public record Revenue(decimal Amount, int Orders)
{
    public static readonly Revenue Zero = new(0m, 0);
    public Revenue Add(decimal amount) => new(Amount + amount, Orders + 1);
}

/// <summary>A sum and a count, to compute averages in windows.</summary>
public record SumCount(double Sum, long Count)
{
    public static readonly SumCount Zero = new(0, 0);
    public SumCount Add(double value) => new(Sum + value, Count + 1);
    public double Average() => Count == 0 ? 0 : Sum / Count;
}
