namespace Streamiz.Examples.DataGenerator;

// Record shapes of the datasets. They're serialized in camelCase, in this property order.
// examples/14-RealWorld has the same shapes (its own copy, so each project stands alone).

// ---------- ecommerce ----------

public record Customer(string CustomerId, string Name, string Email, string Country, string Tier, DateTime UpdatedAt);

public record Product(string ProductId, string Name, string Category, decimal Price, DateTime UpdatedAt);

public record Order(string OrderId, string CustomerId, string ProductId, int Quantity, DateTime OrderedAt);

public record Payment(string PaymentId, string OrderId, decimal Amount, string Method, DateTime PaidAt);

public record Shipment(string OrderId, string Carrier, DateTime ShippedAt, DateTime DeliveredAt);

// ---------- clickstream ----------

public record PageView(string UserId, string Page, string UserAgent, DateTime ViewedAt);

// ---------- iot ----------

public record Device(string DeviceId, string Location, string Model, DateTime InstalledAt);

public record Reading(string DeviceId, double Temperature, double Humidity, int Battery, DateTime MeasuredAt);

// ---------- banking ----------

public record Account(string AccountId, string Holder, string HomeCountry, DateTime OpenedAt);

public record Transaction(string TransactionId, string AccountId, decimal Amount, string Currency, string Country,
    string Merchant, string Category, DateTime Timestamp);

// ---------- stocks ----------

public record Tick(string Symbol, decimal Price, int Volume, DateTime Timestamp);

// ---------- bad ----------

/// <summary>A malformed record: the key, the raw value (not valid for the topic's schema) and the timestamp.</summary>
public record RawRecord(string Key, string Value, DateTime Timestamp);
