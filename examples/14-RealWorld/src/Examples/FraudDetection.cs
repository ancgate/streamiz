using Streamiz.Examples.Common;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Examples.RealWorld.Examples;

/// <summary>
/// Card fraud detection (dataset: banking). Three rules raise alerts: too many transactions in 5 minutes
/// (sliding windows), transactions in 2 countries within 10 minutes, and an amount far above the account's average
/// (per-account state). Malformed transactions are skipped and sent to a dead letter queue.
/// </summary>
public class FraudDetection : IExample
{
    public const string TransactionsTopic = "banking.transactions";
    public const string AlertsTopic = "banking.fraud-alerts";
    public const string DeadLetterTopic = "banking.transactions-dlq";
    public const int VelocityThreshold = 10;
    public static readonly TimeSpan VelocityWindow = TimeSpan.FromMinutes(5);

    public string Name => "fraud-detection";

    public string Description => "Fraud alerts: velocity (sliding windows), 2 countries in 10 minutes and unusual amounts; malformed records to a DLQ.";

    public IEnumerable<string> InputTopics => new[] { TransactionsTopic };

    public void BuildTopology(StreamBuilder builder)
    {
        // transactions are keyed by account id
        var transactions = builder.Stream(TransactionsTopic, new StringSerDes(), new CamelJsonSerDes<Transaction>());

        // Rule 1, velocity: count each account's transactions in every 5-minute span (sliding windows), alert at 10.
        var velocityAlerts = transactions
            .GroupByKey()
            .WindowedBy(SlidingWindowOptions.Of(VelocityWindow))
            .Count(InMemoryWindows.As<string, long>("transactions-5min"))
            .ToStream()
            .Filter((_, count, _) => count >= VelocityThreshold)
            .Map((window, count, _) => KeyValuePair.Create(window.Key,
                new FraudAlert(FraudAlert.Velocity, window.Key,
                    $"{count} transactions between {DateTimeOffset.FromUnixTimeMilliseconds(window.Window.StartMs):HH:mm:ss} and {DateTimeOffset.FromUnixTimeMilliseconds(window.Window.EndMs):HH:mm:ss}",
                    DateTimeOffset.FromUnixTimeMilliseconds(window.Window.EndMs).UtcDateTime)));

        // Rules 2 and 3 need the account's history: a per-account profile (count, total, last country and time)
        // updated by each transaction; the update sets LastAlert when the transaction breaks a rule.
        var profileAlerts = transactions
            .GroupByKey()
            .Aggregate(() => new AccountProfile(), (_, tx, profile) => profile.Next(tx),
                InMemory.As<string, AccountProfile>("account-profiles").WithValueSerdes(new CamelJsonSerDes<AccountProfile>()))
            .ToStream()
            .Filter((_, profile, _) => profile.LastAlert != null)
            .MapValues((profile, _) => profile.LastAlert);

        velocityAlerts.Merge(profileAlerts)
            .To(AlertsTopic, new StringSerDes(), new CamelJsonSerDes<FraudAlert>());
    }

    public void Configure(StreamConfig config)
    {
        // a value that can't be deserialized is skipped (CONTINUE) and sent to the dead letter queue,
        // with headers describing the error (__streams.errors.*)
        config.DeserializationExceptionHandler = (_, _, _) => ExceptionHandlerResponse.CONTINUE;
        config.DeadLetterQueueTopicName = DeadLetterTopic;
    }
}

public record FraudAlert(string Type, string AccountId, string Detail, DateTime At)
{
    public const string Velocity = "velocity";
    public const string MultiCountry = "multi-country";
    public const string LargeAmount = "large-amount";
}

/// <summary>What the detection needs to know about an account, updated by each transaction.</summary>
public class AccountProfile
{
    public static readonly TimeSpan CountryWindow = TimeSpan.FromMinutes(10);
    public const int MinHistory = 5;
    public const decimal AverageFactor = 10m;
    public const decimal MinLargeAmount = 500m;

    public long Count { get; set; }
    public decimal Total { get; set; }
    public string LastCountry { get; set; }
    public DateTime LastTimestamp { get; set; }
    public FraudAlert LastAlert { get; set; }

    public AccountProfile Next(Transaction tx)
    {
        FraudAlert alert = null;
        if (Count >= MinHistory && tx.Amount > MinLargeAmount && tx.Amount > AverageFactor * (Total / Count))
            alert = new FraudAlert(FraudAlert.LargeAmount, tx.AccountId,
                $"{tx.Amount} {tx.Currency} at {tx.Merchant}, average so far {Math.Round(Total / Count, 2)}", tx.Timestamp);
        else if (LastCountry != null && tx.Country != LastCountry && tx.Timestamp - LastTimestamp <= CountryWindow)
            alert = new FraudAlert(FraudAlert.MultiCountry, tx.AccountId,
                $"{LastCountry} then {tx.Country} within {(tx.Timestamp - LastTimestamp).TotalMinutes:F0} minutes", tx.Timestamp);

        return new AccountProfile
        {
            Count = Count + 1,
            Total = Total + tx.Amount,
            LastCountry = tx.Country,
            LastTimestamp = tx.Timestamp,
            LastAlert = alert
        };
    }
}
