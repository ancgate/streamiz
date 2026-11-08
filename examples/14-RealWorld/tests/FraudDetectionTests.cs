using NUnit.Framework;
using Streamiz.Examples.RealWorld.Examples;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;

namespace Streamiz.Examples.RealWorld.Tests;

/// <summary>7398 card transactions (6 fraudulent patterns hidden in them) and 8 malformed records.</summary>
public class FraudDetectionTests
{
    private static readonly FraudDetection Example = new();
    private TopologyTestDriver driver;
    private List<Transaction> transactions;
    private List<FraudAlert> alerts;

    [OneTimeSetUp]
    public void Run()
    {
        var records = Scenario.Load("banking").Where(r => Example.InputTopics.Contains(r.Topic)).ToList();
        transactions = records.Select(r => r.As<Transaction>()).ToList();

        driver = Scenario.Create(Example);
        Scenario.Pipe(driver, records);
        Scenario.Pipe(driver, Scenario.Load("bad")); // malformed values on the same topic

        alerts = Scenario.ReadAll(driver, FraudDetection.AlertsTopic, new StringSerDes(), new CamelJsonSerDes<FraudAlert>())
            .Select(r => r.Message.Value).ToList();
    }

    [OneTimeTearDown]
    public void Dispose() => driver.Dispose();

    private List<string> AlertedAccounts(string type)
        => alerts.Where(a => a.Type == type).Select(a => a.AccountId).Distinct().OrderBy(a => a).ToList();

    private static List<string> Documented(string property)
        => Scenario.Facts("banking").GetProperty(property).EnumerateArray()
            .Select(e => e.GetProperty("accountId").GetString()).OrderBy(a => a).ToList();

    [Test]
    public void VelocityAlerts()
    {
        // independently: accounts with 10 transactions within 5 minutes
        var expected = transactions.GroupBy(t => t.AccountId)
            .Where(g =>
            {
                var times = g.Select(t => t.Timestamp).OrderBy(t => t).ToList();
                return times.Where((t, i) => i + FraudDetection.VelocityThreshold - 1 < times.Count
                                             && times[i + FraudDetection.VelocityThreshold - 1] - t <= FraudDetection.VelocityWindow).Any();
            })
            .Select(g => g.Key).OrderBy(a => a).ToList();

        Assert.AreEqual(expected, AlertedAccounts(FraudAlert.Velocity));
        Assert.AreEqual(Documented("velocityFraud"), AlertedAccounts(FraudAlert.Velocity)); // A0017, A0042
    }

    [Test]
    public void MultiCountryAlerts()
    {
        // independently: two consecutive transactions of an account in different countries within 10 minutes
        var expected = transactions.GroupBy(t => t.AccountId)
            .Where(g => g.OrderBy(t => t.Timestamp).Zip(g.OrderBy(t => t.Timestamp).Skip(1))
                .Any(p => p.First.Country != p.Second.Country && p.Second.Timestamp - p.First.Timestamp <= AccountProfile.CountryWindow))
            .Select(g => g.Key).OrderBy(a => a).ToList();

        Assert.AreEqual(expected, AlertedAccounts(FraudAlert.MultiCountry));
        Assert.AreEqual(Documented("multiCountryFraud"), AlertedAccounts(FraudAlert.MultiCountry)); // A0099, A0150
    }

    [Test]
    public void LargeAmountAlerts()
    {
        // independently: an amount > 500 and > 10x the average of the account's previous transactions (at least 5)
        var expected = transactions.GroupBy(t => t.AccountId)
            .Where(g => g.Select((t, i) => (t, previous: g.Take(i).ToList()))
                .Any(x => x.previous.Count >= AccountProfile.MinHistory && x.t.Amount > AccountProfile.MinLargeAmount
                          && x.t.Amount > AccountProfile.AverageFactor * x.previous.Average(p => p.Amount)))
            .Select(g => g.Key).OrderBy(a => a).ToList();

        Assert.AreEqual(expected, AlertedAccounts(FraudAlert.LargeAmount));
        Assert.AreEqual(Documented("largeAmountFraud"), AlertedAccounts(FraudAlert.LargeAmount)); // A0203, A0256
    }

    [Test]
    public void OnlyTheFraudulentAccountsRaiseAlerts()
    {
        var documented = Scenario.Facts("banking").GetProperty("fraudulentAccounts").EnumerateArray().Select(e => e.GetString());
        CollectionAssert.AreEquivalent(documented, alerts.Select(a => a.AccountId).Distinct());
    }

    [Test]
    public void MalformedRecordsGoToTheDeadLetterQueue()
    {
        var dlq = Scenario.ReadAll(driver, FraudDetection.DeadLetterTopic, new StringSerDes(), new StringSerDes());

        var bad = Scenario.Load("bad");
        Assert.AreEqual(Scenario.Facts("bad").GetProperty("records").GetInt32(), dlq.Count); // 8
        CollectionAssert.AreEqual(bad.Select(r => r.Value), dlq.Select(r => r.Message.Value)); // the original bytes
        CollectionAssert.AreEqual(bad.Select(r => r.Key), dlq.Select(r => r.Message.Key));
        // with headers describing the error
        Assert.That(dlq, Is.All.Matches<Confluent.Kafka.ConsumeResult<string, string>>(
            r => r.Message.Headers.Any(h => h.Key.StartsWith("__streams.errors.", StringComparison.Ordinal))));
    }
}
