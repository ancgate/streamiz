using System.Text;
using NUnit.Framework;
using Streamiz.Examples.AsyncProcessing.Examples;
using Streamiz.Kafka.Net.SerDes;

namespace Streamiz.Examples.AsyncProcessing.Tests;

public class EnrichWithHttpExampleTests
{
    [Test]
    public void OrdersAreEnrichedAndTransientFailuresRetried()
    {
        var directory = new FakeCustomerDirectory();
        directory.FlakyOnce.Add("c-2"); // the first call for c-2 fails with a 503, the retry succeeds

        using var driver = TestDrivers.For(new EnrichWithHttpExample(directory));
        var orders = driver.CreateInputTopic<string, string>("orders");
        var enriched = driver.CreateOutputTopic<string, string>("orders-enriched");

        orders.PipeInput("order-1", "c-1");
        orders.PipeInput("order-2", "c-2");

        var results = enriched.ReadKeyValueList().Select(r => (r.Message.Key, r.Message.Value)).ToList();
        CollectionAssert.AreEquivalent(new[] { ("order-1", "c-1:Alice"), ("order-2", "c-2:Bob") }, results);
        Assert.AreEqual(2, directory.CallsFor("c-2"));
    }
}

public class RekeyAsyncExampleTests
{
    [Test]
    public void SignupsAreKeyedByAccount()
    {
        using var driver = TestDrivers.For(new RekeyAsyncExample(new FakeAccountLookup()));
        var signups = driver.CreateInputTopic<string, string>("signups");
        var byAccount = driver.CreateOutputTopic<string, string>("signups-by-account");

        signups.PipeInput("alice@example.com", "pro");
        signups.PipeInput("bob@example.com", "free");

        var results = byAccount.ReadKeyValueList().Select(r => (r.Message.Key, r.Message.Value)).ToList();
        CollectionAssert.AreEquivalent(new[] { ("acc-42", "pro"), ("acc-7", "free") }, results);
    }
}

public class AuditSinkExampleTests
{
    [Test]
    public void EveryPaymentIsWrittenEvenWhenTheLogIsBusy()
    {
        var auditLog = new FakeAuditLog(failuresBeforeSuccess: 2);
        using var driver = TestDrivers.For(new AuditSinkExample(auditLog));
        var payments = driver.CreateInputTopic<string, string>("payments");

        payments.PipeInput("p-1", "42.00 EUR");
        payments.PipeInput("p-2", "13.37 EUR");

        CollectionAssert.AreEquivalent(new[] { ("p-1", "42.00 EUR"), ("p-2", "13.37 EUR") }, auditLog.Entries.ToList());
    }
}

public class RetryToDeadLetterQueueExampleTests
{
    [Test]
    public void RecordsStillFailingAfterTheRetriesGoToTheDeadLetterQueue()
    {
        var directory = new FakeCustomerDirectory();
        directory.AlwaysFailing.Add("c-broken");

        using var driver = TestDrivers.For(new RetryToDeadLetterQueueExample(directory));
        var orders = driver.CreateInputTopic<string, string>("orders-to-enrich");
        var enriched = driver.CreateOutputTopic<string, string>("orders-enriched-or-dlq");
        var deadLetters = driver.CreateOutputTopic<string, string>("orders-dead-letter");

        orders.PipeInput("order-1", "c-1");
        orders.PipeInput("order-2", "c-broken");

        CollectionAssert.AreEqual(new[] { "c-1:Alice" }, enriched.ReadValueList().ToList());

        var dead = deadLetters.ReadKeyValueList().ToList();
        Assert.AreEqual(1, dead.Count);
        Assert.AreEqual("order-2", dead[0].Message.Key);
        Assert.AreEqual("c-broken", dead[0].Message.Value);
        // the error is described in headers
        var exception = Encoding.UTF8.GetString(dead[0].Message.Headers.GetLastBytes("__streams.errors.exception"));
        StringAssert.Contains("NoneRetryableException", exception);
        // NumberOfRetry is the total number of attempts, the first call included: 2 calls
        Assert.AreEqual(2, directory.CallsFor("c-broken"));
    }
}

public class ParallelV2ExampleTests
{
    [Test]
    public void PaymentsAreScoredConcurrentlyButInOrderPerAccount()
    {
        var scoring = new FakeFraudScoring();
        using var driver = TestDrivers.For(new ParallelV2Example(scoring));
        var payments = driver.CreateInputTopic<string, string>("payments-to-score");
        var scored = driver.CreateOutputTopic<string, string>("payments-scored");

        // 4 accounts x 3 payments, sent in one batch so the task can start several calls at once
        var batch = Enumerable.Range(1, 4)
            .SelectMany(account => Enumerable.Range(1, 3).Select(n => KeyValuePair.Create($"acc-{account}", $"pay-{account}-{n}")))
            .OrderBy(kv => kv.Value[^1]) // interleave the accounts: pay-1-1, pay-2-1, ..., pay-1-2, ...
            .ToList();
        payments.PipeInputs(batch);

        var results = scored.ReadKeyValueList().Select(r => (r.Message.Key, r.Message.Value)).ToList();
        Assert.AreEqual(12, results.Count);

        // per account, the results come out in the input order, even though the first payment is the slowest
        foreach (var account in results.GroupBy(r => r.Key))
            CollectionAssert.AreEqual(
                new[] { "-1:", "-2:", "-3:" },
                account.Select(r => r.Value.Substring(r.Value.IndexOf(':') - 2, 3)).ToList(),
                account.Key);

        // different accounts ran at the same time, within the MaxConcurrency of 16
        Assert.Greater(scoring.MaxInFlight, 1);
        Assert.LessOrEqual(scoring.MaxInFlight, 16);
    }
}
