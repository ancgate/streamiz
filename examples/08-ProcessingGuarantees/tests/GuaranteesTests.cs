using Confluent.Kafka;
using NUnit.Framework;
using Streamiz.Examples.Common;
using Streamiz.Examples.ProcessingGuarantees.Examples;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;

namespace Streamiz.Examples.ProcessingGuarantees.Tests;

/// <summary>
/// What each guarantee changes in the configuration, and the results of the topologies.
/// The guarantees themselves are about crashes and Kafka transactions, which the TopologyTestDriver doesn't
/// simulate (it always runs at-least-once): the README explains how to observe them on a real cluster.
/// </summary>
public class GuaranteesTests
{
    private static TopologyTestDriver Driver(IExample example)
    {
        var builder = new StreamBuilder();
        example.BuildTopology(builder);
        return new TopologyTestDriver(builder.Build(), ExampleRunner.CreateConfig(example));
    }

    [Test]
    public void AtLeastOnceIsTheDefault()
    {
        Assert.AreEqual(ProcessingGuarantee.AT_LEAST_ONCE, new StreamConfig().Guarantee);

        var config = ExampleRunner.CreateConfig(new AtLeastOnceExample());
        Assert.AreEqual(ProcessingGuarantee.AT_LEAST_ONCE, config.Guarantee);
        Assert.AreEqual(1000, config.CommitIntervalMs);
    }

    [Test]
    public void ExactlyOnceUsesTransactions()
    {
        var config = ExampleRunner.CreateConfig(new ExactlyOnceExample());

        Assert.AreEqual(ProcessingGuarantee.EXACTLY_ONCE, config.Guarantee);
        // consumers only read committed transactions
        Assert.AreEqual(IsolationLevel.ReadCommitted, config.IsolationLevel);
        // the producer is idempotent and the transactions are short
        Assert.IsTrue(config.EnableIdempotence);
        Assert.AreEqual(StreamConfig.EOS_DEFAULT_COMMIT_INTERVAL_MS, config.CommitIntervalMs);
    }

    [Test]
    public void AtMostOnceNeverRetries()
    {
        var config = ExampleRunner.CreateConfig(new AtMostOnceExample());

        Assert.AreEqual(ProcessingGuarantee.AT_MOST_ONCE, config.Guarantee);
        Assert.IsFalse(config.EnableIdempotence);
        Assert.AreEqual(0, config.MessageSendMaxRetries);
    }

    [Test]
    public void TheLedgerKeepsThePaymentIds()
    {
        using var driver = Driver(new AtLeastOnceExample());
        var payments = driver.CreateInputTopic<string, string>("payments");
        var ledger = driver.CreateOutputTopic<string, string>("ledger");

        payments.PipeInput("pay-1", "acc-1;1500");

        var entry = ledger.ReadKeyValue();
        Assert.AreEqual("pay-1", entry.Message.Key);
        Assert.AreEqual("ledger-entry|pay-1|acc-1;1500", entry.Message.Value);
    }

    [Test]
    public void TheBalancesAreTheSumOfTheTransactions()
    {
        using var driver = Driver(new ExactlyOnceExample());
        var transactions = driver.CreateInputTopic<string, string>("transactions");
        var balances = driver.CreateOutputTopic<string, long, StringSerDes, Int64SerDes>("balances");

        transactions.PipeInput("acc-1", "+1500");
        transactions.PipeInput("acc-1", "-200");
        transactions.PipeInput("acc-2", "+700");

        driver.Commit();
        var latest = balances.ReadKeyValueList()
            .GroupBy(r => r.Message.Key)
            .ToDictionary(g => g.Key, g => g.Last().Message.Value);

        Assert.AreEqual(1300L, latest["acc-1"]);
        Assert.AreEqual(700L, latest["acc-2"]);
    }

    [Test]
    public void ANotificationPerShippedOrder()
    {
        using var driver = Driver(new AtMostOnceExample());
        var shipped = driver.CreateInputTopic<string, string>("order-shipped");
        var notifications = driver.CreateOutputTopic<string, string>("push-notifications");

        shipped.PipeInput("customer-1", "order-42");

        Assert.AreEqual("Your order order-42 is on its way!", notifications.ReadKeyValue().Message.Value);
    }
}
