using Streamiz.Examples.Common;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Examples.ProcessingGuarantees.Examples;

/// <summary>
/// Exactly-once: the outputs, the store changelogs and the consumed offsets are written in one Kafka
/// transaction. After a crash, the transaction is aborted and the records processed again, so each record
/// changes the balances exactly once, as seen by consumers reading with isolation.level=read_committed.
/// Needs a cluster with transactions (3 brokers by default for the transaction log).
/// </summary>
public class ExactlyOnceExample : IExample
{
    public const string StoreName = "balances";

    public string Name => "exactly-once";
    public string Description => "Keeps account balances right after a crash with Kafka transactions.";
    public IEnumerable<string> InputTopics => new[] { "transactions" };

    public void BuildTopology(StreamBuilder builder)
    {
        builder
            // key: account id, value: amount in cents ("+1500", "-200")
            .Stream<string, string>("transactions")
            .GroupByKey()
            // the balance of each account; a duplicate would credit the account twice
            .Aggregate(
                () => 0L,
                (account, amount, balance) => balance + long.Parse(amount),
                InMemory.As<string, long>(StoreName).WithValueSerdes(new Int64SerDes()))
            .ToStream()
            .To("balances", new StringSerDes(), new Int64SerDes());
    }

    public void Configure(StreamConfig config)
    {
        // also sets: read_committed consumers, an idempotent producer, a commit every 100 ms
        // (each commit ends a transaction: the output is visible to read_committed consumers at the commit)
        config.Guarantee = ProcessingGuarantee.EXACTLY_ONCE;
        // a transaction not committed within this time is aborted by the broker
        config.TransactionTimeout = TimeSpan.FromSeconds(10);
    }
}
