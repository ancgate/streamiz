using Streamiz.Examples.Common;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Examples.Basics;

/// <summary>
/// Aggregating a table (KGroupedTable): when a row changes, its old value is subtracted from the old group
/// and its new value is added to the new group. Here: the total balance and number of accounts per branch.
/// </summary>
public class TableAggregate : IExample
{
    public const string InputTopic = "accounts";
    public const string BalanceTopic = "branch-balances";
    public const string CountTopic = "branch-account-counts";
    public const string BalanceStore = "branch-balance-store";
    public const string CountStore = "branch-count-store";

    public string Name => "table-aggregate";
    public string Description => "Total balance and number of accounts per branch, kept right when accounts change branch.";
    public IEnumerable<string> InputTopics => new[] { InputTopic };

    public void BuildTopology(StreamBuilder builder)
    {
        var accounts = builder.Table(InputTopic, new StringSerDes(), new JsonSerDes<Account>(),
            InMemory.As<string, Account>("accounts-store"));

        // GroupBy on a table re-keys each row: (account id, account) becomes (branch, balance).
        // The serdes of the new key and value are needed for the repartition topic.
        accounts
            .GroupBy<string, double, StringSerDes, DoubleSerDes>(
                (accountId, account, _) => KeyValuePair.Create(account.Branch, account.Balance))
            // Aggregate on a grouped table takes an adder AND a subtractor: when an account changes,
            // its old balance is subtracted from its old branch, and its new balance added to its new branch.
            .Aggregate(
                () => 0.0,
                (branch, balance, total) => total + balance,
                (branch, balance, total) => total - balance,
                InMemory.As<string, double>(BalanceStore).WithValueSerdes(new DoubleSerDes()))
            .ToStream()
            .To(BalanceTopic, new StringSerDes(), new DoubleSerDes());

        // Count on a grouped table works the same way: an account moving to another branch is counted
        // out of the old branch and into the new one.
        accounts
            .GroupBy<string, string, StringSerDes, StringSerDes>(
                (accountId, account, _) => KeyValuePair.Create(account.Branch, accountId))
            .Count(InMemory.As<string, long>(CountStore))
            .ToStream()
            .To(CountTopic, new StringSerDes(), new Int64SerDes());
    }

    public void Configure(StreamConfig config)
    {
    }
}
