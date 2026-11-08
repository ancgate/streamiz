using Streamiz.Examples.Common;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Examples.Joins.Examples;

/// <summary>
/// Table-table joins: the inner, left and outer join of customers and their accounts, kept up to date.
/// </summary>
public class CustomerAccountTableJoinsExample : IExample
{
    public const string CustomersTopic = "customers";
    public const string AccountsTopic = "accounts";
    public const string InnerTopic = "customer-accounts-inner";
    public const string LeftTopic = "customer-accounts-left";
    public const string OuterTopic = "customer-accounts-outer";

    public string Name => "customer-account-table-joins";

    public string Description => "Table-table inner, left and outer joins: customers with their account balance, updated on each change of either side.";

    public IEnumerable<string> InputTopics => new[] { CustomersTopic, AccountsTopic };

    public void BuildTopology(StreamBuilder builder)
    {
        // Both tables are keyed by customer id: name and balance.
        var customers = builder.Table(CustomersTopic, InMemory.As<string, string>("customers-store"));
        var accounts = builder.Table(AccountsTopic, InMemory.As<string, string>("accounts-store"));

        // A table-table join is itself a table: it's updated whenever either side changes,
        // and emits a tombstone (null value) when a joined row disappears.
        // ToStream() turns its changes into records written to a topic.

        // Inner join: only customers that have an account.
        customers.Join(accounts, (name, balance) => $"{name}: {balance}")
            .ToStream()
            .To(InnerTopic);

        // Left join: every customer, with "no account" until the account exists.
        customers.LeftJoin(accounts, (name, balance) => $"{name}: {balance ?? "no account"}")
            .ToStream()
            .To(LeftTopic);

        // Outer join: customers and accounts, whichever side exists.
        customers.OuterJoin(accounts, (name, balance) => $"{name ?? "unknown customer"}: {balance ?? "no account"}")
            .ToStream()
            .To(OuterTopic);
    }

    public void Configure(StreamConfig config)
    {
    }
}
