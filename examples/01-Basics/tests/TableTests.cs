using NUnit.Framework;
using Streamiz.Kafka.Net.SerDes;

namespace Streamiz.Examples.Basics.Tests;

public class TableBasicsTests
{
    [Test]
    public void UpdatesReplaceTheValueAndTombstonesDeleteTheKey()
    {
        using var driver = ExampleDriver.Create(new TableBasics());
        var users = driver.CreateInputTopic<string, string>(TableBasics.InputTopic);
        var known = driver.CreateOutputTopic(TableBasics.OutputTopic, TimeSpan.FromSeconds(1),
            new StringSerDes(), new StringSerDes());

        users.PipeInput("u1", "fr");
        users.PipeInput("u2", "de");
        users.PipeInput("u1", "es");      // update: u1 moved to Spain
        users.PipeInput("u2", "unknown"); // filtered out: u2 is removed from the result
        users.PipeInput("u1", null);      // tombstone: u1 is deleted

        CollectionAssert.AreEqual(
            new[] { ("u1", "FR"), ("u2", "DE"), ("u1", "ES"), ("u2", null), ("u1", null) },
            known.ReadKeyValueList().Select(r => (r.Message.Key, r.Message.Value)).ToArray());

        // the source table keeps the raw latest values: u2 is "unknown", u1 is deleted
        var store = driver.GetKeyValueStore<string, string>(TableBasics.StoreName);
        Assert.AreEqual("unknown", store.Get("u2"));
        Assert.IsNull(store.Get("u1"));
    }
}

public class TableAggregateTests
{
    [Test]
    public void AnAccountMovingToAnotherBranchIsSubtractedFromTheOldOne()
    {
        using var driver = ExampleDriver.Create(new TableAggregate());
        var accounts = driver.CreateInputTopic(TableAggregate.InputTopic, new StringSerDes(), new JsonSerDes<Account>());

        accounts.PipeInput("A1", new Account { Branch = "paris", Balance = 100 });
        accounts.PipeInput("A2", new Account { Branch = "paris", Balance = 50 });
        // A1 moves to Lyon with a new balance: 100 is subtracted from Paris, 120 is added to Lyon
        accounts.PipeInput("A1", new Account { Branch = "lyon", Balance = 120 });

        var balances = driver.GetKeyValueStore<string, double>(TableAggregate.BalanceStore);
        Assert.AreEqual(50.0, balances.Get("paris"));
        Assert.AreEqual(120.0, balances.Get("lyon"));

        var counts = driver.GetKeyValueStore<string, long>(TableAggregate.CountStore);
        Assert.AreEqual(1L, counts.Get("paris"));
        Assert.AreEqual(1L, counts.Get("lyon"));

        // deleting A2 (a tombstone) removes it from its branch
        accounts.PipeInput("A2", null);
        Assert.AreEqual(0.0, balances.Get("paris"));
        Assert.AreEqual(0L, counts.Get("paris"));
    }

    [Test]
    public void EveryChangeOfABranchIsEmitted()
    {
        using var driver = ExampleDriver.Create(new TableAggregate());
        var accounts = driver.CreateInputTopic(TableAggregate.InputTopic, new StringSerDes(), new JsonSerDes<Account>());
        var balances = driver.CreateOutputTopic(TableAggregate.BalanceTopic, TimeSpan.FromSeconds(1),
            new StringSerDes(), new DoubleSerDes());

        accounts.PipeInput("A1", new Account { Branch = "paris", Balance = 100 });
        accounts.PipeInput("A2", new Account { Branch = "paris", Balance = 50 });
        accounts.PipeInput("A1", new Account { Branch = "lyon", Balance = 120 });

        // the subtraction from the old branch comes first, then the addition to the new one
        CollectionAssert.AreEqual(
            new[] { ("paris", 100.0), ("paris", 150.0), ("paris", 50.0), ("lyon", 120.0) },
            balances.ReadKeyValueList().Select(r => (r.Message.Key, r.Message.Value)).ToArray());
    }
}
