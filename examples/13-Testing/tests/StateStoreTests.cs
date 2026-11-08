using NUnit.Framework;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;

namespace Streamiz.Examples.Testing.Tests;

/// <summary>
/// Stateful topologies: besides the output topics, assert on the content of the state stores.
/// The driver gives read-only access to every kind of store.
/// </summary>
public class StateStoreTests
{
    [Test]
    public void KeyValueStore()
    {
        using var driver = Drivers.Create(RunningTotals.Build);
        var deposits = driver.CreateInputTopic<string, long, StringSerDes, Int64SerDes>(RunningTotals.Input);

        deposits.PipeInput("acc-1", 100);
        deposits.PipeInput("acc-2", 20);
        deposits.PipeInput("acc-1", 50);

        var store = driver.GetKeyValueStore<string, long>(RunningTotals.Store);
        Assert.AreEqual(150, store.Get("acc-1"));
        Assert.AreEqual(20, store.Get("acc-2"));
        Assert.AreEqual(2, store.All().Count());
    }

    [Test]
    public void WindowStore()
    {
        using var driver = Drivers.Create(ClicksPerMinute.Build);
        var clicks = driver.CreateInputTopic<string, string>(ClicksPerMinute.Input);

        clicks.PipeInput("home", "c1", Drivers.Start.AddSeconds(5));
        clicks.PipeInput("home", "c2", Drivers.Start.AddSeconds(40));
        clicks.PipeInput("home", "c3", Drivers.Start.AddSeconds(70));

        // the store holds every window, even the ones not emitted yet by the suppress operator
        var store = driver.GetWindowStore<string, long>(ClicksPerMinute.Store);
        var windows = store.Fetch("home", Drivers.Start, Drivers.Start.AddMinutes(5)).ToList();
        CollectionAssert.AreEqual(
            new[]
            {
                (Drivers.Start.GetMilliseconds(), 2L),            // [10:00, 10:01)
                (Drivers.Start.AddMinutes(1).GetMilliseconds(), 1L) // [10:01, 10:02)
            },
            windows.Select(w => (w.Key, w.Value)).ToList());
    }

    [Test]
    public void SessionStore()
    {
        using var driver = Drivers.Create(UserSessions.Build);
        var activity = driver.CreateInputTopic<string, string>(UserSessions.Input);

        activity.PipeInput("alice", "login", Drivers.Start);
        activity.PipeInput("alice", "search", Drivers.Start.AddMinutes(2));
        activity.PipeInput("alice", "login", Drivers.Start.AddMinutes(30)); // more than 5 minutes later: a new session

        var sessions = driver.GetSessionStore<string, long?>(UserSessions.Store).Fetch("alice").ToList();
        CollectionAssert.AreEqual(
            new[]
            {
                (Drivers.Start.GetMilliseconds(), Drivers.Start.AddMinutes(2).GetMilliseconds(), 2L),
                (Drivers.Start.AddMinutes(30).GetMilliseconds(), Drivers.Start.AddMinutes(30).GetMilliseconds(), 1L)
            },
            sessions.Select(s => (s.Key.Window.StartMs, s.Key.Window.EndMs, s.Value.Value)).ToList());
    }

    [Test]
    public void VersionedKeyValueStore()
    {
        using var driver = Drivers.Create(PriceAtOrderTime.Build);
        var prices = driver.CreateInputTopic<string, string>(PriceAtOrderTime.Prices);

        prices.PipeInput("coffee", "4.00", Drivers.Start);
        prices.PipeInput("coffee", "4.50", Drivers.Start.AddMinutes(30));

        var store = driver.GetVersionedKeyValueStore<string, string>(PriceAtOrderTime.Store);
        // latest value, and the value as of a past time
        Assert.AreEqual("4.50", store.Get("coffee").Value);
        var at1015 = store.Get("coffee", Drivers.Start.AddMinutes(15).GetMilliseconds());
        Assert.AreEqual("4.00", at1015.Value);
        Assert.AreEqual(Drivers.Start.AddMinutes(30).GetMilliseconds(), at1015.ValidTo);
    }
}
