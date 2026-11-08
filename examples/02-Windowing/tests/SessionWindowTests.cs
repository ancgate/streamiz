using NUnit.Framework;
using Streamiz.Kafka.Net.SerDes;
using static Streamiz.Examples.Windowing.Tests.ExampleDriver;

namespace Streamiz.Examples.Windowing.Tests;

public class SessionCountTests
{
    [Test]
    public void ARecordWithinTheGapExtendsTheSession()
    {
        using var driver = Create(new SessionCount());
        var views = driver.CreateInputTopic<string, string>(SessionCount.InputTopic);
        var sessions = driver.CreateOutputTopic(SessionCount.OutputTopic, TimeSpan.FromSeconds(1),
            new SessionWindowedSerDes<string>(new StringSerDes()), new NullableInt64SerDes());

        views.PipeInput("alice", "/home", T0);
        views.PipeInput("alice", "/products", T0 + Min(2));  // within 5 minutes: same visit
        views.PipeInput("alice", "/home", T0 + Min(20));     // 18 minutes later: a new visit

        CollectionAssert.AreEqual(
            new (string, TimeSpan, TimeSpan, long?)[]
            {
                ("alice", Min(0), Min(0), 1L),
                ("alice", Min(0), Min(0), null),   // the one-record session is replaced...
                ("alice", Min(0), Min(2), 2L),     // ...by the extended one
                ("alice", Min(20), Min(20), 1L),
            },
            sessions.ReadKeyValueList().Select(Read).ToArray());
    }

    [Test]
    public void ARecordBetweenTwoSessionsMergesThem()
    {
        using var driver = Create(new SessionCount());
        var views = driver.CreateInputTopic<string, string>(SessionCount.InputTopic);
        var sessions = driver.CreateOutputTopic(SessionCount.OutputTopic, TimeSpan.FromSeconds(1),
            new SessionWindowedSerDes<string>(new StringSerDes()), new NullableInt64SerDes());

        views.PipeInput("bob", "/home", T0);
        views.PipeInput("bob", "/cart", T0 + Min(8));   // 8 minutes later: a second session
        views.PipeInput("bob", "/search", T0 + Min(4)); // out of order, within 5 minutes of both

        CollectionAssert.AreEqual(
            new (string, TimeSpan, TimeSpan, long?)[]
            {
                ("bob", Min(0), Min(0), 1L),
                ("bob", Min(8), Min(8), 1L),
                ("bob", Min(0), Min(0), null),  // both sessions are deleted...
                ("bob", Min(8), Min(8), null),
                ("bob", Min(0), Min(8), 3L),    // ...and replaced by the merged session
            },
            sessions.ReadKeyValueList().Select(Read).ToArray());

        var store = driver.GetSessionStore<string, long?>(SessionCount.StoreName);
        Assert.AreEqual(3L, store.FetchSession("bob", Ms(T0), Ms(T0 + Min(8))));
    }
}

public class SessionCartTotalTests
{
    [Test]
    public void TheMergerCombinesTheCartsOfTwoSessions()
    {
        using var driver = Create(new SessionCartTotal());
        var additions = driver.CreateInputTopic(SessionCartTotal.InputTopic, new StringSerDes(), new JsonSerDes<CartItem>());
        var carts = driver.CreateOutputTopic(SessionCartTotal.OutputTopic, TimeSpan.FromSeconds(1),
            new SessionWindowedSerDes<string>(new StringSerDes()), new JsonSerDes<CartSummary>());

        additions.PipeInput("alice", new CartItem { Product = "book", Price = 10 }, T0);
        additions.PipeInput("alice", new CartItem { Product = "lamp", Price = 30 }, T0 + Min(15)); // new session
        additions.PipeInput("alice", new CartItem { Product = "pen", Price = 2 }, T0 + Min(8));    // bridges both

        var records = carts.ReadKeyValueList().Select(Read).ToList();
        CollectionAssert.AreEqual(
            new[]
            {
                (Min(0), Min(0), "book"),
                (Min(15), Min(15), "lamp"),
                (Min(0), Min(0), null),
                (Min(15), Min(15), null),
                (Min(0), Min(15), "book,lamp,pen"),
            },
            records.Select(r => (r.Start, r.End, r.Value == null ? null : string.Join(",", r.Value.Items))).ToArray());
        Assert.AreEqual(42.0, records.Last().Value.Total);
    }
}

public class SessionLastPageTests
{
    [Test]
    public void TheLastPageOfTheVisitIsKept()
    {
        using var driver = Create(new SessionLastPage());
        var views = driver.CreateInputTopic<string, string>(SessionLastPage.InputTopic);
        var exitPages = driver.CreateOutputTopic(SessionLastPage.OutputTopic, TimeSpan.FromSeconds(1),
            new SessionWindowedSerDes<string>(new StringSerDes()), new StringSerDes());

        views.PipeInput("alice", "/home", T0);
        views.PipeInput("alice", "/products", T0 + Min(1));
        views.PipeInput("alice", "/checkout", T0 + Min(3));

        var last = exitPages.ReadKeyValueList().Select(Read).Last();
        Assert.AreEqual(("alice", Min(0), Min(3), "/checkout"), last);
    }
}
