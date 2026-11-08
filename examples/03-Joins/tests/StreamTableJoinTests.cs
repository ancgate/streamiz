using NUnit.Framework;
using Streamiz.Examples.Joins.Examples;
using static Streamiz.Examples.Joins.Tests.ExampleDriver;

namespace Streamiz.Examples.Joins.Tests;

public class ClicksProfileJoinTests
{
    [Test]
    public void ClicksAreEnrichedWithTheLatestProfile()
    {
        using var driver = Create(new ClicksProfileJoinExample());
        var profiles = driver.CreateInputTopic<string, string>(ClicksProfileJoinExample.ProfilesTopic);
        var clicks = driver.CreateInputTopic<string, string>(ClicksProfileJoinExample.ClicksTopic);
        var output = driver.CreateOutputTopic<string, string>(ClicksProfileJoinExample.OutputTopic);

        profiles.PipeInput("u1", "Alice;FR");
        clicks.PipeInput("u1", "/home");
        clicks.PipeInput("u2", "/pricing");   // no profile for u2: dropped
        profiles.PipeInput("u1", "Alice;DE"); // a table update alone produces nothing
        clicks.PipeInput("u1", "/checkout");

        CollectionAssert.AreEqual(new[]
        {
            ("u1", "Alice (FR) visited /home"),
            ("u1", "Alice (DE) visited /checkout")
        }, output.ReadAll());
    }
}

public class ClicksProfileLeftJoinTests
{
    [Test]
    public void ClicksOfUnknownUsersAreAnonymous()
    {
        using var driver = Create(new ClicksProfileLeftJoinExample());
        var profiles = driver.CreateInputTopic<string, string>(ClicksProfileLeftJoinExample.ProfilesTopic);
        var clicks = driver.CreateInputTopic<string, string>(ClicksProfileLeftJoinExample.ClicksTopic);
        var output = driver.CreateOutputTopic<string, string>(ClicksProfileLeftJoinExample.OutputTopic);

        profiles.PipeInput("u1", "Alice;FR");
        clicks.PipeInput("u1", "/home");
        clicks.PipeInput("u2", "/pricing");

        CollectionAssert.AreEqual(new[]
        {
            ("u1", "Alice visited /home"),
            ("u2", "anonymous visited /pricing")
        }, output.ReadAll());
    }
}

public class OrdersCatalogGlobalJoinTests
{
    [Test]
    public void OrdersAreEnrichedWithTheProductOfTheirValue()
    {
        using var driver = Create(new OrdersCatalogGlobalJoinExample());
        var catalog = driver.CreateInputTopic<string, string>(OrdersCatalogGlobalJoinExample.CatalogTopic);
        var orders = driver.CreateInputTopic<string, string>(OrdersCatalogGlobalJoinExample.OrdersTopic);
        var output = driver.CreateOutputTopic<string, string>(OrdersCatalogGlobalJoinExample.OutputTopic);

        catalog.PipeInput("p1", "Laptop;999.00");
        catalog.PipeInput("p2", "Mouse;25.50");

        orders.PipeInput("o1", "p1;2");
        orders.PipeInput("o2", "p2;3");
        orders.PipeInput("o3", "p9;1"); // unknown product: dropped by the inner join

        CollectionAssert.AreEqual(new[]
        {
            ("o1", "2 x Laptop = 1998.00"),
            ("o2", "3 x Mouse = 76.50")
        }, output.ReadAll());
    }
}
