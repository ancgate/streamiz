using NUnit.Framework;
using Streamiz.Examples.Joins.Examples;
using static Streamiz.Examples.Joins.Tests.ExampleDriver;

namespace Streamiz.Examples.Joins.Tests;

public class CustomerAccountTableJoinsTests
{
    [Test]
    public void InnerLeftAndOuterJoinsFollowBothTables()
    {
        using var driver = Create(new CustomerAccountTableJoinsExample());
        var customers = driver.CreateInputTopic<string, string>(CustomerAccountTableJoinsExample.CustomersTopic);
        var accounts = driver.CreateInputTopic<string, string>(CustomerAccountTableJoinsExample.AccountsTopic);
        var inner = driver.CreateOutputTopic<string, string>(CustomerAccountTableJoinsExample.InnerTopic);
        var left = driver.CreateOutputTopic<string, string>(CustomerAccountTableJoinsExample.LeftTopic);
        var outer = driver.CreateOutputTopic<string, string>(CustomerAccountTableJoinsExample.OuterTopic);

        customers.PipeInput("c1", "Alice");
        accounts.PipeInput("c1", "100");
        customers.PipeInput("c2", "Bob");   // no account
        accounts.PipeInput("c3", "50");     // no customer
        accounts.PipeInput("c1", "150");    // balance update

        CollectionAssert.AreEqual(new[]
        {
            ("c1", "Alice: 100"),
            ("c1", "Alice: 150")
        }, inner.ReadAll());

        CollectionAssert.AreEqual(new[]
        {
            ("c1", "Alice: no account"),
            ("c1", "Alice: 100"),
            ("c2", "Bob: no account"),
            ("c1", "Alice: 150")
        }, left.ReadAll());

        CollectionAssert.AreEqual(new[]
        {
            ("c1", "Alice: no account"),
            ("c1", "Alice: 100"),
            ("c2", "Bob: no account"),
            ("c3", "unknown customer: 50"),
            ("c1", "Alice: 150")
        }, outer.ReadAll());
    }

    [Test]
    public void DeletingACustomerDeletesTheJoinedRow()
    {
        using var driver = Create(new CustomerAccountTableJoinsExample());
        var customers = driver.CreateInputTopic<string, string>(CustomerAccountTableJoinsExample.CustomersTopic);
        var accounts = driver.CreateInputTopic<string, string>(CustomerAccountTableJoinsExample.AccountsTopic);
        var inner = driver.CreateOutputTopic<string, string>(CustomerAccountTableJoinsExample.InnerTopic);

        customers.PipeInput("c1", "Alice");
        accounts.PipeInput("c1", "100");
        customers.PipeInput("c1", null);    // tombstone: the customer is deleted

        CollectionAssert.AreEqual(new[]
        {
            ("c1", "Alice: 100"),
            ("c1", (string)null)               // the joined row is deleted too
        }, inner.ReadAll());
    }
}

public class OrdersProductsForeignKeyJoinTests
{
    [Test]
    public void OrdersFollowTheirProductAndTheirForeignKey()
    {
        using var driver = Create(new OrdersProductsForeignKeyJoinExample());
        var orders = driver.CreateInputTopic<string, string>(OrdersProductsForeignKeyJoinExample.OrdersTopic);
        var products = driver.CreateInputTopic<string, string>(OrdersProductsForeignKeyJoinExample.ProductsTopic);
        var output = driver.CreateOutputTopic<string, string>(OrdersProductsForeignKeyJoinExample.OutputTopic);

        products.PipeInput("p1", "Laptop");
        products.PipeInput("p2", "Mouse");

        orders.PipeInput("o1", "p1;2");
        products.PipeInput("p1", "Laptop Pro"); // the product changes: the order is updated
        orders.PipeInput("o1", "p2;1");         // the order now references another product
        orders.PipeInput("o2", "p9;1");         // unknown product: no result for an inner join

        CollectionAssert.AreEqual(new[]
        {
            ("o1", "2 x Laptop"),
            ("o1", "2 x Laptop Pro"),
            ("o1", "1 x Mouse")
        }, output.ReadAll());
    }
}

public class OrdersProductsForeignKeyLeftJoinTests
{
    [Test]
    public void OrdersOfUnknownProductsAreEmittedAndCompletedLater()
    {
        using var driver = Create(new OrdersProductsForeignKeyLeftJoinExample());
        var orders = driver.CreateInputTopic<string, string>(OrdersProductsForeignKeyLeftJoinExample.OrdersTopic);
        var products = driver.CreateInputTopic<string, string>(OrdersProductsForeignKeyLeftJoinExample.ProductsTopic);
        var output = driver.CreateOutputTopic<string, string>(OrdersProductsForeignKeyLeftJoinExample.OutputTopic);

        orders.PipeInput("o2", "p9;1");          // the product doesn't exist yet
        products.PipeInput("p9", "Keyboard");    // it arrives: the order is completed

        CollectionAssert.AreEqual(new[]
        {
            ("o2", "1 x unknown product"),
            ("o2", "1 x Keyboard")
        }, output.ReadAll());
    }
}
