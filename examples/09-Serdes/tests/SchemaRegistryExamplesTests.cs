using NUnit.Framework;
using Streamiz.Examples.Serdes.Examples;
using Streamiz.Examples.Serdes.Model;
using Streamiz.Examples.Serdes.Model.Proto;
using Streamiz.Kafka.Net.SchemaRegistry.SerDes.Avro;
using Streamiz.Kafka.Net.SchemaRegistry.SerDes.Json;
using Streamiz.Kafka.Net.SchemaRegistry.SerDes.Mock;
using Streamiz.Kafka.Net.SchemaRegistry.SerDes.Protobuf;
using Streamiz.Kafka.Net.SerDes;

namespace Streamiz.Examples.Serdes.Tests;

/// <summary>
/// With a "mock://scope" url, the Schema Registry serdes use an in-memory registry: no server is needed.
/// </summary>
public class SchemaRegistryExamplesTests
{
    [TearDown]
    public void DropMockRegistries() => MockSchemaRegistry.DropAllScope();

    [Test]
    public void JsonSchemaOrdersAreFilteredByCurrency()
    {
        using var driver = TestDrivers.For(new SchemaRegistryJsonExample(),
            config => config.SchemaRegistryUrl = "mock://serdes-json");
        var orders = driver.CreateInputTopic<string, Order, StringSerDes, SchemaJsonSerDes<Order>>("orders-json");
        var eurOrders = driver.CreateOutputTopic<string, Order, StringSerDes, SchemaJsonSerDes<Order>>("orders-json-eur");

        orders.PipeInput("o-1", new Order { Id = "o-1", Customer = "alice", Amount = 30, Currency = "EUR" });
        orders.PipeInput("o-2", new Order { Id = "o-2", Customer = "bob", Amount = 250, Currency = "USD" });

        var results = eurOrders.ReadValueList().ToList();
        Assert.AreEqual(1, results.Count);
        Assert.AreEqual("o-1", results[0].Id);
        Assert.AreEqual("alice", results[0].Customer);
    }

    [Test]
    public void FailedAvroPaymentsAreRouted()
    {
        using var driver = TestDrivers.For(new SchemaRegistryAvroExample(),
            config => config.SchemaRegistryUrl = "mock://serdes-avro");
        var payments = driver.CreateInputTopic<string, Payment, StringSerDes, SchemaAvroSerDes<Payment>>("payments-avro");
        var failed = driver.CreateOutputTopic<string, Payment, StringSerDes, SchemaAvroSerDes<Payment>>("payments-failed");

        payments.PipeInput("p-1", new Payment { id = "p-1", account = "acc-1", amount = 12.5, status = "SETTLED" });
        payments.PipeInput("p-2", new Payment { id = "p-2", account = "acc-2", amount = 99.9, status = "FAILED" });

        var results = failed.ReadKeyValueList().ToList();
        Assert.AreEqual(1, results.Count);
        Assert.AreEqual("p-2", results[0].Message.Key);
        Assert.AreEqual("acc-2", results[0].Message.Value.account);
        Assert.AreEqual(99.9, results[0].Message.Value.amount);
    }

    [Test]
    public void VipProtobufCustomersAreKeyedByCountry()
    {
        using var driver = TestDrivers.For(new SchemaRegistryProtobufExample(),
            config => config.SchemaRegistryUrl = "mock://serdes-protobuf");
        var customers = driver.CreateInputTopic<string, Customer, StringSerDes, SchemaProtobufSerDes<Customer>>("customers-proto");
        var vips = driver.CreateOutputTopic<string, Customer, StringSerDes, SchemaProtobufSerDes<Customer>>("vip-customers-by-country");

        customers.PipeInput("c-1", new Customer { Id = "c-1", Name = "Alice", Country = "FR", Vip = true });
        customers.PipeInput("c-2", new Customer { Id = "c-2", Name = "Bob", Country = "US", Vip = false });

        var results = vips.ReadKeyValueList().ToList();
        Assert.AreEqual(1, results.Count);
        Assert.AreEqual("FR", results[0].Message.Key);
        Assert.AreEqual("Alice", results[0].Message.Value.Name);
    }
}
