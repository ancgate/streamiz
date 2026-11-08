using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using NUnit.Framework;
using Streamiz.Examples.Hosting;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;

namespace Streamiz.Examples.Hosting.Tests;

public class OrderTotalsTopologyTests
{
    private class FixedPrices : IPriceCatalog
    {
        public double PriceOf(string sku) => sku == "coffee" ? 4.0 : 0.5;
    }

    [Test]
    public void TotalsAreSummedPerCustomerWithInjectedPrices()
    {
        // the topology class is tested without the host: inject a fake catalog
        var topology = new OrderTotalsTopology(new FixedPrices()).Build();
        var config = new StreamizSettings { ApplicationId = "hosting-test" }.ToStreamConfig();

        using var driver = new TopologyTestDriver(topology, config);
        var orders = driver.CreateInputTopic<string, string>(OrderTotalsTopology.InputTopic);
        var totals = driver.CreateOutputTopic<string, double, StringSerDes, DoubleSerDes>(OrderTotalsTopology.OutputTopic);

        orders.PipeInput("alice", "coffee:2");
        orders.PipeInput("bob", "apple:4");
        orders.PipeInput("alice", "apple:1");

        var results = totals.ReadKeyValueList().Select(r => (r.Message.Key, r.Message.Value)).ToList();
        CollectionAssert.AreEqual(new[] { ("alice", 8.0), ("bob", 2.0), ("alice", 8.5) }, results);
        Assert.AreEqual(8.5, driver.GetKeyValueStore<string, double>(OrderTotalsTopology.StoreName).Get("alice"));
    }
}

public class StreamHealthTests
{
    [TestCase("RUNNING", true, 200)]
    [TestCase("REBALANCING", true, 200)]
    [TestCase("CREATED", false, 503)]
    [TestCase("PENDING_SHUTDOWN", false, 503)]
    [TestCase("NOT_RUNNING", false, 503)]
    [TestCase("ERROR", false, 503)]
    public void StateIsMappedToAHealthStatus(string stateName, bool healthy, int httpStatus)
    {
        var state = new[]
        {
            KafkaStream.State.CREATED, KafkaStream.State.REBALANCING, KafkaStream.State.RUNNING,
            KafkaStream.State.PENDING_SHUTDOWN, KafkaStream.State.NOT_RUNNING, KafkaStream.State.ERROR
        }.Single(s => s.ToString() == stateName);

        var status = StreamHealth.Evaluate(state);

        Assert.AreEqual(stateName, status.State);
        Assert.AreEqual(healthy, status.Healthy);
        Assert.AreEqual(httpStatus, status.HttpStatusCode);
    }

    [Test]
    public void HealthFollowsTheStateChangedEvents()
    {
        var health = new StreamHealth();
        Assert.IsFalse(health.Current.Healthy);

        health.OnStateChanged(KafkaStream.State.CREATED, KafkaStream.State.REBALANCING);
        health.OnStateChanged(KafkaStream.State.REBALANCING, KafkaStream.State.RUNNING);

        Assert.AreEqual("RUNNING", health.Current.State);
        Assert.IsTrue(health.Current.Healthy);
    }
}

public class ServiceRegistrationTests
{
    [Test]
    public void SettingsAndServicesComeFromTheConfiguration()
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string>
            {
                ["Streamiz:ApplicationId"] = "orders-app",
                ["Streamiz:BootstrapServers"] = "broker:9092",
                ["Streamiz:NumStreamThreads"] = "3",
                ["Streamiz:Guarantee"] = "EXACTLY_ONCE",
                ["Prices:coffee"] = "4.2"
            })
            .Build();

        var services = new ServiceCollection()
            .AddSingleton<IConfiguration>(configuration)
            .AddLogging()
            .AddOrderTotalsStream(configuration)
            .BuildServiceProvider();

        var config = services.GetRequiredService<StreamizSettings>().ToStreamConfig();
        Assert.AreEqual("orders-app", config.ApplicationId);
        Assert.AreEqual("broker:9092", config.BootstrapServers);
        Assert.AreEqual(3, config.NumStreamThreads);
        Assert.AreEqual(ProcessingGuarantee.EXACTLY_ONCE, config.Guarantee);

        Assert.AreEqual(4.2, services.GetRequiredService<IPriceCatalog>().PriceOf("coffee"));
        // the hosted service is the same instance as the one the endpoints use to reach the stream
        Assert.AreSame(services.GetRequiredService<StreamizHostedService>(),
            services.GetServices<IHostedService>().OfType<StreamizHostedService>().Single());
    }
}
