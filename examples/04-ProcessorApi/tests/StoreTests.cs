using NUnit.Framework;
using Streamiz.Examples.ProcessorApi.Examples;
using static Streamiz.Examples.ProcessorApi.Tests.ExampleDriver;

namespace Streamiz.Examples.ProcessorApi.Tests;

public class CustomStateStoreTests
{
    [Test]
    public void PageViewsAreCountedInTheCustomStore()
    {
        using var driver = Create(new CustomStateStoreExample());
        var views = driver.CreateInputTopic<string, string>(CustomStateStoreExample.PageViewsTopic);
        var output = driver.CreateOutputTopic<string, string>(CustomStateStoreExample.OutputTopic);

        views.PipeInput("/home", "u1");
        views.PipeInput("/home", "u2");
        views.PipeInput("/pricing", "u1");

        CollectionAssert.AreEqual(new[]
        {
            ("/home", "1"),
            ("/home", "2"),
            ("/pricing", "1")
        }, output.ReadAll());

        // The store can be queried like any other key-value store.
        var store = driver.GetKeyValueStore<string, long>(CustomStateStoreExample.StoreName);
        Assert.AreEqual(2, store.Get("/home"));
        Assert.AreEqual(1, store.Get("/pricing"));
    }

    [Test]
    public void TheStoreKeepsKeysInByteOrder()
    {
        var store = new SortedDictionaryKeyValueStore("test");
        store.Put(Kafka.Net.Crosscutting.Bytes.Wrap(new byte[] { 2 }), new byte[] { 20 });
        store.Put(Kafka.Net.Crosscutting.Bytes.Wrap(new byte[] { 1 }), new byte[] { 10 });
        store.Put(Kafka.Net.Crosscutting.Bytes.Wrap(new byte[] { 3 }), null); // null = delete: nothing stored

        CollectionAssert.AreEqual(new byte[] { 1, 2 }, store.All().Select(kv => kv.Key.Get[0]).ToArray());
        Assert.AreEqual(2, store.ApproximateNumEntries());
    }
}

public class CurrencyRatesGlobalStoreTests
{
    [Test]
    public void PaymentsAreConvertedWithTheGlobalRates()
    {
        using var driver = Create(new CurrencyRatesGlobalStoreExample());
        var rates = driver.CreateInputTopic<string, string>(CurrencyRatesGlobalStoreExample.RatesTopic);
        var payments = driver.CreateInputTopic<string, string>(CurrencyRatesGlobalStoreExample.PaymentsTopic);
        var output = driver.CreateOutputTopic<string, string>(CurrencyRatesGlobalStoreExample.OutputTopic);

        rates.PipeInput("USD", "0.92");
        payments.PipeInput("pay-1", "USD;100.00");
        payments.PipeInput("pay-2", "GBP;10.00");   // no GBP rate yet
        rates.PipeInput("GBP", "1.17");
        payments.PipeInput("pay-3", "GBP;10.00");

        CollectionAssert.AreEqual(new[]
        {
            ("pay-1", "92.00 EUR"),
            ("pay-2", "10.00 GBP (no rate)"),
            ("pay-3", "11.70 EUR")
        }, output.ReadAll());
    }
}

public class GlobalTableLookupTests
{
    [Test]
    public void OrdersAreDescribedWithTheProductFromTheGlobalTable()
    {
        using var driver = Create(new GlobalTableLookupExample());
        var catalog = driver.CreateInputTopic<string, string>(GlobalTableLookupExample.CatalogTopic);
        var orders = driver.CreateInputTopic<string, string>(GlobalTableLookupExample.OrdersTopic);
        var output = driver.CreateOutputTopic<string, string>(GlobalTableLookupExample.OutputTopic);

        catalog.PipeInput("p1", "Laptop");
        orders.PipeInput("o1", "p1;2");
        orders.PipeInput("o2", "p9;1");

        CollectionAssert.AreEqual(new[]
        {
            ("o1", "2 x Laptop"),
            ("o2", "1 x unknown product p9")
        }, output.ReadAll());
    }
}
