using System.Globalization;
using Streamiz.Examples.Common;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.Processors.Public;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Examples.ProcessorApi.Examples;

/// <summary>
/// A global store fed by your own processor, read by another processor: convert payments to EUR.
/// </summary>
public class CurrencyRatesGlobalStoreExample : IExample
{
    public const string RatesTopic = "currency-rates";
    public const string PaymentsTopic = "payments";
    public const string OutputTopic = "payments-in-eur";
    public const string StoreName = "currency-rates-store";

    public string Name => "global-store-currency-rates";

    public string Description => "Global store with AddGlobalStore: keep currency rates on every instance and convert payments to EUR.";

    public IEnumerable<string> InputTopics => new[] { RatesTopic, PaymentsTopic };

    public void BuildTopology(StreamBuilder builder)
    {
        // A global store reads ALL the partitions of its topic on every instance, through the given processor.
        // Its logging is always disabled: the source topic itself is used to restore it.
        builder.AddGlobalStore(
            Stores.KeyValueStoreBuilder(Stores.InMemoryKeyValueStore(StoreName), new StringSerDes(), new StringSerDes()),
            RatesTopic,
            ProcessorBuilder.New<string, string>().Processor<RatesUpdater>().Build(),
            new StringSerDes(),
            new StringSerDes());

        // Payments are keyed by payment id, value "currency;amount".
        // Any processor can read a global store by its name, without connecting it.
        builder.Stream<string, string>(PaymentsTopic)
            .TransformValues(TransformerBuilder.New<string, string, string, string>()
                .Transformer<EurConverter>()
                .Build())
            .To(OutputTopic);
    }

    public void Configure(StreamConfig config)
    {
    }
}

/// <summary>
/// Updates the global store with each rate (currency -> EUR rate). Deleting a currency uses a null value.
/// </summary>
public class RatesUpdater : IProcessor<string, string>
{
    private IKeyValueStore<string, string> rates;

    public void Init(ProcessorContext<string, string> context)
    {
        rates = (IKeyValueStore<string, string>)context.GetStateStore(CurrencyRatesGlobalStoreExample.StoreName);
    }

    public void Process(Record<string, string> record)
    {
        if (record.Value == null)
            rates.Delete(record.Key);
        else
            rates.Put(record.Key, record.Value);
    }

    public void Close()
    {
    }
}

/// <summary>
/// "USD;100.00" -> "92.00 EUR" with a USD rate of 0.92. Unknown currencies are kept as they are.
/// </summary>
public class EurConverter : ITransformer<string, string, string, string>
{
    private IKeyValueStore<string, string> rates;

    public void Init(ProcessorContext<string, string> context)
    {
        rates = (IKeyValueStore<string, string>)context.GetStateStore(CurrencyRatesGlobalStoreExample.StoreName);
    }

    public Record<string, string> Process(Record<string, string> record)
    {
        var parts = record.Value.Split(';');
        var rate = rates.Get(parts[0]);
        if (rate == null)
            return Record<string, string>.Create(record.Key, $"{parts[1]} {parts[0]} (no rate)");

        var eur = decimal.Parse(parts[1], CultureInfo.InvariantCulture) * decimal.Parse(rate, CultureInfo.InvariantCulture);
        return Record<string, string>.Create(record.Key, $"{eur.ToString("0.00", CultureInfo.InvariantCulture)} EUR");
    }

    public void Close()
    {
    }
}

/// <summary>
/// A global table read from a processor: look up the product of each order in the global table's store.
/// </summary>
public class GlobalTableLookupExample : IExample
{
    public const string CatalogTopic = "product-catalog";
    public const string OrdersTopic = "orders";
    public const string OutputTopic = "orders-described";
    public const string StoreName = "product-catalog-store";

    public string Name => "global-table-lookup";

    public string Description => "Read a global table's store from a processor: describe each order with the product it references.";

    public IEnumerable<string> InputTopics => new[] { CatalogTopic, OrdersTopic };

    public void BuildTopology(StreamBuilder builder)
    {
        // The global table is materialized in a named store, which processors can read.
        builder.GlobalTable(CatalogTopic, InMemory.As<string, string>(StoreName));

        // Orders are keyed by order id, value "productId;quantity".
        builder.Stream<string, string>(OrdersTopic)
            .TransformValues(TransformerBuilder.New<string, string, string, string>()
                .Transformer<ProductLookupTransformer>()
                .Build())
            .To(OutputTopic);
    }

    public void Configure(StreamConfig config)
    {
    }
}

/// <summary>
/// A table's store is a timestamped key-value store: each value comes with the timestamp of its last update.
/// </summary>
public class ProductLookupTransformer : ITransformer<string, string, string, string>
{
    private ITimestampedKeyValueStore<string, string> catalog;

    public void Init(ProcessorContext<string, string> context)
    {
        catalog = (ITimestampedKeyValueStore<string, string>)context.GetStateStore(GlobalTableLookupExample.StoreName);
    }

    public Record<string, string> Process(Record<string, string> record)
    {
        var parts = record.Value.Split(';');
        var product = catalog.Get(parts[0]); // ValueAndTimestamp<string>, or null
        var name = product?.Value ?? $"unknown product {parts[0]}";
        return Record<string, string>.Create(record.Key, $"{parts[1]} x {name}");
    }

    public void Close()
    {
    }
}
