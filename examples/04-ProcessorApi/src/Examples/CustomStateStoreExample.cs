using Streamiz.Examples.Common;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.State.Enumerator;
using Streamiz.Kafka.Net.State.Supplier;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Examples.ProcessorApi.Examples;

/// <summary>
/// A custom key-value store implementation, plugged into a DSL aggregation through Materialized.
/// </summary>
public class CustomStateStoreExample : IExample
{
    public const string PageViewsTopic = "page-views";
    public const string OutputTopic = "page-view-counts";
    public const string StoreName = "page-view-counts-store";

    public string Name => "custom-state-store";

    public string Description => "Custom state store: count page views in a store backed by your own implementation (a sorted dictionary here).";

    public IEnumerable<string> InputTopics => new[] { PageViewsTopic };

    public void BuildTopology(StreamBuilder builder)
    {
        // Page views keyed by page.
        builder.Stream<string, string>(PageViewsTopic)
            .GroupByKey()
            // Materialized.Create(supplier): the aggregation stores its state in the store returned by the supplier.
            // Streamiz still wraps it with its own layers (serdes, caching, changelog, metrics).
            .Count(Materialized<string, long, IKeyValueStore<Bytes, byte[]>>
                .Create(new SortedDictionaryStoreSupplier(StoreName))
                .WithValueSerdes(new Int64SerDes()))
            .ToStream()
            .MapValues((count, _) => count.ToString())
            .To(OutputTopic);
    }

    public void Configure(StreamConfig config)
    {
    }
}

/// <summary>
/// Creates one store instance per task (Get() is called for each task that uses the store).
/// </summary>
public class SortedDictionaryStoreSupplier : IKeyValueBytesStoreSupplier
{
    public SortedDictionaryStoreSupplier(string name)
    {
        Name = name;
    }

    public string Name { get; set; }

    public string MetricsScope => "sorted-dictionary";

    public IKeyValueStore<Bytes, byte[]> Get() => new SortedDictionaryKeyValueStore(Name);
}

/// <summary>
/// A store keeping the serialized keys and values in a sorted dictionary.
/// Keys are <see cref="Bytes"/> (comparable, so ranges follow the byte order), values are raw bytes.
/// </summary>
public class SortedDictionaryKeyValueStore : IKeyValueStore<Bytes, byte[]>
{
    private readonly SortedDictionary<Bytes, byte[]> data = new();

    public SortedDictionaryKeyValueStore(string name)
    {
        Name = name;
    }

    public string Name { get; }

    // Not persistent: the content lives in memory and is rebuilt from the changelog topic after a restart.
    public bool Persistent => false;

    // Local store: Streamiz backs it with a changelog topic (a remote store would return false).
    public bool IsLocally => true;

    public bool IsOpen { get; private set; }

    public void Init(ProcessorContext context, IStateStore root)
    {
        // Register the root store (Streamiz's outer layer) and the callback used to restore the content
        // from the changelog: each changelog record is put back in the dictionary.
        context.Register(root, (key, value, _) => Put(key, value));
        IsOpen = true;
    }

    public byte[] Get(Bytes key) => data.TryGetValue(key, out var value) ? value : null;

    public void Put(Bytes key, byte[] value)
    {
        if (value == null)
            data.Remove(key); // a null value is a delete
        else
            data[key] = value;
    }

    public byte[] PutIfAbsent(Bytes key, byte[] value)
    {
        var existing = Get(key);
        if (existing == null)
            Put(key, value);
        return existing;
    }

    public void PutAll(IEnumerable<KeyValuePair<Bytes, byte[]>> entries)
    {
        foreach (var entry in entries)
            Put(entry.Key, entry.Value);
    }

    public byte[] Delete(Bytes key)
    {
        var existing = Get(key);
        data.Remove(key);
        return existing;
    }

    public IKeyValueEnumerator<Bytes, byte[]> Range(Bytes from, Bytes to)
        => new ListKeyValueEnumerator(InRange(from, to).ToList());

    public IKeyValueEnumerator<Bytes, byte[]> ReverseRange(Bytes from, Bytes to)
        => new ListKeyValueEnumerator(InRange(from, to).Reverse().ToList());

    public IEnumerable<KeyValuePair<Bytes, byte[]>> All() => data.ToList();

    public IEnumerable<KeyValuePair<Bytes, byte[]>> ReverseAll() => data.Reverse().ToList();

    public long ApproximateNumEntries() => data.Count;

    public void Flush()
    {
        // Nothing to flush: writes are applied immediately.
    }

    public void Close()
    {
        data.Clear();
        IsOpen = false;
    }

    private IEnumerable<KeyValuePair<Bytes, byte[]>> InRange(Bytes from, Bytes to)
        => data.Where(kv => (from == null || kv.Key.CompareTo(from) >= 0) && (to == null || kv.Key.CompareTo(to) <= 0));

    /// <summary>An enumerator over a snapshot of the entries.</summary>
    private class ListKeyValueEnumerator : IKeyValueEnumerator<Bytes, byte[]>
    {
        private readonly List<KeyValuePair<Bytes, byte[]>> entries;
        private int index = -1;

        public ListKeyValueEnumerator(List<KeyValuePair<Bytes, byte[]>> entries)
        {
            this.entries = entries;
        }

        public KeyValuePair<Bytes, byte[]>? Current => index >= 0 && index < entries.Count ? entries[index] : null;

        object System.Collections.IEnumerator.Current => Current;

        public bool MoveNext() => ++index < entries.Count;

        public Bytes PeekNextKey() => index + 1 < entries.Count ? entries[index + 1].Key : null;

        public void Reset() => index = -1;

        public void Dispose()
        {
        }
    }
}
