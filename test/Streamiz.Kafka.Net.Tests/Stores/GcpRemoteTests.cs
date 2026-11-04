using System.Collections.Generic;
using System.Linq;
using System.Text;
using Moq;
using NUnit.Framework;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Gcp.RemoteStorage;
using Streamiz.Kafka.Net.Gcp.RemoteStorage.Internal;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.State.Enumerator;

namespace Streamiz.Kafka.Net.Tests.Stores;

public class GcpRemoteTests
{
    // a Bigtable table in memory: rows sorted by row key as unsigned bytes, like Bigtable
    private class FakeBigtable : IBigtableOperations
    {
        private class UnsignedBytesComparer : IComparer<byte[]>
        {
            public int Compare(byte[] x, byte[] y) => BytesComparer.Compare(x, y);
        }

        public readonly SortedDictionary<byte[], byte[]> Rows = new(new UnsignedBytesComparer());
        public readonly List<int> WriteAllBatchSizes = new();
        public int EnsureTableCalls;
        public int DisposeCalls;

        public void EnsureTable() => ++EnsureTableCalls;

        public byte[] Read(byte[] rowKey) => Rows.TryGetValue(rowKey, out var value) ? value : null;

        public void Write(byte[] rowKey, byte[] value)
        {
            if (value == null)
                Rows.Remove(rowKey);
            else
                Rows[rowKey] = value;
        }

        public void WriteAll(IReadOnlyList<KeyValuePair<byte[], byte[]>> rows)
        {
            WriteAllBatchSizes.Add(rows.Count);
            foreach (var row in rows)
                Write(row.Key, row.Value);
        }

        public IEnumerable<KeyValuePair<byte[], byte[]>> Scan(byte[] startClosed, byte[] endOpen, bool reversed)
        {
            var rows = Rows
                .Where(r => BytesComparer.Compare(r.Key, startClosed) >= 0 &&
                            (endOpen == null || BytesComparer.Compare(r.Key, endOpen) < 0))
                .ToList();
            if (reversed)
                rows.Reverse();
            return rows;
        }

        public void Dispose() => ++DisposeCalls;
    }

    private class MockBigtableStore : BigtableStore
    {
        private readonly FakeBigtable table;

        public MockBigtableStore(GcpRemoteStorageOptions options, string name, FakeBigtable table)
            : base(options, name)
        {
            this.table = table;
        }

        protected override IBigtableOperations CreateOperations() => table;
    }

    private FakeBigtable table;
    private StreamConfig config;

    private static Bytes Key(string key) => Bytes.Wrap(Encoding.UTF8.GetBytes(key));
    private static Bytes Key(params byte[] key) => Bytes.Wrap(key);
    private static byte[] Value(string value) => Encoding.UTF8.GetBytes(value);
    private static string String(byte[] value) => value == null ? null : Encoding.UTF8.GetString(value);

    private MockBigtableStore CreateStore(string name = "store", int partition = 0, GcpRemoteStorageOptions options = null,
        StreamConfig streamConfig = null)
    {
        var context = new Mock<ProcessorContext>();
        context.Setup(c => c.Id).Returns(new TaskId {Id = 0, Partition = partition});
        context.Setup(c => c.Configuration).Returns(streamConfig ?? config);
        var store = new MockBigtableStore(options, name, table);
        store.Init(context.Object, null);
        return store;
    }

    [SetUp]
    public void Init()
    {
        table = new FakeBigtable();
        config = new StreamConfig {ApplicationId = "app"};
        config.AddConfig(GcpRemoteStorageOptions.projectIdCst, "project");
        config.AddConfig(GcpRemoteStorageOptions.instanceIdCst, "instance");
    }

    [Test]
    public void InitWithoutProjectOrInstanceFails()
    {
        var emptyConfig = new StreamConfig {ApplicationId = "app"};
        Assert.Throws<StreamConfigException>(() => CreateStore(streamConfig: emptyConfig));

        emptyConfig.AddConfig(GcpRemoteStorageOptions.projectIdCst, "project");
        Assert.Throws<StreamConfigException>(() => CreateStore(streamConfig: emptyConfig));
    }

    [Test]
    public void InitReadsTheConfigurationAndAppliesDefaults()
    {
        config.AddConfig(GcpRemoteStorageOptions.tableIdCst, "configured-table");
        var store = CreateStore(options: new GcpRemoteStorageOptions {InstanceId = "explicit-instance"});

        Assert.IsTrue(store.IsOpen);
        Assert.IsFalse(store.IsLocally);
        Assert.IsTrue(store.Persistent);
        Assert.AreEqual("project", store.Options.ProjectId);
        Assert.AreEqual("explicit-instance", store.Options.InstanceId); // an explicit option wins
        Assert.AreEqual("configured-table", store.Options.TableId);
        Assert.AreEqual(GcpRemoteStorageOptions.DEFAULT_COLUMN_FAMILY, store.Options.ColumnFamily);
        Assert.AreEqual(GcpRemoteStorageOptions.DEFAULT_BATCH_SIZE, store.Options.BatchSize);
        Assert.AreEqual(true, store.Options.CreateTableIfNotExists);
        Assert.AreEqual(1, table.EnsureTableCalls);
    }

    [Test]
    public void DefaultTableIsShared()
    {
        var store = CreateStore();
        Assert.AreEqual(GcpRemoteStorageOptions.DEFAULT_TABLE_ID, store.Options.TableId);
    }

    [Test]
    public void TableIsNotCreatedWhenDisabled()
    {
        CreateStore(options: new GcpRemoteStorageOptions {CreateTableIfNotExists = false});
        Assert.AreEqual(0, table.EnsureTableCalls);
    }

    [Test]
    public void InvalidBatchSizeFails()
    {
        Assert.Throws<StreamConfigException>(() => CreateStore(options: new GcpRemoteStorageOptions {BatchSize = 0}));
    }

    [Test]
    public void PutGetAndUpdate()
    {
        var store = CreateStore();
        store.Put(Key("k1"), Value("v1"));
        Assert.AreEqual("v1", String(store.Get(Key("k1"))));

        store.Put(Key("k1"), Value("v2"));
        Assert.AreEqual("v2", String(store.Get(Key("k1"))));
        Assert.AreEqual(1, table.Rows.Count);
        Assert.IsNull(store.Get(Key("missing")));
    }

    [Test]
    public void PutNullDeletes()
    {
        var store = CreateStore();
        store.Put(Key("k1"), Value("v1"));
        store.Put(Key("k1"), null);

        Assert.IsNull(store.Get(Key("k1")));
        Assert.AreEqual(0, table.Rows.Count);
    }

    [Test]
    public void DeleteReturnsTheOldValue()
    {
        var store = CreateStore();
        store.Put(Key("k1"), Value("v1"));

        Assert.AreEqual("v1", String(store.Delete(Key("k1"))));
        Assert.IsNull(store.Get(Key("k1")));
        Assert.IsNull(store.Delete(Key("k1")));
    }

    [Test]
    public void PutIfAbsent()
    {
        var store = CreateStore();
        Assert.IsNull(store.PutIfAbsent(Key("k1"), Value("v1")));
        Assert.AreEqual("v1", String(store.PutIfAbsent(Key("k1"), Value("v2"))));
        Assert.AreEqual("v1", String(store.Get(Key("k1"))));
    }

    [Test]
    public void RangeIsSortedByUnsignedKeyBytesWithInclusiveBounds()
    {
        var store = CreateStore();
        // 0x80 sorts after 0x7F as an unsigned byte
        foreach (var key in new[] {Key(0x80), Key(0x01), Key(0x7F), Key(0x01, 0x00), Key(0x02), Key(0xFF)})
            store.Put(key, new byte[] {key.Get[0]});

        var forward = store.Range(Key(0x01, 0x00), Key(0x80)).ToEnumerable().Select(kv => kv.Key.Get).ToList();
        CollectionAssert.AreEqual(new[] {new byte[] {0x01, 0x00}, new byte[] {0x02}, new byte[] {0x7F}, new byte[] {0x80}},
            forward);

        var backward = store.ReverseRange(Key(0x01, 0x00), Key(0x80)).ToEnumerable().Select(kv => kv.Key.Get).ToList();
        CollectionAssert.AreEqual(new[] {new byte[] {0x80}, new byte[] {0x7F}, new byte[] {0x02}, new byte[] {0x01, 0x00}},
            backward);
    }

    [Test]
    public void RangeWithoutBoundsAndInvalidRange()
    {
        var store = CreateStore();
        foreach (var k in new[] {"a", "b", "c"})
            store.Put(Key(k), Value(k));

        CollectionAssert.AreEqual(new[] {"a", "b"},
            store.Range(null, Key("b")).ToEnumerable().Select(kv => String(kv.Value)).ToList());
        CollectionAssert.AreEqual(new[] {"c", "b"},
            store.ReverseRange(Key("b"), null).ToEnumerable().Select(kv => String(kv.Value)).ToList());
        Assert.IsEmpty(store.Range(Key("c"), Key("a")).ToEnumerable().ToList());
    }

    [Test]
    public void AllAndReverseAll()
    {
        var store = CreateStore();
        foreach (var k in new[] {"b", "c", "a"})
            store.Put(Key(k), Value(k));

        CollectionAssert.AreEqual(new[] {"a", "b", "c"}, store.All().Select(kv => String(kv.Value)).ToList());
        CollectionAssert.AreEqual(new[] {"c", "b", "a"}, store.ReverseAll().Select(kv => String(kv.Value)).ToList());
        Assert.AreEqual(0, store.ApproximateNumEntries());
    }

    [Test]
    public void PutAllIsWrittenInBatches()
    {
        var store = CreateStore(options: new GcpRemoteStorageOptions {BatchSize = 3});
        store.Put(Key("k0"), Value("old"));

        var entries = Enumerable.Range(1, 7)
            .Select(i => new KeyValuePair<Bytes, byte[]>(Key($"k{i}"), Value($"v{i}")))
            .Append(new KeyValuePair<Bytes, byte[]>(Key("k0"), null)) // a null value deletes
            .ToList();
        store.PutAll(entries);

        CollectionAssert.AreEqual(new[] {3, 3, 2}, table.WriteAllBatchSizes);
        Assert.IsNull(store.Get(Key("k0")));
        Assert.AreEqual(7, store.All().Count());
        Assert.AreEqual("v7", String(store.Get(Key("k7"))));
    }

    [Test]
    public void TasksStoresAndApplicationsAreIsolatedInTheSameTable()
    {
        var task1 = CreateStore("store", 1);
        var task10 = CreateStore("store", 10); // "0-1" is a string prefix of "0-10"
        var otherStore = CreateStore("store2", 1);
        var otherAppConfig = new StreamConfig {ApplicationId = "app2"};
        otherAppConfig.AddConfig(GcpRemoteStorageOptions.projectIdCst, "project");
        otherAppConfig.AddConfig(GcpRemoteStorageOptions.instanceIdCst, "instance");
        var otherApp = CreateStore("store", 1, streamConfig: otherAppConfig);

        task1.Put(Key("k"), Value("task1"));
        task10.Put(Key("k"), Value("task10"));
        otherStore.Put(Key("k"), Value("store2"));
        otherApp.Put(Key("k"), Value("app2"));
        task10.Put(Key("other"), Value("task10-other"));

        Assert.AreEqual(5, table.Rows.Count);
        Assert.AreEqual("task1", String(task1.Get(Key("k"))));
        Assert.AreEqual("task10", String(task10.Get(Key("k"))));
        Assert.AreEqual("store2", String(otherStore.Get(Key("k"))));
        Assert.AreEqual("app2", String(otherApp.Get(Key("k"))));
        CollectionAssert.AreEqual(new[] {"task1"}, task1.All().Select(kv => String(kv.Value)).ToList());
        CollectionAssert.AreEqual(new[] {"task1"},
            task1.Range(null, null).ToEnumerable().Select(kv => String(kv.Value)).ToList());
        Assert.AreEqual(2, task10.All().Count());
    }

    [Test]
    public void FlushAndClose()
    {
        var store = CreateStore();
        store.Put(Key("k"), Value("v"));
        store.Flush();
        Assert.AreEqual("v", String(table.Read(table.Rows.Keys.Single())));

        store.Close();
        Assert.IsFalse(store.IsOpen);
        Assert.AreEqual(1, table.DisposeCalls);
    }

    [Test]
    public void RowKeyLayout()
    {
        var prefix = BigtableRowKeys.Prefix("ab", "s", "0-1");
        CollectionAssert.AreEqual(
            new byte[] {0, 2, (byte) 'a', (byte) 'b', 0, 1, (byte) 's', 0, 3, (byte) '0', (byte) '-', (byte) '1'},
            prefix);

        var rowKey = BigtableRowKeys.RowKey(prefix, new byte[] {9});
        Assert.IsTrue(BigtableRowKeys.HasPrefix(rowKey, prefix));
        CollectionAssert.AreEqual(new byte[] {9}, BigtableRowKeys.Key(rowKey, prefix));

        CollectionAssert.AreEqual(new byte[] {1, 3}, BigtableRowKeys.PrefixEnd(new byte[] {1, 2, 0xFF}));
        Assert.IsNull(BigtableRowKeys.PrefixEnd(new byte[] {0xFF, 0xFF}));
        CollectionAssert.AreEqual(new byte[] {1, 2, 0}, BigtableRowKeys.Successor(new byte[] {1, 2}));
    }

    [Test]
    public void SupplierAndMaterialized()
    {
        var supplier = new GcpRemoteStorageSupplier(null, "my-store");
        Assert.AreEqual("gcp-remote-storage", supplier.MetricsScope);
        var store = supplier.Get();
        Assert.IsInstanceOf<BigtableStore>(store);
        Assert.AreEqual("my-store", store.Name);

        var materialized = GcpRemoteStorage.As<string, string>("my-store");
        Assert.AreEqual("my-store", materialized.StoreName);
        Assert.IsFalse(materialized.LoggingEnabled);
        Assert.IsInstanceOf<GcpRemoteStorageSupplier>(materialized.StoreSupplier);
    }
}

internal static class GcpRemoteTestsEnumeratorExtensions
{
    public static IEnumerable<KeyValuePair<K, V>> ToEnumerable<K, V>(this IKeyValueEnumerator<K, V> enumerator)
    {
        using (enumerator)
        {
            while (enumerator.MoveNext())
                yield return enumerator.Current.Value;
        }
    }
}
