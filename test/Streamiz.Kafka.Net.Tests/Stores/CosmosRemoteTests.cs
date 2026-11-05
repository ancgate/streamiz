using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Azure.Core;
using Moq;
using NUnit.Framework;
using Streamiz.Kafka.Net.Azure.Cosmos.RemoteStorage;
using Streamiz.Kafka.Net.Azure.Cosmos.RemoteStorage.Internal;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.State.Enumerator;

namespace Streamiz.Kafka.Net.Tests.Stores;

public class CosmosRemoteTests
{
    // a Cosmos DB container in memory: items by partition and id, queries sorted by k (ordinal, like Cosmos DB string
    // comparison), read in pages of PageSize, transactional batches of at most 100 operations, ids limited to 1023 bytes
    private class FakeContainer : ICosmosOperations
    {
        public const int PageSize = 3;

        public readonly Dictionary<string, Dictionary<string, CosmosItem>> Partitions = new();
        public readonly List<int> UpsertBatchSizes = new();
        public int EnsureContainerCalls;
        public int DisposeCalls;
        public int PagesRead;

        private Dictionary<string, CosmosItem> Partition(string partition)
        {
            if (!Partitions.TryGetValue(partition, out var items))
                Partitions[partition] = items = new Dictionary<string, CosmosItem>();
            return items;
        }

        private static void CheckId(string id)
        {
            if (string.IsNullOrEmpty(id) || Encoding.UTF8.GetByteCount(id) > 1023 ||
                id.IndexOfAny(new[] {'/', '\\', '#', '?'}) >= 0)
                throw new ArgumentException($"invalid Cosmos DB id {id}");
        }

        private static CosmosItem Copy(CosmosItem item)
            => new() {Id = item.Id, T = item.T, K = item.K, V = item.V};

        public void EnsureContainer() => ++EnsureContainerCalls;

        public CosmosItem Read(string partition, string id)
        {
            CheckId(id);
            return Partition(partition).TryGetValue(id, out var item) ? Copy(item) : null;
        }

        public void Upsert(CosmosItem item)
        {
            CheckId(item.Id);
            Partition(item.T)[item.Id] = Copy(item);
        }

        public void Delete(string partition, string id)
        {
            CheckId(id);
            Partition(partition).Remove(id);
        }

        public CosmosItem ReadAndDelete(string partition, string id)
        {
            var item = Read(partition, id);
            if (item != null)
                Partition(partition).Remove(id);
            return item;
        }

        public void UpsertBatch(string partition, IReadOnlyList<CosmosItem> items)
        {
            if (items.Count == 0 || items.Count > 100)
                throw new InvalidOperationException("a transactional batch has between 1 and 100 operations");
            if (items.Any(i => i.T != partition))
                throw new InvalidOperationException("a transactional batch has only one partition key");
            UpsertBatchSizes.Add(items.Count);
            foreach (var item in items)
                Upsert(item);
        }

        public IEnumerable<CosmosItem> Query(string partition, string fromHex, string toHex, bool descending)
        {
            var items = Partition(partition).Values
                .Where(i => (fromHex == null || string.CompareOrdinal(i.K, fromHex) >= 0) &&
                            (toHex == null || string.CompareOrdinal(i.K, toHex) <= 0))
                .Select(Copy)
                .ToList();
            items.Sort((a, b) => string.CompareOrdinal(a.K, b.K));
            if (descending)
                items.Reverse();

            for (int i = 0; i < items.Count; i += PageSize)
            {
                ++PagesRead;
                foreach (var item in items.Skip(i).Take(PageSize))
                    yield return item;
            }
        }

        public long Count(string partition) => Partition(partition).Count;

        public void Dispose() => ++DisposeCalls;
    }

    private class MockCosmosStore : CosmosStore
    {
        private readonly FakeContainer container;

        public MockCosmosStore(CosmosRemoteStorageOptions options, string name, FakeContainer container)
            : base(options, name)
        {
            this.container = container;
        }

        protected override ICosmosOperations CreateOperations() => container;
    }

    private FakeContainer container;
    private StreamConfig config;

    private static Bytes Key(string key) => Bytes.Wrap(Encoding.UTF8.GetBytes(key));
    private static Bytes Key(params byte[] key) => Bytes.Wrap(key);
    private static byte[] Value(string value) => Encoding.UTF8.GetBytes(value);
    private static string String(byte[] value) => value == null ? null : Encoding.UTF8.GetString(value);

    private MockCosmosStore CreateStore(string name = "store", int partition = 0,
        CosmosRemoteStorageOptions options = null, StreamConfig streamConfig = null)
    {
        var context = new Mock<ProcessorContext>();
        context.Setup(c => c.Id).Returns(new TaskId {Id = 0, Partition = partition});
        context.Setup(c => c.Configuration).Returns(streamConfig ?? config);
        var store = new MockCosmosStore(options, name, container);
        store.Init(context.Object, null);
        return store;
    }

    [SetUp]
    public void Init()
    {
        container = new FakeContainer();
        config = new StreamConfig {ApplicationId = "app"};
        config.AddConfig(CosmosRemoteStorageOptions.connectionStringCst,
            "AccountEndpoint=https://localhost:8081/;AccountKey=a2V5;");
    }

    #region Init and options

    [Test]
    public void InitWithoutAccountFails()
    {
        var emptyConfig = new StreamConfig {ApplicationId = "app"};
        Assert.Throws<StreamConfigException>(() => CreateStore(streamConfig: emptyConfig));

        // an endpoint needs a key or a credential
        Assert.Throws<StreamConfigException>(() => CreateStore(streamConfig: emptyConfig,
            options: new CosmosRemoteStorageOptions {AccountEndpoint = "https://account.documents.azure.com:443/"}));

        Assert.DoesNotThrow(() => CreateStore(streamConfig: emptyConfig,
            options: new CosmosRemoteStorageOptions
            {
                AccountEndpoint = "https://account.documents.azure.com:443/", AccountKey = "a2V5"
            }));
        Assert.DoesNotThrow(() => CreateStore(streamConfig: emptyConfig,
            options: new CosmosRemoteStorageOptions
            {
                AccountEndpoint = "https://account.documents.azure.com:443/",
                Credential = new Mock<TokenCredential>().Object
            }));
    }

    [Test]
    public void InitReadsTheConfigurationAndAppliesDefaults()
    {
        config.AddConfig(CosmosRemoteStorageOptions.databaseNameCst, "configured-database");
        config.AddConfig(CosmosRemoteStorageOptions.containerPrefixCst, "app-");
        config.AddConfig(CosmosRemoteStorageOptions.throughputCst, 400);
        var store = CreateStore(options: new CosmosRemoteStorageOptions {DatabaseName = "explicit-database"});

        Assert.IsTrue(store.IsOpen);
        Assert.IsFalse(store.IsLocally);
        Assert.IsTrue(store.Persistent);
        Assert.AreEqual("explicit-database", store.Options.DatabaseName); // an explicit option wins
        Assert.AreEqual("app-store", store.ContainerName);
        Assert.AreEqual(400, store.Options.Throughput);
        Assert.AreEqual(CosmosRemoteStorageOptions.DEFAULT_BATCH_SIZE, store.Options.BatchSize);
        Assert.AreEqual(true, store.Options.CreateIfNotExists);
        Assert.AreEqual(1, container.EnsureContainerCalls);
    }

    [Test]
    public void DefaultDatabase()
    {
        var store = CreateStore();
        Assert.AreEqual(CosmosRemoteStorageOptions.DEFAULT_DATABASE_NAME, store.Options.DatabaseName);
        Assert.AreEqual("store", store.ContainerName);
    }

    [Test]
    public void ContainerIsNotCreatedWhenDisabled()
    {
        config.AddConfig(CosmosRemoteStorageOptions.createIfNotExistsCst, false);
        CreateStore();
        Assert.AreEqual(0, container.EnsureContainerCalls);
    }

    [Test]
    public void InvalidOptionsFail()
    {
        Assert.Throws<StreamConfigException>(() => CreateStore(options: new CosmosRemoteStorageOptions {BatchSize = 0}));
        Assert.Throws<StreamConfigException>(() => CreateStore(options: new CosmosRemoteStorageOptions {BatchSize = 101}));
        Assert.Throws<StreamConfigException>(() =>
            CreateStore(options: new CosmosRemoteStorageOptions {ConsistencyLevel = "Strongest"}));
        Assert.DoesNotThrow(() => CreateStore(options: new CosmosRemoteStorageOptions {ConsistencyLevel = "session"}));
    }

    [Test]
    public void ContainerNamesAreSanitized()
    {
        Assert.AreEqual("my_store_1_", CosmosKeys.ContainerName(null, "my/store#1?"));
        Assert.AreEqual("prefix-a_b", CosmosKeys.ContainerName("prefix-", "a\\b"));
        Assert.AreEqual(255, CosmosKeys.ContainerName("p", new string('x', 300)).Length);
        Assert.Throws<ArgumentException>(() => CosmosKeys.ContainerName(null, " "));
    }

    [Test]
    public void SupplierAndMaterialized()
    {
        var supplier = new CosmosRemoteStorageSupplier(null, "store");
        Assert.AreEqual("azure-cosmos-remote-storage", supplier.MetricsScope);
        var store = supplier.Get();
        Assert.IsInstanceOf<CosmosStore>(store);
        Assert.AreEqual("store", store.Name);

        var materialized = CosmosRemoteStorage.As<string, string>("store");
        Assert.AreEqual("store", materialized.StoreName);
        Assert.IsFalse(materialized.LoggingEnabled);
        Assert.IsInstanceOf<CosmosRemoteStorageSupplier>(materialized.StoreSupplier);
    }

    #endregion

    #region Key-value operations

    [Test]
    public void PutGetUpdate()
    {
        var store = CreateStore();
        Assert.IsNull(store.Get(Key("missing")));

        store.Put(Key("key1"), Value("value1"));
        Assert.AreEqual("value1", String(store.Get(Key("key1"))));

        store.Put(Key("key1"), Value("value2"));
        Assert.AreEqual("value2", String(store.Get(Key("key1"))));

        var item = container.Partitions["0-0"].Values.Single();
        Assert.AreEqual(CosmosKeys.Hex(Encoding.UTF8.GetBytes("key1")), item.K);
        Assert.AreEqual(item.K, item.Id);
        Assert.AreEqual("0-0", item.T);
        Assert.AreEqual(Convert.ToBase64String(Value("value2")), item.V);
    }

    [Test]
    public void EmptyKeyAndValue()
    {
        var store = CreateStore();
        store.Put(Key(), Array.Empty<byte>());
        CollectionAssert.AreEqual(Array.Empty<byte>(), store.Get(Key()));
        Assert.AreEqual(CosmosKeys.EmptyKeyId, container.Partitions["0-0"].Values.Single().Id);
        CollectionAssert.AreEqual(Array.Empty<byte>(), store.All().Single().Key.Get);
    }

    [Test]
    public void NullPutDeletes()
    {
        var store = CreateStore();
        store.Put(Key("key1"), Value("value1"));
        store.Put(Key("key1"), null);
        Assert.IsNull(store.Get(Key("key1")));
        Assert.AreEqual(0, store.ApproximateNumEntries());

        Assert.DoesNotThrow(() => store.Put(Key("missing"), null));
    }

    [Test]
    public void DeleteReturnsTheOldValue()
    {
        var store = CreateStore();
        store.Put(Key("key1"), Value("value1"));
        Assert.AreEqual("value1", String(store.Delete(Key("key1"))));
        Assert.IsNull(store.Get(Key("key1")));
        Assert.IsNull(store.Delete(Key("key1")));
    }

    [Test]
    public void PutIfAbsent()
    {
        var store = CreateStore();
        Assert.IsNull(store.PutIfAbsent(Key("key1"), Value("value1")));
        Assert.AreEqual("value1", String(store.PutIfAbsent(Key("key1"), Value("value2"))));
        Assert.AreEqual("value1", String(store.Get(Key("key1"))));
    }

    [Test]
    public void ApproximateNumEntriesCountsTheTaskItems()
    {
        var store = CreateStore();
        var other = CreateStore(partition: 1);
        store.Put(Key("a"), Value("1"));
        store.Put(Key("b"), Value("2"));
        other.Put(Key("c"), Value("3"));
        Assert.AreEqual(2, store.ApproximateNumEntries());
        Assert.AreEqual(1, other.ApproximateNumEntries());
    }

    #endregion

    #region Ranges

    private static readonly byte[][] OrderedKeys =
    {
        new byte[] {0x00},
        new byte[] {0x00, 0x00},
        new byte[] {0x01},
        new byte[] {0x7F},
        new byte[] {0x7F, 0xFF},
        new byte[] {0x80},
        new byte[] {0x80, 0x00},
        new byte[] {0xFF}
    };

    private MockCosmosStore StoreWithOrderedKeys()
    {
        var store = CreateStore();
        // inserted in another order: the query sorts them
        foreach (var key in OrderedKeys.Reverse())
            store.Put(Key(key), new[] {key[0]});
        return store;
    }

    private static List<byte[]> Keys(IEnumerable<KeyValuePair<Bytes, byte[]>> entries)
        => entries.Select(e => e.Key.Get).ToList();

    private static List<byte[]> Keys(IKeyValueEnumerator<Bytes, byte[]> enumerator)
    {
        var keys = new List<byte[]>();
        while (enumerator.MoveNext())
            keys.Add(enumerator.Current.Value.Key.Get);
        enumerator.Dispose();
        return keys;
    }

    [Test]
    public void AllIsSortedLikeTheKeyBytes()
    {
        var store = StoreWithOrderedKeys();
        CollectionAssert.AreEqual(OrderedKeys, Keys(store.All()));
        CollectionAssert.AreEqual(OrderedKeys.Reverse(), Keys(store.ReverseAll()));
        Assert.AreEqual(2 * 3, container.PagesRead); // 8 items in pages of 3, twice
    }

    [Test]
    public void RangeBoundsAreIncluded()
    {
        var store = StoreWithOrderedKeys();
        var expected = new[] {new byte[] {0x7F}, new byte[] {0x7F, 0xFF}, new byte[] {0x80}};

        CollectionAssert.AreEqual(expected, Keys(store.Range(Key(0x7F), Key(0x80))));
        CollectionAssert.AreEqual(expected.Reverse(), Keys(store.ReverseRange(Key(0x7F), Key(0x80))));
    }

    [Test]
    public void RangeWithNullBounds()
    {
        var store = StoreWithOrderedKeys();
        CollectionAssert.AreEqual(OrderedKeys.Take(3), Keys(store.Range(null, Key(0x01))));
        CollectionAssert.AreEqual(OrderedKeys.Skip(5), Keys(store.Range(Key(0x80), null)));
        CollectionAssert.AreEqual(OrderedKeys, Keys(store.Range(null, null)));
        CollectionAssert.AreEqual(OrderedKeys.Skip(5).Reverse(), Keys(store.ReverseRange(Key(0x80), null)));
    }

    [Test]
    public void RangeFromGreaterThanToIsEmpty()
    {
        var store = StoreWithOrderedKeys();
        Assert.IsEmpty(Keys(store.Range(Key(0x80), Key(0x7F))));
        Assert.IsEmpty(Keys(store.ReverseRange(Key(0x80), Key(0x7F))));
    }

    [Test]
    public void RangesAreReadLazily()
    {
        var store = StoreWithOrderedKeys();
        var enumerator = store.Range(null, null);
        Assert.IsTrue(enumerator.MoveNext());
        Assert.AreEqual(1, container.PagesRead);
        enumerator.Dispose();
    }

    #endregion

    #region PutAll

    [Test]
    public void PutAllIsSentInTransactionalBatches()
    {
        var store = CreateStore();
        var entries = Enumerable.Range(0, 250)
            .Select(i => new KeyValuePair<Bytes, byte[]>(Key($"key{i:000}"), Value($"value{i}")))
            .ToList();
        store.PutAll(entries);

        CollectionAssert.AreEqual(new[] {100, 100, 50}, container.UpsertBatchSizes);
        Assert.AreEqual(250, store.ApproximateNumEntries());
        Assert.AreEqual("value123", String(store.Get(Key("key123"))));
    }

    [Test]
    public void PutAllWithConfiguredBatchSize()
    {
        config.AddConfig(CosmosRemoteStorageOptions.batchSizeCst, 10);
        var store = CreateStore();
        store.PutAll(Enumerable.Range(0, 25)
            .Select(i => new KeyValuePair<Bytes, byte[]>(Key($"key{i:00}"), Value("v"))));
        CollectionAssert.AreEqual(new[] {10, 10, 5}, container.UpsertBatchSizes);
    }

    [Test]
    public void PutAllLastValueWinsAndNullsDelete()
    {
        var store = CreateStore();
        store.Put(Key("deleted"), Value("old"));

        store.PutAll(new[]
        {
            new KeyValuePair<Bytes, byte[]>(Key("key1"), Value("first")),
            new KeyValuePair<Bytes, byte[]>(Key("deleted"), null),
            new KeyValuePair<Bytes, byte[]>(Key("missing"), null), // deleting a missing item doesn't fail
            new KeyValuePair<Bytes, byte[]>(Key("key1"), Value("last")),
            new KeyValuePair<Bytes, byte[]>(Key("key2"), Value("value2")),
            new KeyValuePair<Bytes, byte[]>(Key("key2"), null)
        });

        Assert.AreEqual("last", String(store.Get(Key("key1"))));
        Assert.IsNull(store.Get(Key("deleted")));
        Assert.IsNull(store.Get(Key("missing")));
        Assert.IsNull(store.Get(Key("key2")));
        CollectionAssert.AreEqual(new[] {1}, container.UpsertBatchSizes); // only key1 is upserted
    }

    #endregion

    [Test]
    public void LongKeysGetAHashedId()
    {
        var store = CreateStore();
        var longKey1 = Enumerable.Repeat((byte) 0xAB, 600).Concat(new byte[] {0x01}).ToArray();
        var longKey2 = Enumerable.Repeat((byte) 0xAB, 600).Concat(new byte[] {0x02}).ToArray();

        store.Put(Key(longKey1), Value("value1"));
        store.Put(Key(longKey2), Value("value2"));

        var ids = container.Partitions["0-0"].Keys.ToList();
        Assert.AreEqual(2, ids.Count);
        Assert.IsTrue(ids.All(id => id.StartsWith(CosmosKeys.HashedIdPrefix) && id.Length == 2 + 64));

        Assert.AreEqual("value1", String(store.Get(Key(longKey1))));
        CollectionAssert.AreEqual(new[] {longKey1, longKey2}, Keys(store.Range(Key(longKey1), Key(longKey2))));

        Assert.AreEqual("value1", String(store.Delete(Key(longKey1))));
        Assert.IsNull(store.Get(Key(longKey1)));
        store.Put(Key(longKey2), null);
        Assert.AreEqual(0, store.ApproximateNumEntries());
    }

    [Test]
    public void TasksAreIsolated()
    {
        var store0 = CreateStore(partition: 0);
        var store1 = CreateStore(partition: 1);

        store0.Put(Key("key"), Value("task0"));
        store1.Put(Key("key"), Value("task1"));
        store1.Put(Key("other"), Value("task1"));

        Assert.AreEqual("task0", String(store0.Get(Key("key"))));
        Assert.AreEqual("task1", String(store1.Get(Key("key"))));
        Assert.AreEqual(1, store0.All().Count());
        Assert.AreEqual(2, store1.All().Count());

        store0.Delete(Key("key"));
        Assert.AreEqual("task1", String(store1.Get(Key("key"))));
    }

    [Test]
    public void CloseDisposesTheClient()
    {
        var store = CreateStore();
        store.Flush();
        store.Close();
        Assert.IsFalse(store.IsOpen);
        Assert.AreEqual(1, container.DisposeCalls);
        Assert.DoesNotThrow(() => store.Close());
    }

    [Test]
    public void HexKeysOrderLikeTheBytes()
    {
        var keys = new[] {new byte[0], new byte[] {0x00}, new byte[] {0x00, 0x01}, new byte[] {0x0F}, new byte[] {0x10},
            new byte[] {0x7F}, new byte[] {0x80}, new byte[] {0xFF}, new byte[] {0xFF, 0x00}};
        for (int i = 0; i + 1 < keys.Length; ++i)
        {
            Assert.Less(BytesComparer.Compare(keys[i], keys[i + 1]), 0);
            Assert.Less(string.CompareOrdinal(CosmosKeys.Hex(keys[i]), CosmosKeys.Hex(keys[i + 1])), 0);
            CollectionAssert.AreEqual(keys[i], CosmosKeys.FromHex(CosmosKeys.Hex(keys[i])));
        }
    }
}
