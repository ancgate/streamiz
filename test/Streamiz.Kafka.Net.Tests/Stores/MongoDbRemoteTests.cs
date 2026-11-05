using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Moq;
using NUnit.Framework;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.MongoDb.RemoteStorage;
using Streamiz.Kafka.Net.MongoDb.RemoteStorage.Internal;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.State;

namespace Streamiz.Kafka.Net.Tests.Stores;

public class MongoDbRemoteTests
{
    // a MongoDB collection in memory: documents (task, hex key) sorted like the { t: 1, k: 1 } index with the
    // simple collation (ordinal string comparison)
    private class FakeCollection : IMongoDbOperations
    {
        private class DocumentKeyComparer : IComparer<(string Task, string HexKey)>
        {
            public int Compare((string Task, string HexKey) x, (string Task, string HexKey) y)
            {
                int task = string.CompareOrdinal(x.Task, y.Task);
                return task != 0 ? task : string.CompareOrdinal(x.HexKey, y.HexKey);
            }
        }

        public readonly SortedDictionary<(string Task, string HexKey), byte[]> Documents = new(new DocumentKeyComparer());
        public readonly List<int> BulkWriteSizes = new();
        public int EnsureIndexCalls;
        public int DisposeCalls;

        public void EnsureIndex() => ++EnsureIndexCalls;

        public byte[] Get(string task, string hexKey)
            => Documents.TryGetValue((task, hexKey), out var value) ? value : null;

        public void Put(MongoDbWrite write)
        {
            if (write.Value == null)
                Documents.Remove((write.Task, write.HexKey));
            else
                Documents[(write.Task, write.HexKey)] = write.Value;
        }

        public byte[] Delete(string task, string hexKey)
        {
            var old = Get(task, hexKey);
            Documents.Remove((task, hexKey));
            return old;
        }

        public void BulkWrite(IReadOnlyList<MongoDbWrite> writes)
        {
            BulkWriteSizes.Add(writes.Count);
            foreach (var write in writes)
                Put(write);
        }

        public IEnumerable<KeyValuePair<string, byte[]>> Find(string task, string fromHex, string toHex, bool descending)
        {
            var documents = Documents
                .Where(d => d.Key.Task == task &&
                            (fromHex == null || string.CompareOrdinal(d.Key.HexKey, fromHex) >= 0) &&
                            (toHex == null || string.CompareOrdinal(d.Key.HexKey, toHex) <= 0))
                .Select(d => new KeyValuePair<string, byte[]>(d.Key.HexKey, d.Value))
                .ToList();
            if (descending)
                documents.Reverse();
            return documents;
        }

        public long Count(string task) => Documents.Keys.Count(k => k.Task == task);

        public void Dispose() => ++DisposeCalls;
    }

    private class MockMongoDbStore : MongoDbStore
    {
        private readonly FakeCollection collection;

        public MockMongoDbStore(MongoDbRemoteStorageOptions options, string name, FakeCollection collection)
            : base(options, name)
        {
            this.collection = collection;
        }

        public string CreatedCollectionName { get; private set; }

        protected override IMongoDbOperations CreateOperations(string collectionName)
        {
            CreatedCollectionName = collectionName;
            return collection;
        }
    }

    private FakeCollection collection;
    private StreamConfig config;

    private static Bytes Key(string key) => Bytes.Wrap(Encoding.UTF8.GetBytes(key));
    private static Bytes Key(params byte[] key) => Bytes.Wrap(key);
    private static byte[] Value(string value) => Encoding.UTF8.GetBytes(value);
    private static string String(byte[] value) => value == null ? null : Encoding.UTF8.GetString(value);

    private MockMongoDbStore CreateStore(string name = "store", int partition = 0, MongoDbRemoteStorageOptions options = null,
        StreamConfig streamConfig = null)
    {
        var context = new Mock<ProcessorContext>();
        context.Setup(c => c.Id).Returns(new TaskId {Id = 0, Partition = partition});
        context.Setup(c => c.Configuration).Returns(streamConfig ?? config);
        var store = new MockMongoDbStore(options, name, collection);
        store.Init(context.Object, null);
        return store;
    }

    private static List<string> Keys(IEnumerable<KeyValuePair<Bytes, byte[]>> entries)
        => entries.Select(e => String(e.Key.Get)).ToList();

    [SetUp]
    public void Init()
    {
        collection = new FakeCollection();
        config = new StreamConfig {ApplicationId = "app"};
        config.AddConfig(MongoDbRemoteStorageOptions.connectionStringCst, "mongodb://localhost:27017");
    }

    [Test]
    public void InitWithoutConnectionStringFails()
    {
        var emptyConfig = new StreamConfig {ApplicationId = "app"};
        Assert.Throws<StreamConfigException>(() => CreateStore(streamConfig: emptyConfig));
    }

    [Test]
    public void InitWithAnInvalidBatchSizeFails()
    {
        Assert.Throws<StreamConfigException>(() =>
            CreateStore(options: new MongoDbRemoteStorageOptions {BatchSize = 0}));
    }

    [Test]
    public void InitReadsTheConfigurationAndAppliesDefaults()
    {
        config.AddConfig(MongoDbRemoteStorageOptions.databaseNameCst, "configured-db");
        config.AddConfig(MongoDbRemoteStorageOptions.collectionPrefixCst, "configured-");
        var store = CreateStore(options: new MongoDbRemoteStorageOptions {CollectionPrefix = "app-"});

        Assert.IsTrue(store.IsOpen);
        Assert.IsFalse(store.IsLocally);
        Assert.IsTrue(store.Persistent);
        Assert.AreEqual("mongodb://localhost:27017", store.Options.ConnectionString);
        Assert.AreEqual("configured-db", store.Options.DatabaseName);
        Assert.AreEqual("app-", store.Options.CollectionPrefix); // an explicit option wins
        Assert.AreEqual(MongoDbRemoteStorageOptions.DEFAULT_BATCH_SIZE, store.Options.BatchSize);
        Assert.AreEqual(true, store.Options.CreateIndex);
        Assert.AreEqual("app-store", store.CollectionName);
        Assert.AreEqual("app-store", store.CreatedCollectionName);
        Assert.AreEqual(1, collection.EnsureIndexCalls);
    }

    [Test]
    public void DefaultDatabaseAndNoPrefix()
    {
        var store = CreateStore();
        Assert.AreEqual(MongoDbRemoteStorageOptions.DEFAULT_DATABASE_NAME, store.Options.DatabaseName);
        Assert.AreEqual("store", store.CollectionName);
    }

    [Test]
    public void IndexCreationCanBeDisabled()
    {
        config.AddConfig(MongoDbRemoteStorageOptions.createIndexCst, false);
        var store = CreateStore();
        Assert.AreEqual(false, store.Options.CreateIndex);
        Assert.AreEqual(0, collection.EnsureIndexCalls);
    }

    [Test]
    public void CollectionNameIsSanitized()
    {
        Assert.AreEqual("my-store", MongoDbKeys.CollectionName(null, "my-store"));
        Assert.AreEqual("p.my_store_", MongoDbKeys.CollectionName("p.", "my$store\0"));
        Assert.AreEqual("_system.users", MongoDbKeys.CollectionName("system.", "users"));
        Assert.AreEqual("streamiz", MongoDbKeys.CollectionName(null, null));
    }

    [Test]
    public void PutGetUpdate()
    {
        var store = CreateStore();
        store.Put(Key("key1"), Value("value1"));
        store.Put(Key("key2"), Value("value2"));
        store.Put(Key("key1"), Value("value1bis"));

        Assert.AreEqual("value1bis", String(store.Get(Key("key1"))));
        Assert.AreEqual("value2", String(store.Get(Key("key2"))));
        Assert.IsNull(store.Get(Key("missing")));
        Assert.AreEqual(2, store.ApproximateNumEntries());
    }

    [Test]
    public void DocumentsUseTheTaskAndTheHexKey()
    {
        var store = CreateStore(partition: 3);
        store.Put(Key("ab"), Value("v"));

        var document = collection.Documents.Single();
        Assert.AreEqual("0-3", document.Key.Task);
        Assert.AreEqual("6162", document.Key.HexKey);
        Assert.AreEqual("0-3|6162", MongoDbKeys.Id(document.Key.Task, document.Key.HexKey));
    }

    [Test]
    public void PutNullDeletes()
    {
        var store = CreateStore();
        store.Put(Key("key1"), Value("value1"));
        store.Put(Key("key1"), null);

        Assert.IsNull(store.Get(Key("key1")));
        Assert.AreEqual(0, collection.Documents.Count);
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
    public void KeysAreSortedAsUnsignedBytesWithPrefixesFirst()
    {
        var store = CreateStore();
        var keys = new[]
        {
            new byte[] {0xFF}, new byte[] {0x80}, new byte[] {0x7F}, new byte[] {0x00},
            Encoding.UTF8.GetBytes("ab"), Encoding.UTF8.GetBytes("a"), Encoding.UTF8.GetBytes("b"), Array.Empty<byte>()
        };
        foreach (var key in keys)
            store.Put(Key(key), Value("v"));

        var expected = keys.OrderBy(k => k, new UnsignedBytesComparer()).ToList();

        CollectionAssert.AreEqual(expected, store.All().Select(e => e.Key.Get).ToList());
        expected.Reverse();
        CollectionAssert.AreEqual(expected, store.ReverseAll().Select(e => e.Key.Get).ToList());
    }

    [Test]
    public void RangeIsInclusiveInBothDirections()
    {
        var store = CreateStore();
        foreach (var key in new[] {"a", "b", "c", "d", "e"})
            store.Put(Key(key), Value(key));

        CollectionAssert.AreEqual(new[] {"b", "c", "d"}, Keys(store.Range(Key("b"), Key("d")).ToEnumerable()));
        CollectionAssert.AreEqual(new[] {"d", "c", "b"}, Keys(store.ReverseRange(Key("b"), Key("d")).ToEnumerable()));
        CollectionAssert.AreEqual(new[] {"c"}, Keys(store.Range(Key("c"), Key("c")).ToEnumerable()));
        CollectionAssert.AreEqual(new[] {"b", "c"}, Keys(store.Range(Key("ab"), Key("c")).ToEnumerable()));
    }

    [Test]
    public void RangeWithNullBounds()
    {
        var store = CreateStore();
        foreach (var key in new[] {"a", "b", "c"})
            store.Put(Key(key), Value(key));

        CollectionAssert.AreEqual(new[] {"a", "b"}, Keys(store.Range(null, Key("b")).ToEnumerable()));
        CollectionAssert.AreEqual(new[] {"c", "b"}, Keys(store.ReverseRange(Key("b"), null).ToEnumerable()));
        CollectionAssert.AreEqual(new[] {"a", "b", "c"}, Keys(store.Range(null, null).ToEnumerable()));
    }

    [Test]
    public void RangeFromGreaterThanToIsEmpty()
    {
        var store = CreateStore();
        store.Put(Key("a"), Value("a"));
        store.Put(Key("c"), Value("c"));

        Assert.IsEmpty(store.Range(Key("c"), Key("a")).ToEnumerable().ToList());
        Assert.IsEmpty(store.ReverseRange(Key("c"), Key("a")).ToEnumerable().ToList());
    }

    [Test]
    public void PutAllIsChunkedAndKeepsTheLastWrite()
    {
        config.AddConfig(MongoDbRemoteStorageOptions.batchSizeCst, 3);
        var store = CreateStore();
        store.Put(Key("old"), Value("old"));

        store.PutAll(new List<KeyValuePair<Bytes, byte[]>>
        {
            new(Key("k1"), Value("v1")),
            new(Key("k2"), Value("v2")),
            new(Key("k1"), Value("v1bis")), // a later write of a key wins
            new(Key("k3"), Value("v3")),
            new(Key("old"), null),            // a null value deletes
            new(Key("k4"), Value("v4")),
            new(Key("k5"), Value("v5")),
            new(Key("k6"), Value("v6")),
        });

        CollectionAssert.AreEqual(new[] {3, 3, 2}, collection.BulkWriteSizes);
        CollectionAssert.AreEqual(new[] {"k1", "k2", "k3", "k4", "k5", "k6"}, Keys(store.All()));
        Assert.AreEqual("v1bis", String(store.Get(Key("k1"))));
        Assert.IsNull(store.Get(Key("old")));
    }

    [Test]
    public void TasksAreIsolated()
    {
        var task1 = CreateStore(partition: 1);
        var task10 = CreateStore(partition: 10);

        task1.Put(Key("a"), Value("task1"));
        task10.Put(Key("a"), Value("task10"));
        task10.Put(Key("b"), Value("task10"));

        Assert.AreEqual("task1", String(task1.Get(Key("a"))));
        Assert.AreEqual("task10", String(task10.Get(Key("a"))));
        CollectionAssert.AreEqual(new[] {"a"}, Keys(task1.All()));
        CollectionAssert.AreEqual(new[] {"a", "b"}, Keys(task10.All()));
        Assert.AreEqual(1, task1.ApproximateNumEntries());
        Assert.AreEqual(2, task10.ApproximateNumEntries());

        Assert.AreEqual("task1", String(task1.Delete(Key("a"))));
        Assert.AreEqual("task10", String(task10.Get(Key("a"))));
    }

    [Test]
    public void HexKeysSortLikeTheBytes()
    {
        var random = new Random(42);
        var comparer = new UnsignedBytesComparer();
        var keys = Enumerable.Range(0, 300).Select(_ =>
        {
            var key = new byte[random.Next(0, 6)];
            random.NextBytes(key);
            return key;
        }).ToList();

        foreach (var x in keys)
        {
            CollectionAssert.AreEqual(x, MongoDbKeys.FromHex(MongoDbKeys.ToHex(x)));
            foreach (var y in keys.Take(40))
                Assert.AreEqual(Math.Sign(comparer.Compare(x, y)),
                    Math.Sign(string.CompareOrdinal(MongoDbKeys.ToHex(x), MongoDbKeys.ToHex(y))),
                    $"{MongoDbKeys.ToHex(x)} vs {MongoDbKeys.ToHex(y)}");
        }
    }

    [Test]
    public void FlushAndClose()
    {
        var store = CreateStore();
        store.Put(Key("a"), Value("a"));
        store.Flush();
        store.Close();

        Assert.IsFalse(store.IsOpen);
        Assert.AreEqual(1, collection.DisposeCalls);
        store.Close(); // closing twice is fine
        Assert.AreEqual(1, collection.DisposeCalls);
    }

    [Test]
    public void SupplierAndMaterialized()
    {
        var supplier = new MongoDbRemoteStorageSupplier(null, "my-store");
        Assert.AreEqual("mongodb-remote-storage", supplier.MetricsScope);
        var store = supplier.Get();
        Assert.IsInstanceOf<MongoDbStore>(store);
        Assert.AreEqual("my-store", store.Name);

        var materialized = MongoDbRemoteStorage.As<string, string>("my-store");
        Assert.AreEqual("my-store", materialized.StoreName);
        Assert.IsFalse(materialized.LoggingEnabled);
        Assert.IsInstanceOf<MongoDbRemoteStorageSupplier>(materialized.StoreSupplier);
    }

    // Runs against a real MongoDB when STREAMIZ_MONGODB_CONNECTION_STRING is set (e.g. mongodb://localhost:27017)
    [Test]
    public void RealMongoDb()
    {
        var connectionString = Environment.GetEnvironmentVariable("STREAMIZ_MONGODB_CONNECTION_STRING");
        if (string.IsNullOrEmpty(connectionString))
            Assert.Ignore("STREAMIZ_MONGODB_CONNECTION_STRING is not set");

        var databaseName = $"streamiz-test-{Guid.NewGuid():N}";
        var client = new MongoDB.Driver.MongoClient(connectionString);
        // a default collation on the collection: the store must still compare the hex keys as binary strings
        client.GetDatabase(databaseName).CreateCollection("store", new MongoDB.Driver.CreateCollectionOptions
        {
            Collation = new MongoDB.Driver.Collation("en", strength: MongoDB.Driver.CollationStrength.Secondary)
        });

        IKeyValueStore<Bytes, byte[]> Open(int partition)
        {
            var realConfig = new StreamConfig {ApplicationId = "app"};
            realConfig.AddConfig(MongoDbRemoteStorageOptions.connectionStringCst, connectionString);
            realConfig.AddConfig(MongoDbRemoteStorageOptions.databaseNameCst, databaseName);
            realConfig.AddConfig(MongoDbRemoteStorageOptions.batchSizeCst, 2);
            var context = new Mock<ProcessorContext>();
            context.Setup(c => c.Id).Returns(new TaskId {Id = 0, Partition = partition});
            context.Setup(c => c.Configuration).Returns(realConfig);
            var store = new MongoDbStore(null, "store");
            store.Init(context.Object, null);
            return store;
        }

        var task0 = Open(0);
        var task1 = Open(1);
        try
        {
            var keys = new[]
            {
                new byte[] {0xFF}, new byte[] {0x80}, new byte[] {0x7F}, new byte[] {0x00},
                Encoding.UTF8.GetBytes("ab"), Encoding.UTF8.GetBytes("a"), Encoding.UTF8.GetBytes("b")
            };
            foreach (var key in keys)
                task0.Put(Key(key), Value("v"));
            task1.Put(Key("a"), Value("task1"));

            var expected = keys.OrderBy(k => k, new UnsignedBytesComparer()).ToList();
            CollectionAssert.AreEqual(expected, task0.All().Select(e => e.Key.Get).ToList());
            expected.Reverse();
            CollectionAssert.AreEqual(expected, task0.ReverseAll().Select(e => e.Key.Get).ToList());
            CollectionAssert.AreEqual(new[] {"a", "ab", "b"}, Keys(task0.Range(Key("a"), Key("b")).ToEnumerable()));
            CollectionAssert.AreEqual(new[] {"b", "ab"}, Keys(task0.ReverseRange(Key("aa"), Key("b")).ToEnumerable()));

            task0.Put(Key("a"), Value("v2"));
            Assert.AreEqual("v2", String(task0.Get(Key("a"))));
            Assert.AreEqual("task1", String(task1.Get(Key("a"))));
            Assert.AreEqual("v2", String(task0.Delete(Key("a"))));
            Assert.IsNull(task0.Get(Key("a")));
            task0.Put(Key("b"), null);
            Assert.IsNull(task0.Get(Key("b")));

            task1.PutAll(new List<KeyValuePair<Bytes, byte[]>>
            {
                new(Key("x"), Value("1")), new(Key("y"), Value("1")), new(Key("x"), Value("2")),
                new(Key("a"), null), new(Key("z"), Value("1"))
            });
            CollectionAssert.AreEqual(new[] {"x", "y", "z"}, Keys(task1.All()));
            Assert.AreEqual("2", String(task1.Get(Key("x"))));
            Assert.AreEqual(3, task1.ApproximateNumEntries());
            Assert.AreEqual(5, task0.ApproximateNumEntries());
        }
        finally
        {
            task0.Close();
            task1.Close();
            client.DropDatabase(databaseName);
            client.Dispose();
        }
    }

    private class UnsignedBytesComparer : IComparer<byte[]>
    {
        public int Compare(byte[] x, byte[] y) => BytesComparer.Compare(x, y);
    }
}
