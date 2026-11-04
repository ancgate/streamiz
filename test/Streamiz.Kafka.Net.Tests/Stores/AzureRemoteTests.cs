using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading;
using Azure;
using Azure.Data.Tables;
using Moq;
using NUnit.Framework;
using Streamiz.Kafka.Net.Azure.RemoteStorage;
using Streamiz.Kafka.Net.Azure.RemoteStorage.Internal;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.State;

namespace Streamiz.Kafka.Net.Tests.Stores;

public class AzureRemoteTests
{
    private StreamConfig config;
    private Mock<ProcessorContext> context;
    private MockAzureStore store;

    // Mimics the Azure Tables service: one dictionary per partition, transactions limited to 100 operations of a single
    // partition, and query filters which must be translatable to OData (no method calls, e.g. no byte comparison).
    private class MockAzureStore : AzureTableStore
    {
        // shared by the stores of different tasks, like a real table
        public readonly Dictionary<string, Dictionary<string, AzureTableEntity>> data;
        public readonly List<int> transactionSizes = new();

        public MockAzureStore(
            AzureRemoteStorageOptions azureRemoteStorageOptions,
            string name,
            Dictionary<string, Dictionary<string, AzureTableEntity>> data = null)
            : base(azureRemoteStorageOptions, name)
        {
            this.data = data ?? new Dictionary<string, Dictionary<string, AzureTableEntity>>();
        }

        private void Upsert(AzureTableEntity entity)
        {
            entity.Timestamp = DateTimeOffset.Now;
            if (!data.ContainsKey(entity.PartitionKey))
                data.Add(entity.PartitionKey, new Dictionary<string, AzureTableEntity>());

            data[entity.PartitionKey].AddOrUpdate(entity.RowKey, entity);
        }

        protected override TableClient CreateTableClient()
        {
            var tableClient = new Mock<TableClient>();
            tableClient
                .Setup(client =>
                    client.UpsertEntity(It.IsAny<AzureTableEntity>(), TableUpdateMode.Replace, CancellationToken.None))
                .Returns((AzureTableEntity entity, TableUpdateMode mode, CancellationToken token) =>
                {
                    Assert.IsNotNull(entity.Value, "an entity is upserted with a null value");
                    Upsert(entity);
                    return Moq.Mock.Of<Response>();
                });

            // DeleteEntity doesn't fail when the entity doesn't exist
            tableClient
                .Setup(client => client.DeleteEntity(It.IsAny<string>(), It.IsAny<string>(), default, default))
                .Returns((string partitionKey, string rowKey, ETag tag, CancellationToken token) =>
                {
                    if (data.ContainsKey(partitionKey))
                        data[partitionKey].Remove(rowKey);
                    return Moq.Mock.Of<Response>();
                });

            tableClient
                .Setup(client =>
                    client.GetEntityIfExists<AzureTableEntity>(It.IsAny<string>(), It.IsAny<string>(), null, default))
                .Returns((string partitionKey, string rowKey, IEnumerable<string> select, CancellationToken token) =>
                {
                    if (data.ContainsKey(partitionKey) && data[partitionKey].ContainsKey(rowKey))
                        return Response.FromValue(data[partitionKey][rowKey], Moq.Mock.Of<Response>());

                    return Moq.Mock.Of<NullableResponse<AzureTableEntity>>();
                });

            tableClient
                .Setup(client => client.SubmitTransaction(It.IsAny<IEnumerable<TableTransactionAction>>(), default))
                .Returns((IEnumerable<TableTransactionAction> transactionActions, CancellationToken token) =>
                {
                    var actions = transactionActions.ToList();
                    // the service limits of a transaction
                    Assert.LessOrEqual(actions.Count, 100, "a transaction holds at most 100 operations");
                    Assert.AreEqual(1, actions.Select(a => a.Entity.PartitionKey).Distinct().Count(),
                        "a transaction targets a single partition");
                    Assert.AreEqual(actions.Count, actions.Select(a => a.Entity.RowKey).Distinct().Count(),
                        "a transaction can't hold the same entity twice");
                    transactionSizes.Add(actions.Count);

                    foreach (var action in actions)
                    {
                        Assert.AreEqual(TableTransactionActionType.UpsertReplace, action.ActionType);
                        Upsert((AzureTableEntity) action.Entity);
                    }
                    return Moq.Mock.Of<Response<IReadOnlyList<Response>>>();
                });

            tableClient.Setup(client =>
                    client.Query<AzureTableEntity>(It.IsAny<Expression<Func<AzureTableEntity, bool>>>(), null, null,
                        default))
                .Returns((Expression<Func<AzureTableEntity, bool>> express, int? maxParge, IEnumerable<string> select,
                    CancellationToken token) =>
                {
                    AssertTranslatableToOData(express);

                    List<AzureTableEntity> entities = new List<AzureTableEntity>();
                    var expressCompile = express.Compile();

                    foreach(var kv in data)
                        foreach(var innerKv in kv.Value)
                            if(expressCompile.Invoke(innerKv.Value))
                                entities.Add(innerKv.Value);

                    var page = Page<AzureTableEntity>.FromValues(entities, null, Moq.Mock.Of<Response>());
                    return Pageable<AzureTableEntity>.FromPages(new List<Page<AzureTableEntity>> { page });
                });

            tableClient
                .Setup(client => client.CreateIfNotExists(CancellationToken.None))
                .Returns(() => null);

            return tableClient.Object;
        }
    }

    // The Azure SDK translates simple property comparisons to OData, not method calls (like a byte array comparison)
    private class MethodCallFinder : ExpressionVisitor
    {
        public readonly List<string> methodCalls = new();

        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            methodCalls.Add(node.Method.DeclaringType?.Name + "." + node.Method.Name);
            return base.VisitMethodCall(node);
        }
    }

    private static void AssertTranslatableToOData(Expression expression)
    {
        var finder = new MethodCallFinder();
        finder.Visit(expression);
        CollectionAssert.IsEmpty(finder.methodCalls, "the query filter can't be translated to OData");
    }

    private static Bytes Key(string key) => Bytes.Wrap(Encoding.UTF8.GetBytes(key));
    private static byte[] Value(string value) => Encoding.UTF8.GetBytes(value);
    private static string String(byte[] bytes) => Encoding.UTF8.GetString(bytes);

    private Mock<ProcessorContext> CreateContext(int partition)
    {
        var mock = new Mock<ProcessorContext>();
        mock.Setup(c => c.Id)
            .Returns(new TaskId() { Id = 0, Partition = partition });
        mock.Setup(c => c.Configuration)
            .Returns(config);
        return mock;
    }

    [SetUp]
    public void Init()
    {
        config = new StreamConfig();
        config.AddConfig(AzureRemoteStorageOptions.storageUriCst, "uri");
        config.AddConfig(AzureRemoteStorageOptions.storageAccountKeyCst, "key");
        config.AddConfig(AzureRemoteStorageOptions.accountNameCst, "account");

        context = CreateContext(0);

        store = new MockAzureStore(null, "name");
        store.Init(context.Object, null);
    }


    [Test]
    public void InsertOneElement()
    {
        store.Put(Bytes.Wrap(Encoding.UTF8.GetBytes("test")), Encoding.UTF8.GetBytes("value"));
        Assert.AreEqual(1, store.data.Count);
    }

    [Test]
    public void InsertAndUpdateOneElement()
    {
        store.Put(Bytes.Wrap(Encoding.UTF8.GetBytes("test")), Encoding.UTF8.GetBytes("value"));
        store.Put(Bytes.Wrap(Encoding.UTF8.GetBytes("test")), Encoding.UTF8.GetBytes("value2"));
        Assert.AreEqual(1, store.data.Count);
    }

    [Test]
    public void InsertAndDeleteOneElement()
    {
        store.Put(Bytes.Wrap(Encoding.UTF8.GetBytes("test")), Encoding.UTF8.GetBytes("value"));
        store.Delete(Bytes.Wrap(Encoding.UTF8.GetBytes("test")));
        Assert.AreEqual(0, store.data[context.Object.Id.ToString()].Count);
    }

    [Test]
    public void InsertAndGettingOneElement()
    {
        store.Put(Bytes.Wrap(Encoding.UTF8.GetBytes("test")), Encoding.UTF8.GetBytes("value"));
        var item = store.Get(Bytes.Wrap(Encoding.UTF8.GetBytes("test")));
        Assert.IsNotNull(item);
        Assert.AreEqual(Encoding.UTF8.GetBytes("value"), item);
    }

    [Test]
    public void GettingNullElement()
    {
        var item = store.Get(Bytes.Wrap(Encoding.UTF8.GetBytes("test")));
        Assert.IsNull(item);
    }

    [Test]
    public void FlushClose()
    {
        store.Put(Bytes.Wrap(Encoding.UTF8.GetBytes("test")), Encoding.UTF8.GetBytes("value"));
        store.Flush();
        store.Close();
        Assert.AreEqual(1, store.data.Count); // nothing is deleted
    }

    [Test]
    public void GetAllElement()
    {
        store.Put(Bytes.Wrap(Encoding.UTF8.GetBytes("test")), Encoding.UTF8.GetBytes("value"));
        store.Put(Bytes.Wrap(Encoding.UTF8.GetBytes("test1")), Encoding.UTF8.GetBytes("value1"));
        store.Put(Bytes.Wrap(Encoding.UTF8.GetBytes("test2")), Encoding.UTF8.GetBytes("value2"));
        var items = store.All();
        Assert.AreEqual(3, items.Count());
    }

    [Test]
    public void GetReverseAllElement()
    {
        store.Put(Bytes.Wrap(Encoding.UTF8.GetBytes("test")), Encoding.UTF8.GetBytes("value"));
        store.Put(Bytes.Wrap(Encoding.UTF8.GetBytes("test1")), Encoding.UTF8.GetBytes("value1"));
        store.Put(Bytes.Wrap(Encoding.UTF8.GetBytes("test2")), Encoding.UTF8.GetBytes("value2"));
        var items = store.ReverseAll();
        Assert.AreEqual(3, items.Count());
    }

    [Test]
    public void GetRangeElement()
    {
        store.Put(Bytes.Wrap(Encoding.UTF8.GetBytes("test")), Encoding.UTF8.GetBytes("value"));
        store.Put(Bytes.Wrap(Encoding.UTF8.GetBytes("test1")), Encoding.UTF8.GetBytes("value1"));
        store.Put(Bytes.Wrap(Encoding.UTF8.GetBytes("test2")), Encoding.UTF8.GetBytes("value2"));
        var items = store.Range(Bytes.Wrap(Encoding.UTF8.GetBytes("test")), Bytes.Wrap(Encoding.UTF8.GetBytes("test1")))
            .ToList();
        Assert.AreEqual(2, items.Count());
    }

    [Test]
    public void GetReverseRangeElement()
    {
        store.Put(Bytes.Wrap(Encoding.UTF8.GetBytes("test")), Encoding.UTF8.GetBytes("value"));
        store.Put(Bytes.Wrap(Encoding.UTF8.GetBytes("test1")), Encoding.UTF8.GetBytes("value1"));
        store.Put(Bytes.Wrap(Encoding.UTF8.GetBytes("test2")), Encoding.UTF8.GetBytes("value2"));
        var items = store.ReverseRange(Bytes.Wrap(Encoding.UTF8.GetBytes("test")), Bytes.Wrap(Encoding.UTF8.GetBytes("test1")))
            .ToList();
        Assert.AreEqual(2, items.Count());
    }

    [Test]
    public void PutAllElement()
    {
        var elements = new List<KeyValuePair<Bytes, byte[]>>
        {
            KeyValuePair.Create(Bytes.Wrap(Encoding.UTF8.GetBytes("test")), Encoding.UTF8.GetBytes("value")),
            KeyValuePair.Create(Bytes.Wrap(Encoding.UTF8.GetBytes("test1")), Encoding.UTF8.GetBytes("value1")),
            KeyValuePair.Create(Bytes.Wrap(Encoding.UTF8.GetBytes("test2")), Encoding.UTF8.GetBytes("value2"))
        };

        store.PutAll(elements);
        var items = store.All();
        Assert.AreEqual(3, items.Count());
    }

    [Test]
    public void PutNullValueDeletesTheElement()
    {
        store.Put(Key("test"), Value("value"));
        store.Put(Key("test"), null);

        Assert.IsNull(store.Get(Key("test")));
        Assert.AreEqual(0, store.data[context.Object.Id.ToString()].Count);
    }

    [Test]
    public void PutNullValueOfAMissingElementDoesNothing()
    {
        store.Put(Key("test"), null);
        Assert.IsNull(store.Get(Key("test")));
    }

    [Test]
    public void PutAllWithNullValuesDeletesTheElements()
    {
        store.Put(Key("a"), Value("1"));
        store.Put(Key("b"), Value("2"));

        store.PutAll(new List<KeyValuePair<Bytes, byte[]>>
        {
            KeyValuePair.Create(Key("a"), (byte[]) null),
            KeyValuePair.Create(Key("c"), Value("3")),
            KeyValuePair.Create(Key("missing"), (byte[]) null)
        });

        CollectionAssert.AreEqual(new[] {"b", "c"}, store.All().Select(kv => String(kv.Key.Get)).ToList());
    }

    [Test]
    public void PutAllWithTheSameKeyTwiceKeepsTheLastValue()
    {
        store.PutAll(new List<KeyValuePair<Bytes, byte[]>>
        {
            KeyValuePair.Create(Key("a"), Value("1")),
            KeyValuePair.Create(Key("a"), Value("2")),
            KeyValuePair.Create(Key("b"), Value("3")),
            KeyValuePair.Create(Key("b"), (byte[]) null)
        });

        Assert.AreEqual("2", String(store.Get(Key("a"))));
        Assert.IsNull(store.Get(Key("b")));
    }

    [Test]
    public void PutAllIsSplitInTransactionsOf100Entities()
    {
        var elements = Enumerable.Range(0, 250)
            .Select(i => KeyValuePair.Create(Key($"key{i:D3}"), Value($"value{i}")))
            .ToList();

        store.PutAll(elements);

        CollectionAssert.AreEqual(new[] {100, 100, 50}, store.transactionSizes);
        Assert.AreEqual(250, store.All().Count());
    }

    [Test]
    public void DeleteReturnsTheOldValue()
    {
        store.Put(Key("test"), Value("value"));

        Assert.AreEqual("value", String(store.Delete(Key("test"))));
        Assert.IsNull(store.Delete(Key("test")));
        Assert.IsNull(store.Get(Key("test")));
    }

    [Test]
    public void AllIsOrderedByKeyBytes()
    {
        // inserted out of order; 0xFF must come after ASCII letters (unsigned byte order)
        store.Put(Bytes.Wrap(new byte[] {0xFF}), Value("ff"));
        store.Put(Key("b"), Value("b"));
        store.Put(Key("a"), Value("a"));
        store.Put(Key("ab"), Value("ab"));

        CollectionAssert.AreEqual(new[] {"a", "ab", "b", "ff"}, store.All().Select(kv => String(kv.Value)).ToList());
        CollectionAssert.AreEqual(new[] {"ff", "b", "ab", "a"}, store.ReverseAll().Select(kv => String(kv.Value)).ToList());
    }

    [Test]
    public void RangeIsOrderedByKeyAndInclusive()
    {
        foreach (var key in new[] {"d", "a", "c", "e", "b"})
            store.Put(Key(key), Value(key));

        CollectionAssert.AreEqual(new[] {"b", "c", "d"},
            store.Range(Key("b"), Key("d")).ToList().Select(kv => String(kv.Key.Get)).ToList());
        CollectionAssert.AreEqual(new[] {"d", "c", "b"},
            store.ReverseRange(Key("b"), Key("d")).ToList().Select(kv => String(kv.Key.Get)).ToList());
    }

    [Test]
    public void QueriesOnlyReturnTheElementsOfTheTask()
    {
        var otherTaskStore = new MockAzureStore(null, "name", store.data);
        otherTaskStore.Init(CreateContext(1).Object, null);

        store.Put(Key("a"), Value("task0"));
        otherTaskStore.Put(Key("a"), Value("task1"));
        otherTaskStore.Put(Key("b"), Value("task1"));

        CollectionAssert.AreEqual(new[] {"task0"}, store.All().Select(kv => String(kv.Value)).ToList());
        CollectionAssert.AreEqual(new[] {"task0"},
            store.Range(Key("a"), Key("z")).ToList().Select(kv => String(kv.Value)).ToList());
        Assert.AreEqual(2, otherTaskStore.All().Count());
    }

    [TestCase("my-store_name", "mystorename")]
    [TestCase("store.with.dots", "storewithdots")]
    [TestCase("1store", "t1store")]
    [TestCase("ab", "ab0")]
    [TestCase("", "t00")]
    public void TableNameIsValidForAzure(string storeName, string tableName)
    {
        Assert.AreEqual(tableName, AzureTableStore.ToTableName(storeName));
    }

    [Test]
    public void LongTableNameIsTruncated()
    {
        Assert.AreEqual(63, AzureTableStore.ToTableName(new string('a', 100)).Length);
    }
}
