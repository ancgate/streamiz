using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Moq;
using NUnit.Framework;
using Streamiz.Kafka.Net.Aws.RemoteStorage;
using Streamiz.Kafka.Net.Aws.RemoteStorage.Internal;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.State;

namespace Streamiz.Kafka.Net.Tests.Stores;

public class AwsRemoteTests
{
    // In-memory model of DynamoDB: tables of partitions, items ordered by their binary sort key (unsigned bytes).
    private class DynamoDbModel
    {
        private class UnsignedBytesComparer : IComparer<byte[]>
        {
            public int Compare(byte[] x, byte[] y) => BytesComparer.Compare(x, y);
        }

        public readonly Dictionary<string, Dictionary<string, SortedDictionary<byte[], byte[]>>> tables = new();
        public readonly Dictionary<string, string> tableStatus = new();
        public readonly List<int> batchSizes = new();
        public int queryPageSize = 3;
        // number of write requests left unprocessed by the next BatchWriteItem call
        public int unprocessedOnNextBatch;
        public int createTableCalls;

        private SortedDictionary<byte[], byte[]> Partition(string table, string partition)
        {
            if (!tables.TryGetValue(table, out var partitions))
                throw new ResourceNotFoundException($"Table {table} not found");
            if (!partitions.TryGetValue(partition, out var items))
            {
                items = new SortedDictionary<byte[], byte[]>(new UnsignedBytesComparer());
                partitions.Add(partition, items);
            }
            return items;
        }

        private static string TaskOf(Dictionary<string, AttributeValue> key)
            => key[DynamoDbStore.PartitionKeyAttribute].S;

        private static byte[] SortKeyOf(Dictionary<string, AttributeValue> key)
        {
            var sortKey = key[DynamoDbStore.SortKeyAttribute].B.ToArray();
            Assert.Greater(sortKey.Length, 0, "a key attribute can't be empty");
            return sortKey;
        }

        private static AttributeValue Binary(byte[] bytes) => new() {B = new MemoryStream(bytes)};

        private Dictionary<string, AttributeValue> Item(string task, byte[] sortKey, byte[] value)
            => new()
            {
                {DynamoDbStore.PartitionKeyAttribute, new AttributeValue {S = task}},
                {DynamoDbStore.SortKeyAttribute, Binary(sortKey)},
                {DynamoDbStore.ValueAttribute, Binary(value)}
            };

        public Mock<IAmazonDynamoDB> CreateClient()
        {
            var client = new Mock<IAmazonDynamoDB>();

            client.Setup(c => c.DescribeTableAsync(It.IsAny<DescribeTableRequest>(), It.IsAny<CancellationToken>()))
                .Returns((DescribeTableRequest request, CancellationToken _) =>
                {
                    if (!tableStatus.TryGetValue(request.TableName, out var status))
                        throw new ResourceNotFoundException($"Table {request.TableName} not found");
                    // a created table becomes active after one describe
                    tableStatus[request.TableName] = TableStatus.ACTIVE;
                    return Task.FromResult(new DescribeTableResponse
                        {Table = new TableDescription {TableName = request.TableName, TableStatus = status}});
                });

            client.Setup(c => c.CreateTableAsync(It.IsAny<CreateTableRequest>(), It.IsAny<CancellationToken>()))
                .Returns((CreateTableRequest request, CancellationToken _) =>
                {
                    ++createTableCalls;
                    Assert.AreEqual(BillingMode.PAY_PER_REQUEST, request.BillingMode);
                    Assert.AreEqual(KeyType.HASH, request.KeySchema.Single(k => k.AttributeName == DynamoDbStore.PartitionKeyAttribute).KeyType);
                    Assert.AreEqual(KeyType.RANGE, request.KeySchema.Single(k => k.AttributeName == DynamoDbStore.SortKeyAttribute).KeyType);
                    Assert.AreEqual(ScalarAttributeType.B, request.AttributeDefinitions.Single(a => a.AttributeName == DynamoDbStore.SortKeyAttribute).AttributeType);
                    tables[request.TableName] = new Dictionary<string, SortedDictionary<byte[], byte[]>>();
                    tableStatus[request.TableName] = TableStatus.CREATING;
                    return Task.FromResult(new CreateTableResponse());
                });

            client.Setup(c => c.GetItemAsync(It.IsAny<GetItemRequest>(), It.IsAny<CancellationToken>()))
                .Returns((GetItemRequest request, CancellationToken _) =>
                {
                    var items = Partition(request.TableName, TaskOf(request.Key));
                    var sortKey = SortKeyOf(request.Key);
                    return Task.FromResult(new GetItemResponse
                    {
                        Item = items.TryGetValue(sortKey, out var value) ? Item(TaskOf(request.Key), sortKey, value) : null
                    });
                });

            client.Setup(c => c.PutItemAsync(It.IsAny<PutItemRequest>(), It.IsAny<CancellationToken>()))
                .Returns((PutItemRequest request, CancellationToken _) =>
                {
                    Assert.IsNotNull(request.Item[DynamoDbStore.ValueAttribute].B, "an item is put without value");
                    Partition(request.TableName, TaskOf(request.Item))[SortKeyOf(request.Item)] =
                        request.Item[DynamoDbStore.ValueAttribute].B.ToArray();
                    return Task.FromResult(new PutItemResponse());
                });

            client.Setup(c => c.DeleteItemAsync(It.IsAny<DeleteItemRequest>(), It.IsAny<CancellationToken>()))
                .Returns((DeleteItemRequest request, CancellationToken _) =>
                {
                    var items = Partition(request.TableName, TaskOf(request.Key));
                    var sortKey = SortKeyOf(request.Key);
                    Dictionary<string, AttributeValue> old = null;
                    if (items.TryGetValue(sortKey, out var value))
                    {
                        items.Remove(sortKey);
                        if (ReturnValue.ALL_OLD.Equals(request.ReturnValues))
                            old = Item(TaskOf(request.Key), sortKey, value);
                    }
                    return Task.FromResult(new DeleteItemResponse {Attributes = old});
                });

            client.Setup(c => c.BatchWriteItemAsync(It.IsAny<BatchWriteItemRequest>(), It.IsAny<CancellationToken>()))
                .Returns((BatchWriteItemRequest request, CancellationToken _) =>
                {
                    var unprocessed = new Dictionary<string, List<WriteRequest>>();
                    foreach (var (table, writes) in request.RequestItems)
                    {
                        // the service limits of a batch
                        Assert.LessOrEqual(writes.Count, 25, "a batch holds at most 25 write requests");
                        var keys = writes.Select(w => w.PutRequest?.Item ?? w.DeleteRequest.Key)
                            .Select(k => TaskOf(k) + Convert.ToBase64String(SortKeyOf(k))).ToList();
                        Assert.AreEqual(keys.Count, keys.Distinct().Count(), "a batch can't hold the same item twice");
                        batchSizes.Add(writes.Count);

                        int processedCount = writes.Count - Math.Min(unprocessedOnNextBatch, writes.Count);
                        if (processedCount < writes.Count)
                            unprocessed[table] = writes.Skip(processedCount).ToList();
                        unprocessedOnNextBatch = 0;

                        foreach (var write in writes.Take(processedCount))
                        {
                            if (write.PutRequest != null)
                                Partition(table, TaskOf(write.PutRequest.Item))[SortKeyOf(write.PutRequest.Item)] =
                                    write.PutRequest.Item[DynamoDbStore.ValueAttribute].B.ToArray();
                            else
                                Partition(table, TaskOf(write.DeleteRequest.Key)).Remove(SortKeyOf(write.DeleteRequest.Key));
                        }
                    }
                    return Task.FromResult(new BatchWriteItemResponse {UnprocessedItems = unprocessed});
                });

            // understands the key conditions written by the store: "#task = :task" [AND #key BETWEEN|>=|<= ...]
            client.Setup(c => c.QueryAsync(It.IsAny<QueryRequest>(), It.IsAny<CancellationToken>()))
                .Returns((QueryRequest request, CancellationToken _) =>
                {
                    var condition = request.KeyConditionExpression;
                    var values = request.ExpressionAttributeValues;
                    StringAssert.StartsWith("#task = :task", condition);
                    Assert.AreEqual(DynamoDbStore.PartitionKeyAttribute, request.ExpressionAttributeNames["#task"]);

                    byte[] from = values.TryGetValue(":from", out var f) ? f.B.ToArray() : null;
                    byte[] to = values.TryGetValue(":to", out var t) ? t.B.ToArray() : null;
                    if (condition.Contains("BETWEEN"))
                        Assert.LessOrEqual(BytesComparer.Compare(from, to), 0, "BETWEEN fails when the lower bound is greater");

                    IEnumerable<KeyValuePair<byte[], byte[]>> items = Partition(request.TableName, values[":task"].S)
                        .Where(kv => (from == null || BytesComparer.Compare(kv.Key, from) >= 0) &&
                                     (to == null || BytesComparer.Compare(kv.Key, to) <= 0));
                    if (request.ScanIndexForward == false)
                        items = items.Reverse();

                    if (request.ExclusiveStartKey != null)
                    {
                        var start = SortKeyOf(request.ExclusiveStartKey);
                        items = items.SkipWhile(kv => BytesComparer.Compare(kv.Key, start) != 0).Skip(1);
                    }

                    var all = items.ToList();
                    var page = all.Take(queryPageSize).ToList();
                    var response = new QueryResponse
                    {
                        Count = page.Count,
                        LastEvaluatedKey = all.Count > queryPageSize
                            ? new Dictionary<string, AttributeValue>
                            {
                                {DynamoDbStore.PartitionKeyAttribute, new AttributeValue {S = values[":task"].S}},
                                {DynamoDbStore.SortKeyAttribute, Binary(page.Last().Key)}
                            }
                            : null
                    };
                    if (!Select.COUNT.Equals(request.Select))
                        response.Items = page.Select(kv => Item(values[":task"].S, kv.Key, kv.Value)).ToList();
                    return Task.FromResult(response);
                });

            return client;
        }
    }

    private class MockDynamoDbStore : DynamoDbStore
    {
        private readonly DynamoDbModel model;

        public MockDynamoDbStore(AwsRemoteStorageOptions options, string name, DynamoDbModel model)
            : base(options, name)
        {
            this.model = model;
            TableStatusPollInterval = TimeSpan.FromMilliseconds(1);
        }

        protected override IAmazonDynamoDB CreateClient() => model.CreateClient().Object;
    }

    private StreamConfig config;
    private DynamoDbModel model;
    private MockDynamoDbStore store;

    private static Bytes Key(string key) => Bytes.Wrap(Encoding.UTF8.GetBytes(key));
    private static byte[] Value(string value) => Encoding.UTF8.GetBytes(value);
    private static string String(byte[] bytes) => Encoding.UTF8.GetString(bytes);

    private Mock<ProcessorContext> CreateContext(int partition)
    {
        var context = new Mock<ProcessorContext>();
        context.Setup(c => c.Id).Returns(new TaskId {Id = 0, Partition = partition});
        context.Setup(c => c.Configuration).Returns(config);
        return context;
    }

    private MockDynamoDbStore CreateStore(int partition = 0, AwsRemoteStorageOptions options = null, string name = "store")
    {
        var s = new MockDynamoDbStore(options, name, model);
        s.Init(CreateContext(partition).Object, null);
        return s;
    }

    private Dictionary<string, SortedDictionary<byte[], byte[]>> Table => model.tables[store.TableName];

    [SetUp]
    public void Init()
    {
        config = new StreamConfig();
        config.AddConfig(AwsRemoteStorageOptions.regionCst, "eu-west-1");
        model = new DynamoDbModel();
        store = CreateStore();
    }

    [Test]
    public void InsertOneElement()
    {
        store.Put(Key("test"), Value("value"));
        Assert.AreEqual(1, Table["0-0"].Count);
    }

    [Test]
    public void InsertAndUpdateOneElement()
    {
        store.Put(Key("test"), Value("value"));
        store.Put(Key("test"), Value("value2"));
        Assert.AreEqual(1, Table["0-0"].Count);
        Assert.AreEqual("value2", String(store.Get(Key("test"))));
    }

    [Test]
    public void InsertAndDeleteOneElement()
    {
        store.Put(Key("test"), Value("value"));
        store.Delete(Key("test"));
        Assert.AreEqual(0, Table["0-0"].Count);
    }

    [Test]
    public void GettingNullElement()
    {
        Assert.IsNull(store.Get(Key("test")));
    }

    [Test]
    public void EmptyKeyAndEmptyValue()
    {
        store.Put(Bytes.Wrap(Array.Empty<byte>()), Array.Empty<byte>());
        CollectionAssert.AreEqual(Array.Empty<byte>(), store.Get(Bytes.Wrap(Array.Empty<byte>())));
        Assert.AreEqual(0, store.All().Single().Key.Get.Length);
    }

    [Test]
    public void FlushClose()
    {
        store.Put(Key("test"), Value("value"));
        store.Flush();
        store.Close();
        Assert.AreEqual(1, Table["0-0"].Count); // nothing is deleted
        Assert.IsFalse(store.IsOpen);
    }

    [Test]
    public void PutNullValueDeletesTheElement()
    {
        store.Put(Key("test"), Value("value"));
        store.Put(Key("test"), null);
        Assert.IsNull(store.Get(Key("test")));
        Assert.AreEqual(0, Table["0-0"].Count);
    }

    [Test]
    public void DeleteReturnsTheOldValue()
    {
        store.Put(Key("test"), Value("value"));
        Assert.AreEqual("value", String(store.Delete(Key("test"))));
        Assert.IsNull(store.Delete(Key("test")));
    }

    [Test]
    public void PutIfAbsent()
    {
        Assert.IsNull(store.PutIfAbsent(Key("test"), Value("first")));
        Assert.AreEqual("first", String(store.PutIfAbsent(Key("test"), Value("second"))));
        Assert.AreEqual("first", String(store.Get(Key("test"))));
    }

    [Test]
    public void AllIsOrderedByUnsignedKeyBytesAcrossPages()
    {
        // 0xFF must come after ASCII letters; 7 items with pages of 3 items
        store.Put(Bytes.Wrap(new byte[] {0xFF}), Value("ff"));
        foreach (var key in new[] {"d", "a", "c", "ab", "b", "e"})
            store.Put(Key(key), Value(key));

        CollectionAssert.AreEqual(new[] {"a", "ab", "b", "c", "d", "e", "ff"},
            store.All().Select(kv => String(kv.Value)).ToList());
        CollectionAssert.AreEqual(new[] {"ff", "e", "d", "c", "b", "ab", "a"},
            store.ReverseAll().Select(kv => String(kv.Value)).ToList());
    }

    [Test]
    public void RangeIsInclusiveOrderedAndPaginated()
    {
        foreach (var key in new[] {"g", "a", "e", "c", "b", "f", "d"})
            store.Put(Key(key), Value(key));

        CollectionAssert.AreEqual(new[] {"b", "c", "d", "e", "f"},
            store.Range(Key("b"), Key("f")).ToList().Select(kv => String(kv.Key.Get)).ToList());
        CollectionAssert.AreEqual(new[] {"f", "e", "d", "c", "b"},
            store.ReverseRange(Key("b"), Key("f")).ToList().Select(kv => String(kv.Key.Get)).ToList());
    }

    [Test]
    public void RangeWithFromGreaterThanToIsEmpty()
    {
        store.Put(Key("a"), Value("a"));
        CollectionAssert.IsEmpty(store.Range(Key("z"), Key("a")).ToList());
    }

    [Test]
    public void PutAllIsSplitInBatchesOf25()
    {
        var entries = Enumerable.Range(0, 60)
            .Select(i => KeyValuePair.Create(Key($"key{i:D2}"), Value($"value{i}")))
            .ToList();

        store.PutAll(entries);

        CollectionAssert.AreEqual(new[] {25, 25, 10}, model.batchSizes);
        Assert.AreEqual(60, store.ApproximateNumEntries());
        Assert.AreEqual("value59", String(store.Get(Key("key59"))));
    }

    [Test]
    public void PutAllKeepsTheLastValueAndDeletesNullValues()
    {
        store.Put(Key("a"), Value("old"));
        store.PutAll(new List<KeyValuePair<Bytes, byte[]>>
        {
            KeyValuePair.Create(Key("a"), (byte[]) null),
            KeyValuePair.Create(Key("b"), Value("1")),
            KeyValuePair.Create(Key("b"), Value("2")),
            KeyValuePair.Create(Key("missing"), (byte[]) null)
        });

        Assert.IsNull(store.Get(Key("a")));
        Assert.AreEqual("2", String(store.Get(Key("b"))));
    }

    [Test]
    public void PutAllRetriesTheUnprocessedItems()
    {
        model.unprocessedOnNextBatch = 4;
        var entries = Enumerable.Range(0, 10)
            .Select(i => KeyValuePair.Create(Key($"key{i}"), Value($"value{i}")))
            .ToList();

        store.PutAll(entries);

        CollectionAssert.AreEqual(new[] {10, 4}, model.batchSizes);
        Assert.AreEqual(10, store.All().Count());
    }

    [Test]
    public void EachTaskOnlySeesItsItems()
    {
        var otherTaskStore = CreateStore(1);
        store.Put(Key("a"), Value("task0"));
        otherTaskStore.Put(Key("a"), Value("task1"));
        otherTaskStore.Put(Key("b"), Value("task1"));

        Assert.AreEqual("task0", String(store.Get(Key("a"))));
        CollectionAssert.AreEqual(new[] {"task0"}, store.All().Select(kv => String(kv.Value)).ToList());
        CollectionAssert.AreEqual(new[] {"task0"},
            store.Range(Key("a"), Key("z")).ToList().Select(kv => String(kv.Value)).ToList());
        Assert.AreEqual(1, store.ApproximateNumEntries());
        Assert.AreEqual(2, otherTaskStore.ApproximateNumEntries());
    }

    [Test]
    public void TableIsCreatedOnceAndWaitedUntilActive()
    {
        Assert.AreEqual(1, model.createTableCalls);
        Assert.AreEqual(TableStatus.ACTIVE, model.tableStatus[store.TableName]);

        CreateStore(1); // the table exists already
        Assert.AreEqual(1, model.createTableCalls);
    }

    [Test]
    public void TableIsNotCreatedWhenDisabled()
    {
        var s = new MockDynamoDbStore(new AwsRemoteStorageOptions {CreateTableIfNotExists = false}, "other-store", model);
        s.Init(CreateContext(0).Object, null);

        Assert.AreEqual(1, model.createTableCalls); // only the store of the setup
        Assert.Throws<ResourceNotFoundException>(() => s.Get(Key("a")));
    }

    [Test]
    public void TableNameUsesThePrefixFromTheConfiguration()
    {
        config.AddConfig(AwsRemoteStorageOptions.tableNamePrefixCst, "app1-");
        var s = CreateStore(0, null, "my store");
        Assert.AreEqual("app1-my_store", s.TableName);
    }

    [Test]
    public void OptionsInCodeWinOverTheConfiguration()
    {
        config.AddConfig(AwsRemoteStorageOptions.tableNamePrefixCst, "config-");
        var s = CreateStore(0, new AwsRemoteStorageOptions {TableNamePrefix = "code-"}, "store");
        Assert.AreEqual("code-store", s.TableName);
    }

    [Test]
    public void AccessKeyWithoutSecretKeyIsRejected()
    {
        var s = new MockDynamoDbStore(new AwsRemoteStorageOptions {AccessKey = "key"}, "store", model);
        Assert.Throws<StreamConfigException>(() => s.Init(CreateContext(0).Object, null));
    }

    [TestCase(null, "store", "store")]
    [TestCase("prefix.", "my-store_1", "prefix.my-store_1")]
    [TestCase(null, "a b/c", "a_b_c")]
    [TestCase(null, "ab", "ab_")]
    public void TableNameIsValidForDynamoDb(string prefix, string storeName, string tableName)
    {
        Assert.AreEqual(tableName, DynamoDbStore.ToTableName(prefix, storeName));
    }

    [Test]
    public void MaterializedUsesTheAwsSupplierWithoutLogging()
    {
        var materialized = AwsRemoteStorage.As<string, string>("aws-store");
        Assert.IsInstanceOf<AwsRemoteStorageSupplier>(materialized.StoreSupplier);
        Assert.AreEqual("aws-remote-storage", materialized.StoreSupplier.MetricsScope);
        Assert.IsFalse(materialized.LoggingEnabled);
    }
}
