using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using Amazon;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.Runtime;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.State.Enumerator;

namespace Streamiz.Kafka.Net.Aws.RemoteStorage.Internal
{
    /// <summary>
    /// Key-value store kept in a DynamoDB table, one table per store name.
    /// Items: partition key "TaskId" (string, the task id), sort key "Key" (binary, 0x00 followed by the key bytes, because a
    /// key attribute can't be empty; DynamoDB orders binary values by unsigned bytes, so the order of the keys is kept),
    /// attribute "Value" (binary).
    /// </summary>
    internal class DynamoDbStore : IKeyValueStore<Bytes, byte[]>
    {
        internal const string PartitionKeyAttribute = "TaskId";
        internal const string SortKeyAttribute = "Key";
        internal const string ValueAttribute = "Value";

        // BatchWriteItem accepts at most 25 write requests
        internal const int MaxBatchWriteSize = 25;
        private const int MaxUnprocessedRetries = 10;

        private static readonly BytesComparer bytesComparer = new BytesComparer();

        private readonly AwsRemoteStorageOptions options;
        private string tableName;
        private string partitionKey;

        public string Name { get; }
        public bool Persistent => true;
        public bool IsLocally => false; // avoid changelog topic
        public bool IsOpen { get; private set; }

        protected IAmazonDynamoDB Client { get; private set; }
        internal string TableName => tableName;

        // how often the status of a table being created is checked
        internal TimeSpan TableStatusPollInterval { get; set; } = TimeSpan.FromMilliseconds(500);

        public DynamoDbStore(AwsRemoteStorageOptions options, string name)
        {
            this.options = options ?? new AwsRemoteStorageOptions();
            Name = name;
        }

        #region IKeyValueStore impl

        public void Init(ProcessorContext context, IStateStore root)
        {
            partitionKey = context.Id.ToString();
            var configOptions = context.Configuration?.Read<AwsRemoteStorageOptions>();
            if (configOptions != null)
            {
                options.TableNamePrefix ??= configOptions.TableNamePrefix;
                options.Region ??= configOptions.Region;
                options.ServiceUrl ??= configOptions.ServiceUrl;
                options.AccessKey ??= configOptions.AccessKey;
                options.SecretKey ??= configOptions.SecretKey;
                options.SessionToken ??= configOptions.SessionToken;
                options.CreateTableIfNotExists ??= configOptions.CreateTableIfNotExists;
            }

            if (options.AccessKey != null && options.SecretKey == null)
                throw new StreamConfigException("AWS remote store needs a SecretKey when an AccessKey is configured.");

            tableName = ToTableName(options.TableNamePrefix, Name);
            Client = CreateClient();

            if (options.CreateTableIfNotExists ?? true)
                CreateTableIfNotExists();

            IsOpen = true;

            if (root != null)
                context.Register(root, (key, value, _) => Put(key, value));
        }

        protected virtual IAmazonDynamoDB CreateClient()
        {
            var config = new AmazonDynamoDBConfig();
            if (options.Region != null)
                config.RegionEndpoint = RegionEndpoint.GetBySystemName(options.Region);
            if (options.ServiceUrl != null)
            {
                config.ServiceURL = options.ServiceUrl;
                if (options.Region != null)
                    config.AuthenticationRegion = options.Region;
            }

            if (options.AccessKey == null)
                return new AmazonDynamoDBClient(config); // default credentials chain

            AWSCredentials credentials = options.SessionToken != null
                ? new SessionAWSCredentials(options.AccessKey, options.SecretKey, options.SessionToken)
                : new BasicAWSCredentials(options.AccessKey, options.SecretKey);
            return new AmazonDynamoDBClient(credentials, config);
        }

        public void Flush()
        {
            /* nothing : each write is sent to DynamoDB */
        }

        public void Close()
        {
            IsOpen = false;
            Client?.Dispose();
        }

        public byte[] Get(Bytes key)
        {
            var response = Wait(Client.GetItemAsync(new GetItemRequest
            {
                TableName = tableName,
                Key = ItemKey(key),
                ConsistentRead = true
            }));

            return response.Item != null && response.Item.TryGetValue(ValueAttribute, out var value)
                ? value.B?.ToArray()
                : null;
        }

        public IKeyValueEnumerator<Bytes, byte[]> Range(Bytes from, Bytes to)
            => new WrapEnumerableKeyValueEnumerator<Bytes, byte[]>(Query(from, to, true));

        public IKeyValueEnumerator<Bytes, byte[]> ReverseRange(Bytes from, Bytes to)
            => new WrapEnumerableKeyValueEnumerator<Bytes, byte[]>(Query(from, to, false));

        public IEnumerable<KeyValuePair<Bytes, byte[]>> All()
            => Query(null, null, true);

        public IEnumerable<KeyValuePair<Bytes, byte[]>> ReverseAll()
            => Query(null, null, false);

        // exact count of the task's items, with a COUNT query (it reads the task's items, without returning them)
        public long ApproximateNumEntries()
        {
            long count = 0;
            Dictionary<string, AttributeValue> exclusiveStartKey = null;
            do
            {
                var request = QueryRequest(null, null, true);
                request.Select = Select.COUNT;
                request.ExclusiveStartKey = exclusiveStartKey;
                var response = Wait(Client.QueryAsync(request));
                count += response.Count ?? 0;
                exclusiveStartKey = NextStartKey(response);
            } while (exclusiveStartKey != null);

            return count;
        }

        public void Put(Bytes key, byte[] value)
        {
            // a null value is a delete, as for the other key-value stores
            if (value == null)
            {
                Delete(key);
                return;
            }

            Wait(Client.PutItemAsync(new PutItemRequest
            {
                TableName = tableName,
                Item = Item(key, value)
            }));
        }

        public byte[] PutIfAbsent(Bytes key, byte[] value)
        {
            var originalValue = Get(key);
            if (originalValue == null)
                Put(key, value);
            return originalValue;
        }

        public void PutAll(IEnumerable<KeyValuePair<Bytes, byte[]>> entries)
        {
            // a batch can't hold the same item twice: the last value of each key wins
            var lastValues = new Dictionary<Bytes, byte[]>(bytesComparer);
            var keys = new List<Bytes>();
            foreach (var entry in entries)
            {
                if (!lastValues.ContainsKey(entry.Key))
                    keys.Add(entry.Key);
                lastValues[entry.Key] = entry.Value;
            }

            var writeRequests = keys
                .Select(k => lastValues[k] == null
                    ? new WriteRequest {DeleteRequest = new DeleteRequest {Key = ItemKey(k)}}
                    : new WriteRequest {PutRequest = new PutRequest {Item = Item(k, lastValues[k])}})
                .ToList();

            for (int i = 0; i < writeRequests.Count; i += MaxBatchWriteSize)
                BatchWrite(writeRequests.Skip(i).Take(MaxBatchWriteSize).ToList());
        }

        public byte[] Delete(Bytes key)
        {
            var response = Wait(Client.DeleteItemAsync(new DeleteItemRequest
            {
                TableName = tableName,
                Key = ItemKey(key),
                ReturnValues = ReturnValue.ALL_OLD
            }));

            return response.Attributes != null && response.Attributes.TryGetValue(ValueAttribute, out var value)
                ? value.B?.ToArray()
                : null;
        }

        #endregion

        #region Private

        // DynamoDB table names: 3 to 255 characters among letters, digits, '_', '-' and '.'
        internal static string ToTableName(string prefix, string storeName)
        {
            var name = new string(((prefix ?? string.Empty) + (storeName ?? string.Empty))
                .Select(c => char.IsLetterOrDigit(c) && c < 128 || c == '_' || c == '-' || c == '.' ? c : '_')
                .ToArray());
            if (name.Length < 3)
                name = name.PadRight(3, '_');
            return name.Length > 255 ? name.Substring(0, 255) : name;
        }

        internal static byte[] ToSortKey(Bytes key)
        {
            var sortKey = new byte[key.Get.Length + 1];
            Array.Copy(key.Get, 0, sortKey, 1, key.Get.Length);
            return sortKey;
        }

        internal static Bytes FromSortKey(byte[] sortKey)
        {
            var key = new byte[sortKey.Length - 1];
            Array.Copy(sortKey, 1, key, 0, key.Length);
            return Bytes.Wrap(key);
        }

        private static AttributeValue Binary(byte[] bytes)
            => new AttributeValue {B = new MemoryStream(bytes)};

        private Dictionary<string, AttributeValue> ItemKey(Bytes key)
            => new Dictionary<string, AttributeValue>
            {
                {PartitionKeyAttribute, new AttributeValue {S = partitionKey}},
                {SortKeyAttribute, Binary(ToSortKey(key))}
            };

        private Dictionary<string, AttributeValue> Item(Bytes key, byte[] value)
        {
            var item = ItemKey(key);
            item.Add(ValueAttribute, Binary(value));
            return item;
        }

        private QueryRequest QueryRequest(Bytes from, Bytes to, bool forward)
        {
            var values = new Dictionary<string, AttributeValue>
            {
                {":task", new AttributeValue {S = partitionKey}}
            };
            var condition = "#task = :task";
            if (from != null && to != null)
            {
                condition += " AND #key BETWEEN :from AND :to";
                values.Add(":from", Binary(ToSortKey(from)));
                values.Add(":to", Binary(ToSortKey(to)));
            }
            else if (from != null)
            {
                condition += " AND #key >= :from";
                values.Add(":from", Binary(ToSortKey(from)));
            }
            else if (to != null)
            {
                condition += " AND #key <= :to";
                values.Add(":to", Binary(ToSortKey(to)));
            }

            var names = new Dictionary<string, string> {{"#task", PartitionKeyAttribute}};
            if (from != null || to != null)
                names.Add("#key", SortKeyAttribute);

            return new QueryRequest
            {
                TableName = tableName,
                KeyConditionExpression = condition,
                ExpressionAttributeNames = names,
                ExpressionAttributeValues = values,
                ScanIndexForward = forward,
                ConsistentRead = true
            };
        }

        // the task's items between from and to (both included, null = unbounded), ordered by key
        private IEnumerable<KeyValuePair<Bytes, byte[]>> Query(Bytes from, Bytes to, bool forward)
        {
            // BETWEEN fails when from > to: there is no item in that range
            if (from != null && to != null && bytesComparer.Compare(from, to) > 0)
                return new List<KeyValuePair<Bytes, byte[]>>();

            var results = new List<KeyValuePair<Bytes, byte[]>>();
            Dictionary<string, AttributeValue> exclusiveStartKey = null;
            do
            {
                var request = QueryRequest(from, to, forward);
                request.ExclusiveStartKey = exclusiveStartKey;
                var response = Wait(Client.QueryAsync(request));
                if (response.Items != null)
                    results.AddRange(response.Items.Select(item => new KeyValuePair<Bytes, byte[]>(
                        FromSortKey(item[SortKeyAttribute].B.ToArray()),
                        item.TryGetValue(ValueAttribute, out var value) ? value.B?.ToArray() : null)));
                exclusiveStartKey = NextStartKey(response);
            } while (exclusiveStartKey != null);

            return results;
        }

        private static Dictionary<string, AttributeValue> NextStartKey(QueryResponse response)
            => response.LastEvaluatedKey is {Count: > 0} ? response.LastEvaluatedKey : null;

        // DynamoDB may not write all the items of a batch (throttling): the unprocessed ones are sent again, with a backoff
        private void BatchWrite(List<WriteRequest> writeRequests)
        {
            var requestItems = new Dictionary<string, List<WriteRequest>> {{tableName, writeRequests}};
            for (int attempt = 0; ; ++attempt)
            {
                var response = Wait(Client.BatchWriteItemAsync(new BatchWriteItemRequest {RequestItems = requestItems}));
                if (response.UnprocessedItems == null ||
                    !response.UnprocessedItems.TryGetValue(tableName, out var unprocessed) ||
                    unprocessed == null || unprocessed.Count == 0)
                    return;

                if (attempt >= MaxUnprocessedRetries)
                    throw new StreamsException(
                        $"DynamoDB didn't write {unprocessed.Count} items of table {tableName} after {MaxUnprocessedRetries} retries");

                Thread.Sleep(Math.Min(50 * (1 << attempt), 2000));
                requestItems = new Dictionary<string, List<WriteRequest>> {{tableName, unprocessed}};
            }
        }

        private void CreateTableIfNotExists()
        {
            if (TableStatusOrNull() == null)
            {
                try
                {
                    Wait(Client.CreateTableAsync(new CreateTableRequest
                    {
                        TableName = tableName,
                        BillingMode = BillingMode.PAY_PER_REQUEST,
                        AttributeDefinitions = new List<AttributeDefinition>
                        {
                            new AttributeDefinition {AttributeName = PartitionKeyAttribute, AttributeType = ScalarAttributeType.S},
                            new AttributeDefinition {AttributeName = SortKeyAttribute, AttributeType = ScalarAttributeType.B}
                        },
                        KeySchema = new List<KeySchemaElement>
                        {
                            new KeySchemaElement {AttributeName = PartitionKeyAttribute, KeyType = KeyType.HASH},
                            new KeySchemaElement {AttributeName = SortKeyAttribute, KeyType = KeyType.RANGE}
                        }
                    }));
                }
                catch (ResourceInUseException)
                {
                    // another task (or instance) is creating the same table
                }
            }

            // the table can be used once it is active
            var deadline = DateTime.Now.AddMinutes(2);
            while (!TableStatus.ACTIVE.Equals(TableStatusOrNull()))
            {
                if (DateTime.Now > deadline)
                    throw new StreamsException($"The DynamoDB table {tableName} is not active after 2 minutes");
                Thread.Sleep(TableStatusPollInterval);
            }
        }

        private TableStatus TableStatusOrNull()
        {
            try
            {
                return Wait(Client.DescribeTableAsync(new DescribeTableRequest {TableName = tableName})).Table?.TableStatus;
            }
            catch (ResourceNotFoundException)
            {
                return null;
            }
        }

        // the AWS SDK only has async methods on .NET Core : the store is synchronous, like the other state stores
        private static T Wait<T>(System.Threading.Tasks.Task<T> task)
            => task.GetAwaiter().GetResult();

        #endregion
    }
}
