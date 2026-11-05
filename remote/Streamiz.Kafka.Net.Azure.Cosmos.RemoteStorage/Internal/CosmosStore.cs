using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Logging;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.State.Enumerator;

namespace Streamiz.Kafka.Net.Azure.Cosmos.RemoteStorage.Internal
{
    /// <summary>
    /// Key-value store kept in an Azure Cosmos DB container, one container per store (prefix + store name).
    /// Items are partitioned by task id (partition key path <c>/t</c>) and hold the key in lowercase hexadecimal (<c>k</c>,
    /// which orders like the key bytes) and the value in base64 (<c>v</c>). See <see cref="CosmosKeys"/> for the item ids.
    /// The data is durable in Cosmos DB, so the store has no changelog topic (<see cref="IsLocally"/> is false).
    /// </summary>
    internal class CosmosStore : IKeyValueStore<Bytes, byte[]>
    {
        private static readonly ILogger log = Logger.GetLogger(typeof(CosmosStore));
        private static readonly BytesComparer bytesComparer = new();

        private readonly CosmosRemoteStorageOptions options;
        private ICosmosOperations operations;
        private string partition;

        public CosmosStore(CosmosRemoteStorageOptions options, string name)
        {
            this.options = options ?? new CosmosRemoteStorageOptions();
            Name = name;
        }

        public string Name { get; }
        public bool Persistent => true;
        public bool IsLocally => false; // no changelog topic
        public bool IsOpen { get; private set; }

        internal CosmosRemoteStorageOptions Options => options;
        internal string ContainerName { get; private set; }

        #region IStateStore

        public void Init(ProcessorContext context, IStateStore root)
        {
            var configured = context.Configuration?.Read<CosmosRemoteStorageOptions>();
            if (configured != null)
            {
                options.ConnectionString ??= configured.ConnectionString;
                options.AccountEndpoint ??= configured.AccountEndpoint;
                options.AccountKey ??= configured.AccountKey;
                options.DatabaseName ??= configured.DatabaseName;
                options.ContainerPrefix ??= configured.ContainerPrefix;
                options.CreateIfNotExists ??= configured.CreateIfNotExists;
                options.Throughput ??= configured.Throughput;
                options.ConsistencyLevel ??= configured.ConsistencyLevel;
                options.BatchSize ??= configured.BatchSize;
                options.MaxRetryAttemptsOnRateLimitedRequests ??= configured.MaxRetryAttemptsOnRateLimitedRequests;
            }

            options.DatabaseName ??= CosmosRemoteStorageOptions.DEFAULT_DATABASE_NAME;
            options.CreateIfNotExists ??= true;
            options.BatchSize ??= CosmosRemoteStorageOptions.DEFAULT_BATCH_SIZE;
            Validate(options);

            partition = context.Id.ToString();
            ContainerName = CosmosKeys.ContainerName(options.ContainerPrefix, Name);

            operations = CreateOperations();
            if (options.CreateIfNotExists.Value)
                operations.EnsureContainer();

            IsOpen = true;

            if (root != null)
            {
                // register the store (no changelog, nothing is restored)
                context.Register(root, (key, value, _) => Put(key, value));
            }
        }

        private static void Validate(CosmosRemoteStorageOptions options)
        {
            if (string.IsNullOrEmpty(options.ConnectionString))
            {
                if (string.IsNullOrEmpty(options.AccountEndpoint))
                    throw new StreamConfigException(
                        "Azure Cosmos DB remote store needs a ConnectionString, or an AccountEndpoint with an AccountKey or a Credential.");
                if (string.IsNullOrEmpty(options.AccountKey) && options.Credential == null)
                    throw new StreamConfigException(
                        "Azure Cosmos DB remote store needs an AccountKey or a Credential with the AccountEndpoint.");
            }

            if (options.BatchSize < 1 || options.BatchSize > CosmosRemoteStorageOptions.DEFAULT_BATCH_SIZE)
                throw new StreamConfigException(
                    $"Azure Cosmos DB remote store batch size must be between 1 and {CosmosRemoteStorageOptions.DEFAULT_BATCH_SIZE}.");

            if (!string.IsNullOrEmpty(options.ConsistencyLevel) &&
                !Enum.TryParse<ConsistencyLevel>(options.ConsistencyLevel, true, out _))
                throw new StreamConfigException(
                    $"Azure Cosmos DB remote store consistency level {options.ConsistencyLevel} is unknown " +
                    $"(possible values: {string.Join(", ", Enum.GetNames(typeof(ConsistencyLevel)))}).");
        }

        /// <summary>
        /// Container calls of the store (overridden in tests)
        /// </summary>
        protected virtual ICosmosOperations CreateOperations()
            => new CosmosOperations(options, ContainerName);

        public void Flush()
        {
            // every write is sent to Cosmos DB right away
        }

        public void Close()
        {
            IsOpen = false;
            operations?.Dispose();
            operations = null;
        }

        #endregion

        #region IKeyValueStore

        public byte[] Get(Bytes key)
            => Value(operations.Read(partition, Id(key)));

        public void Put(Bytes key, byte[] value)
        {
            if (value == null)
                operations.Delete(partition, Id(key));
            else
                operations.Upsert(Item(key.Get, value));
        }

        public byte[] PutIfAbsent(Bytes key, byte[] value)
        {
            var originalValue = Get(key);
            if (originalValue == null)
                Put(key, value);
            return originalValue;
        }

        // The upserts are sent in transactional batches (one partition: the task). A transactional delete fails when the item
        // doesn't exist, so null values are deleted one by one (a missing item is ignored). The last value of a key wins.
        public void PutAll(IEnumerable<KeyValuePair<Bytes, byte[]>> entries)
        {
            var lastValues = new Dictionary<string, KeyValuePair<byte[], byte[]>>();
            foreach (var entry in entries)
                lastValues[CosmosKeys.Hex(entry.Key.Get)] = new KeyValuePair<byte[], byte[]>(entry.Key.Get, entry.Value);

            int batchSize = options.BatchSize ?? CosmosRemoteStorageOptions.DEFAULT_BATCH_SIZE;
            var batch = new List<CosmosItem>(batchSize);
            foreach (var entry in lastValues.Values)
            {
                if (entry.Value == null)
                {
                    operations.Delete(partition, Id(entry.Key));
                    continue;
                }

                batch.Add(Item(entry.Key, entry.Value));
                if (batch.Count == batchSize)
                {
                    operations.UpsertBatch(partition, batch);
                    batch = new List<CosmosItem>(batchSize);
                }
            }

            if (batch.Count > 0)
                operations.UpsertBatch(partition, batch);
        }

        // the item is read and deleted in one transactional batch
        public byte[] Delete(Bytes key)
            => Value(operations.ReadAndDelete(partition, Id(key)));

        public IKeyValueEnumerator<Bytes, byte[]> Range(Bytes from, Bytes to)
            => new WrapEnumerableKeyValueEnumerator<Bytes, byte[]>(InternalRange(from, to, false));

        public IKeyValueEnumerator<Bytes, byte[]> ReverseRange(Bytes from, Bytes to)
            => new WrapEnumerableKeyValueEnumerator<Bytes, byte[]>(InternalRange(from, to, true));

        public IEnumerable<KeyValuePair<Bytes, byte[]>> All()
            => Query(null, null, false);

        public IEnumerable<KeyValuePair<Bytes, byte[]>> ReverseAll()
            => Query(null, null, true);

        // exact: a COUNT query on the task's partition
        public long ApproximateNumEntries()
            => operations.Count(partition);

        #endregion

        #region Private

        private string Id(Bytes key)
            => Id(key.Get);

        private static string Id(byte[] key)
            => CosmosKeys.Id(CosmosKeys.Hex(key), key);

        private CosmosItem Item(byte[] key, byte[] value)
        {
            var hex = CosmosKeys.Hex(key);
            return new CosmosItem
            {
                Id = CosmosKeys.Id(hex, key),
                T = partition,
                K = hex,
                V = Convert.ToBase64String(value)
            };
        }

        private static byte[] Value(CosmosItem item)
            => item?.V == null ? null : Convert.FromBase64String(item.V);

        // from and to are both included, null means no bound
        private IEnumerable<KeyValuePair<Bytes, byte[]>> InternalRange(Bytes from, Bytes to, bool descending)
        {
            if (from != null && to != null && bytesComparer.Compare(from, to) > 0)
            {
                log.LogWarning("Returning empty iterator for fetch with invalid key range: from > to. " +
                               "This may be due to range arguments set in the wrong order, " +
                               "or serdes that don't preserve ordering when lexicographically comparing the serialized bytes.");
                return Enumerable.Empty<KeyValuePair<Bytes, byte[]>>();
            }

            return Query(from == null ? null : CosmosKeys.Hex(from.Get), to == null ? null : CosmosKeys.Hex(to.Get), descending);
        }

        private IEnumerable<KeyValuePair<Bytes, byte[]>> Query(string fromHex, string toHex, bool descending)
            => operations.Query(partition, fromHex, toHex, descending)
                .Select(item => new KeyValuePair<Bytes, byte[]>(Bytes.Wrap(CosmosKeys.FromHex(item.K)), Value(item)));

        #endregion
    }
}
