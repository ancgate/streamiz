using System.Collections.Generic;
using System.Linq;
using Microsoft.Extensions.Logging;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.State.Enumerator;

namespace Streamiz.Kafka.Net.MongoDb.RemoteStorage.Internal
{
    /// <summary>
    /// Key-value store kept in a MongoDB collection (one collection per store). Each document holds one key of one task
    /// (see <see cref="IMongoDbOperations"/>); the key is a hex string, so ranges are sorted by key bytes like the local stores.
    /// The data is durable in MongoDB, so the store has no changelog topic (<see cref="IsLocally"/> is false).
    /// </summary>
    internal class MongoDbStore : IKeyValueStore<Bytes, byte[]>
    {
        private static readonly ILogger log = Logger.GetLogger(typeof(MongoDbStore));
        private static readonly BytesComparer bytesComparer = new();

        private readonly MongoDbRemoteStorageOptions options;
        private IMongoDbOperations operations;
        private string task;

        public MongoDbStore(MongoDbRemoteStorageOptions options, string name)
        {
            this.options = options ?? new MongoDbRemoteStorageOptions();
            Name = name;
        }

        public string Name { get; }
        public bool Persistent => true;
        public bool IsLocally => false; // no changelog topic
        public bool IsOpen { get; private set; }

        internal MongoDbRemoteStorageOptions Options => options;
        internal string CollectionName { get; private set; }

        #region IStateStore

        public void Init(ProcessorContext context, IStateStore root)
        {
            var configured = context.Configuration?.Read<MongoDbRemoteStorageOptions>();
            if (configured != null)
            {
                options.ConnectionString ??= configured.ConnectionString;
                options.DatabaseName ??= configured.DatabaseName;
                options.CollectionPrefix ??= configured.CollectionPrefix;
                options.CreateIndex ??= configured.CreateIndex;
                options.BatchSize ??= configured.BatchSize;
            }

            options.DatabaseName ??= MongoDbRemoteStorageOptions.DEFAULT_DATABASE_NAME;
            options.CreateIndex ??= true;
            options.BatchSize ??= MongoDbRemoteStorageOptions.DEFAULT_BATCH_SIZE;

            if (string.IsNullOrEmpty(options.ConnectionString))
                throw new StreamConfigException("MongoDB remote store needs the ConnectionString configuration.");
            if (options.BatchSize <= 0)
                throw new StreamConfigException("MongoDB remote store batch size must be greater than 0.");

            task = context.Id.ToString();
            CollectionName = MongoDbKeys.CollectionName(options.CollectionPrefix, Name);

            operations = CreateOperations(CollectionName);
            if (options.CreateIndex.Value)
                operations.EnsureIndex();

            IsOpen = true;

            if (root != null)
            {
                // register the store (no changelog, nothing is restored)
                context.Register(root, (key, value, _) => Put(key, value));
            }
        }

        /// <summary>
        /// MongoDB client of the store (overridden in tests)
        /// </summary>
        protected virtual IMongoDbOperations CreateOperations(string collectionName)
            => new MongoDbOperations(options, collectionName);

        public void Flush()
        {
            // every write is sent to MongoDB right away
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
            => operations.Get(task, MongoDbKeys.ToHex(key.Get));

        public void Put(Bytes key, byte[] value)
            => operations.Put(new MongoDbWrite(task, MongoDbKeys.ToHex(key.Get), value)); // a null value deletes the document

        public byte[] PutIfAbsent(Bytes key, byte[] value)
        {
            var originalValue = Get(key);
            if (originalValue == null)
                Put(key, value);
            return originalValue;
        }

        public void PutAll(IEnumerable<KeyValuePair<Bytes, byte[]>> entries)
        {
            int batchSize = options.BatchSize ?? MongoDbRemoteStorageOptions.DEFAULT_BATCH_SIZE;
            var batch = new List<MongoDbWrite>(batchSize);
            foreach (var entry in entries)
            {
                batch.Add(new MongoDbWrite(task, MongoDbKeys.ToHex(entry.Key.Get), entry.Value));
                if (batch.Count == batchSize)
                {
                    operations.BulkWrite(batch);
                    batch = new List<MongoDbWrite>(batchSize);
                }
            }

            if (batch.Count > 0)
                operations.BulkWrite(batch);
        }

        public byte[] Delete(Bytes key)
            => operations.Delete(task, MongoDbKeys.ToHex(key.Get));

        public IKeyValueEnumerator<Bytes, byte[]> Range(Bytes from, Bytes to)
            => new WrapEnumerableKeyValueEnumerator<Bytes, byte[]>(InternalRange(from, to, false));

        public IKeyValueEnumerator<Bytes, byte[]> ReverseRange(Bytes from, Bytes to)
            => new WrapEnumerableKeyValueEnumerator<Bytes, byte[]>(InternalRange(from, to, true));

        public IEnumerable<KeyValuePair<Bytes, byte[]>> All()
            => Find(null, null, false);

        public IEnumerable<KeyValuePair<Bytes, byte[]>> ReverseAll()
            => Find(null, null, true);

        // exact count of the task's documents (it reads the { t, k } index entries of the task)
        public long ApproximateNumEntries()
            => operations.Count(task);

        #endregion

        #region Private

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

            return Find(from == null ? null : MongoDbKeys.ToHex(from.Get),
                to == null ? null : MongoDbKeys.ToHex(to.Get), descending);
        }

        private IEnumerable<KeyValuePair<Bytes, byte[]>> Find(string fromHex, string toHex, bool descending)
            => operations
                .Find(task, fromHex, toHex, descending)
                .Select(document => new KeyValuePair<Bytes, byte[]>(
                    Bytes.Wrap(MongoDbKeys.FromHex(document.Key)), document.Value));

        #endregion
    }
}
