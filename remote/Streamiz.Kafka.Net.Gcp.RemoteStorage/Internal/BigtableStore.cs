using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Extensions.Logging;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.State.Enumerator;

namespace Streamiz.Kafka.Net.Gcp.RemoteStorage.Internal
{
    /// <summary>
    /// Key-value store kept in a GCP Bigtable table. The rows of a store's task share a prefix
    /// (see <see cref="BigtableRowKeys"/>), so several applications, stores and tasks can use the same table,
    /// and ranges are sorted by key bytes like the local stores.
    /// The data is durable in Bigtable, so the store has no changelog topic (<see cref="IsLocally"/> is false).
    /// </summary>
    internal class BigtableStore : IKeyValueStore<Bytes, byte[]>
    {
        private static readonly ILogger log = Logger.GetLogger(typeof(BigtableStore));
        private static readonly BytesComparer bytesComparer = new();

        private readonly GcpRemoteStorageOptions options;
        private IBigtableOperations operations;
        private byte[] prefix;
        private byte[] prefixEnd;

        public BigtableStore(GcpRemoteStorageOptions options, string name)
        {
            this.options = options ?? new GcpRemoteStorageOptions();
            Name = name;
        }

        public string Name { get; }
        public bool Persistent => true;
        public bool IsLocally => false; // no changelog topic
        public bool IsOpen { get; private set; }

        internal GcpRemoteStorageOptions Options => options;

        #region IStateStore

        public void Init(ProcessorContext context, IStateStore root)
        {
            var configured = context.Configuration?.Read<GcpRemoteStorageOptions>();
            if (configured != null)
            {
                options.ProjectId ??= configured.ProjectId;
                options.InstanceId ??= configured.InstanceId;
                options.TableId ??= configured.TableId;
                options.ColumnFamily ??= configured.ColumnFamily;
                options.AppProfileId ??= configured.AppProfileId;
                options.EmulatorHost ??= configured.EmulatorHost;
                options.CredentialsPath ??= configured.CredentialsPath;
                options.CreateTableIfNotExists ??= configured.CreateTableIfNotExists;
                options.BatchSize ??= configured.BatchSize;
            }

            options.TableId ??= GcpRemoteStorageOptions.DEFAULT_TABLE_ID;
            options.ColumnFamily ??= GcpRemoteStorageOptions.DEFAULT_COLUMN_FAMILY;
            options.CreateTableIfNotExists ??= true;
            options.BatchSize ??= GcpRemoteStorageOptions.DEFAULT_BATCH_SIZE;

            if (string.IsNullOrEmpty(options.ProjectId) || string.IsNullOrEmpty(options.InstanceId))
                throw new StreamConfigException(
                    "GCP Bigtable remote store needs the ProjectId and InstanceId configuration.");
            if (options.BatchSize <= 0)
                throw new StreamConfigException("GCP Bigtable remote store batch size must be greater than 0.");

            prefix = BigtableRowKeys.Prefix(context.Configuration?.ApplicationId, Name, context.Id.ToString());
            prefixEnd = BigtableRowKeys.PrefixEnd(prefix);

            operations = CreateOperations();
            if (options.CreateTableIfNotExists.Value)
                operations.EnsureTable();

            IsOpen = true;

            if (root != null)
            {
                // register the store (no changelog, nothing is restored)
                context.Register(root, (key, value, _) => Put(key, value));
            }
        }

        /// <summary>
        /// Bigtable client of the store (overridden in tests)
        /// </summary>
        protected virtual IBigtableOperations CreateOperations()
            => new BigtableOperations(options);

        public void Flush()
        {
            // every write is sent to Bigtable right away
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
            => operations.Read(RowKey(key));

        public void Put(Bytes key, byte[] value)
            => operations.Write(RowKey(key), value); // a null value deletes the row

        public byte[] PutIfAbsent(Bytes key, byte[] value)
        {
            var originalValue = Get(key);
            if (originalValue == null)
                Put(key, value);
            return originalValue;
        }

        public void PutAll(IEnumerable<KeyValuePair<Bytes, byte[]>> entries)
        {
            int batchSize = options.BatchSize ?? GcpRemoteStorageOptions.DEFAULT_BATCH_SIZE;
            var batch = new List<KeyValuePair<byte[], byte[]>>(batchSize);
            foreach (var entry in entries)
            {
                batch.Add(new KeyValuePair<byte[], byte[]>(RowKey(entry.Key), entry.Value));
                if (batch.Count == batchSize)
                {
                    operations.WriteAll(batch);
                    batch = new List<KeyValuePair<byte[], byte[]>>(batchSize);
                }
            }

            if (batch.Count > 0)
                operations.WriteAll(batch);
        }

        // Not atomic: the old value is read, then the row is deleted. A store's task is the only writer of its rows.
        public byte[] Delete(Bytes key)
        {
            var rowKey = RowKey(key);
            var oldValue = operations.Read(rowKey);
            if (oldValue != null)
                operations.Write(rowKey, null);
            return oldValue;
        }

        public IKeyValueEnumerator<Bytes, byte[]> Range(Bytes from, Bytes to)
            => new WrapEnumerableKeyValueEnumerator<Bytes, byte[]>(InternalRange(from, to, false));

        public IKeyValueEnumerator<Bytes, byte[]> ReverseRange(Bytes from, Bytes to)
            => new WrapEnumerableKeyValueEnumerator<Bytes, byte[]>(InternalRange(from, to, true));

        public IEnumerable<KeyValuePair<Bytes, byte[]>> All()
            => Scan(prefix, prefixEnd, false);

        public IEnumerable<KeyValuePair<Bytes, byte[]>> ReverseAll()
            => Scan(prefix, prefixEnd, true);

        // Bigtable has no cheap row count: counting would scan the whole task range
        public long ApproximateNumEntries()
            => 0;

        #endregion

        #region Private

        private byte[] RowKey(Bytes key)
            => BigtableRowKeys.RowKey(prefix, key.Get);

        // from and to are both included, null means no bound
        private IEnumerable<KeyValuePair<Bytes, byte[]>> InternalRange(Bytes from, Bytes to, bool reversed)
        {
            if (from != null && to != null && bytesComparer.Compare(from, to) > 0)
            {
                log.LogWarning("Returning empty iterator for fetch with invalid key range: from > to. " +
                               "This may be due to range arguments set in the wrong order, " +
                               "or serdes that don't preserve ordering when lexicographically comparing the serialized bytes.");
                return Enumerable.Empty<KeyValuePair<Bytes, byte[]>>();
            }

            var start = from == null ? prefix : RowKey(from);
            var end = to == null ? prefixEnd : BigtableRowKeys.Successor(RowKey(to));
            return Scan(start, end, reversed);
        }

        private IEnumerable<KeyValuePair<Bytes, byte[]>> Scan(byte[] startClosed, byte[] endOpen, bool reversed)
        {
            foreach (var row in operations.Scan(startClosed, endOpen, reversed))
            {
                // the range only covers the task's rows, this is a safety net
                if (BigtableRowKeys.HasPrefix(row.Key, prefix))
                    yield return new KeyValuePair<Bytes, byte[]>(
                        Bytes.Wrap(BigtableRowKeys.Key(row.Key, prefix)), row.Value);
            }
        }

        #endregion
    }
}
