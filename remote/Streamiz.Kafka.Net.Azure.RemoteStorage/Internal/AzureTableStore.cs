using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Azure.Data.Tables;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.State.Enumerator;

namespace Streamiz.Kafka.Net.Azure.RemoteStorage.Internal
{
    internal class AzureTableStore : IKeyValueStore<Bytes, byte[]>
    {
        // a table transaction holds at most 100 operations
        internal const int MaxTransactionSize = 100;

        private static readonly BytesComparer _bytesComparer = new BytesComparer();

        private readonly AzureRemoteStorageOptions _azureRemoteStorageOptions;
        private TableClient _tableClient;
        private string _partitionKey;
        private readonly string _azureTableName;

        public string Name { get; }
        public bool Persistent => true;

        public bool IsLocally => false; // avoid changelog topic
        public bool IsOpen { get; private set; }

        public AzureTableStore(AzureRemoteStorageOptions azureRemoteStorageOptions,
            string name)
        {
            _azureRemoteStorageOptions = azureRemoteStorageOptions ?? new AzureRemoteStorageOptions();
            _azureTableName = ToTableName(name);
            Name = name;
        }

        #region IKeyValueStore impl

        public void Init(ProcessorContext context, IStateStore root)
        {
            _partitionKey = context.Id.ToString();
            var remoteStorageOptions = context.Configuration?.Read<AzureRemoteStorageOptions>();

            if (remoteStorageOptions != null)
            {
                _azureRemoteStorageOptions.StorageUri = _azureRemoteStorageOptions.StorageUri ?? remoteStorageOptions.StorageUri;
                _azureRemoteStorageOptions.StorageAccountKey = _azureRemoteStorageOptions.StorageAccountKey ?? remoteStorageOptions.StorageAccountKey;
                _azureRemoteStorageOptions.AccountName = _azureRemoteStorageOptions.AccountName ?? remoteStorageOptions.AccountName;
            }

            if (_azureRemoteStorageOptions.StorageUri == null ||
                _azureRemoteStorageOptions.StorageAccountKey == null ||
                _azureRemoteStorageOptions.AccountName == null)
                throw new StreamConfigException(
                    "Azure remote store needs StorageUri, StorageAccountKey and AccountName configuration.");

            _tableClient = CreateTableClient();
            _tableClient.CreateIfNotExists(CancellationToken.None);

            IsOpen = true;

            if (root != null)
            {
                // register the store
                context.Register(root, (key, value, _) => Put(key, value));
            }
        }

        protected virtual TableClient CreateTableClient()
        {
            return new TableClient(
                new Uri(_azureRemoteStorageOptions.StorageUri),
                _azureTableName,
                new TableSharedKeyCredential(_azureRemoteStorageOptions.AccountName,
                    _azureRemoteStorageOptions.StorageAccountKey));
        }

        public void Flush()
        {
            /* nothing */
        }

        public void Close()
        {
            /* nothing */
        }

        public byte[] Get(Bytes key)
        {
            var hashKey = ToHashKey(key);

            var entity = _tableClient.GetEntityIfExists<AzureTableEntity>(
                _partitionKey,
                hashKey);

            return entity.HasValue ? entity.Value.Value : null;
        }

        public IKeyValueEnumerator<Bytes, byte[]> Range(Bytes from, Bytes to)
            => new WrapEnumerableKeyValueEnumerator<Bytes, byte[]>(InternalRange(from, to, true));

        public IKeyValueEnumerator<Bytes, byte[]> ReverseRange(Bytes from, Bytes to)
            => new WrapEnumerableKeyValueEnumerator<Bytes, byte[]>(InternalRange(from, to, false));

        public IEnumerable<KeyValuePair<Bytes, byte[]>> All()
            => InternalAll(true);

        public IEnumerable<KeyValuePair<Bytes, byte[]>> ReverseAll()
            => InternalAll(false);

        // Azure Tables can't count the entities without reading all of them: not worth it for an approximation
        public long ApproximateNumEntries()
            => 0;

        public void Put(Bytes key, byte[] value)
        {
            // a null value is a delete, as for the other key-value stores
            if (value == null)
            {
                DeleteEntity(key);
                return;
            }

            var hashKey = ToHashKey(key);
            var entity = new AzureTableEntity
            {
                Value = value,
                Key = key.Get,
                PartitionKey = _partitionKey,
                RowKey = hashKey
            };

            _tableClient.UpsertEntity(entity, TableUpdateMode.Replace, CancellationToken.None);
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
            // a transaction can't hold the same entity twice: the last value of each key wins
            var lastValues = new Dictionary<Bytes, byte[]>(_bytesComparer);
            var keys = new List<Bytes>();
            foreach (var entry in entries)
            {
                if (!lastValues.ContainsKey(entry.Key))
                    keys.Add(entry.Key);
                lastValues[entry.Key] = entry.Value;
            }

            // a transactional delete fails when the entity doesn't exist: deletes are sent one by one
            foreach (var key in keys.Where(k => lastValues[k] == null))
                DeleteEntity(key);

            var upserts = keys
                .Where(k => lastValues[k] != null)
                .Select(k => new TableTransactionAction(TableTransactionActionType.UpsertReplace,
                    new AzureTableEntity
                    {
                        Key = k.Get,
                        Value = lastValues[k],
                        PartitionKey = _partitionKey,
                        RowKey = ToHashKey(k)
                    }))
                .ToList();

            // all the entities are in the same partition (the task), but a transaction holds at most 100 operations
            for (int i = 0; i < upserts.Count; i += MaxTransactionSize)
                _tableClient.SubmitTransaction(upserts.Skip(i).Take(MaxTransactionSize).ToList());
        }

        public byte[] Delete(Bytes key)
        {
            var oldValue = Get(key);
            if (oldValue != null)
                DeleteEntity(key);
            return oldValue;
        }

        #endregion

        #region Private

        // Azure table names: alphanumeric characters only, starting with a letter, 3 to 63 characters
        internal static string ToTableName(string storeName)
        {
            var tableName = new string((storeName ?? string.Empty).Where(char.IsLetterOrDigit).ToArray());
            if (tableName.Length == 0 || !char.IsLetter(tableName[0]))
                tableName = "t" + tableName;
            if (tableName.Length < 3)
                tableName = tableName.PadRight(3, '0');
            return tableName.Length > 63 ? tableName.Substring(0, 63) : tableName;
        }

        // DeleteEntity doesn't fail when the entity doesn't exist
        private void DeleteEntity(Bytes key)
            => _tableClient.DeleteEntity(_partitionKey, ToHashKey(key));

        private string ToHashKey(Bytes bytesKey)
        {
            return Convert.ToBase64String(bytesKey.Get, Base64FormattingOptions.None);
        }

        // the entities of this task, ordered by key bytes. Only the partition filter is sent to Azure (it can't
        // compare byte arrays, and the Base64 RowKey doesn't keep the byte order): the key range is applied here.
        private IEnumerable<KeyValuePair<Bytes, byte[]>> QueryPartition(Func<Bytes, bool> keyFilter, bool forward)
        {
            var partitionKey = _partitionKey;
            var entries = _tableClient
                .Query<AzureTableEntity>(t => t.PartitionKey == partitionKey)
                .Select(e => new KeyValuePair<Bytes, byte[]>(Bytes.Wrap(e.Key), e.Value))
                .Where(kv => keyFilter(kv.Key))
                .ToList();

            entries.Sort((kv1, kv2) => forward
                ? _bytesComparer.Compare(kv1.Key, kv2.Key)
                : _bytesComparer.Compare(kv2.Key, kv1.Key));
            return entries;
        }

        private IEnumerable<KeyValuePair<Bytes, byte[]>> InternalAll(bool forward)
            => QueryPartition(_ => true, forward);

        private IEnumerable<KeyValuePair<Bytes, byte[]>> InternalRange(Bytes from, Bytes to, bool forward)
            => QueryPartition(key =>
                (from == null || _bytesComparer.Compare(key, from) >= 0) &&
                (to == null || _bytesComparer.Compare(key, to) <= 0), forward);

        #endregion
    }
}
