using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.State;

namespace Streamiz.Kafka.Net.Table.Internal
{
    /// <summary>
    /// The store of a KTable: a timestamped key-value store or a versioned key-value store (KIP-889).
    /// </summary>
    internal class KeyValueStoreWrapper<K, V>
    {
        /// <summary>
        /// Return code of <see cref="Put"/> when the record is the latest of its key.
        /// </summary>
        public const long PUT_RETURN_CODE_IS_LATEST = VersionedKeyValueStoreReturnCodes.PUT_RETURN_CODE_VALID_TO_UNDEFINED;

        private readonly ITimestampedKeyValueStore<K, V> timestampedStore;
        private readonly IVersionedKeyValueStore<K, V> versionedStore;

        public KeyValueStoreWrapper(ProcessorContext context, string storeName)
        {
            var store = context.GetStateStore(storeName);
            switch (store)
            {
                case ITimestampedKeyValueStore<K, V> timestamped:
                    timestampedStore = timestamped;
                    break;
                case IVersionedKeyValueStore<K, V> versioned:
                    versionedStore = versioned;
                    break;
                default:
                    throw new InvalidStateStoreException(
                        $"KTable source state store {storeName} must be a timestamped key-value store or a versioned key-value store, not {store?.GetType().Name ?? "null"}");
            }
        }

        public IStateStore Store => (IStateStore) timestampedStore ?? versionedStore;

        public bool IsVersioned => versionedStore != null;

        public string Name => Store.Name;

        /// <summary>
        /// Latest value of the key
        /// </summary>
        public ValueAndTimestamp<V> Get(K key)
        {
            if (timestampedStore != null)
                return timestampedStore.Get(key);

            var record = versionedStore.Get(key);
            return record == null ? null : ValueAndTimestamp<V>.Make(record.Value, record.Timestamp);
        }

        /// <summary>
        /// Value of the key as of a timestamp (versioned store only)
        /// </summary>
        public ValueAndTimestamp<V> Get(K key, long asOfTimestamp)
        {
            if (versionedStore == null)
                throw new IllegalStateException($"The store {Name} is not versioned: it can't be read as of a timestamp");

            var record = versionedStore.Get(key, asOfTimestamp);
            return record == null ? null : ValueAndTimestamp<V>.Make(record.Value, record.Timestamp);
        }

        /// <summary>
        /// Put a record
        /// </summary>
        /// <returns><see cref="PUT_RETURN_CODE_IS_LATEST"/> if the record is the latest value of its key,
        /// <see cref="VersionedKeyValueStoreReturnCodes.PUT_RETURN_CODE_NOT_PUT"/> if it was not put, otherwise its validTo</returns>
        public long Put(K key, V value, long timestamp)
        {
            if (timestampedStore != null)
            {
                timestampedStore.Put(key, ValueAndTimestamp<V>.Make(value, timestamp));
                return PUT_RETURN_CODE_IS_LATEST;
            }

            return versionedStore.Put(key, value, timestamp);
        }
    }
}
