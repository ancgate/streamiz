using System;
using System.Collections.Generic;
using Streamiz.Kafka.Net.Errors;

namespace Streamiz.Kafka.Net.State.Internal
{
    /// <summary>
    /// Read-only versioned store over the stores of all the tasks of this instance (interactive queries).
    /// </summary>
    internal class CompositeReadOnlyVersionedKeyValueStore<K, V> : IReadOnlyVersionedKeyValueStore<K, V>
    {
        private readonly IStateStoreProvider<IReadOnlyVersionedKeyValueStore<K, V>, K, V> storeProvider;
        private readonly IQueryableStoreType<IReadOnlyVersionedKeyValueStore<K, V>, K, V> storeType;
        private readonly string storeName;

        public CompositeReadOnlyVersionedKeyValueStore(
            IStateStoreProvider<IReadOnlyVersionedKeyValueStore<K, V>, K, V> storeProvider,
            IQueryableStoreType<IReadOnlyVersionedKeyValueStore<K, V>, K, V> storeType,
            string storeName)
        {
            this.storeProvider = storeProvider;
            this.storeType = storeType;
            this.storeName = storeName;
        }

        private IEnumerable<IReadOnlyVersionedKeyValueStore<K, V>> Stores => storeProvider.Stores(storeName, storeType);

        // a key lives in one task: return the record of the first store which has one
        private VersionedRecord<V> First(Func<IReadOnlyVersionedKeyValueStore<K, V>, VersionedRecord<V>> get)
        {
            try
            {
                foreach (var store in Stores)
                {
                    var record = get(store);
                    if (record != null)
                        return record;
                }

                return null;
            }
            catch (InvalidStateStoreException e)
            {
                throw new InvalidStateStoreException("State store is not available anymore and may have " +
                    "been migrated to another instance; please re-discover its location from the state metadata.", e);
            }
        }

        public VersionedRecord<V> Get(K key)
            => First(store => store.Get(key));

        public VersionedRecord<V> Get(K key, long asOfTimestamp)
            => First(store => store.Get(key, asOfTimestamp));
    }
}
