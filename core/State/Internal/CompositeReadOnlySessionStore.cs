using System;
using System.Collections.Generic;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.State.Enumerator;

namespace Streamiz.Kafka.Net.State.Internal
{
    /// <summary>
    /// Read-only session store over the session stores of all the tasks (interactive queries).
    /// </summary>
    internal class CompositeReadOnlySessionStore<K, V> : IReadOnlySessionStore<K, V>
    {
        private readonly IStateStoreProvider<IReadOnlySessionStore<K, V>, K, V> storeProvider;
        private readonly IQueryableStoreType<IReadOnlySessionStore<K, V>, K, V> storeType;
        private readonly string storeName;

        public CompositeReadOnlySessionStore(
            IStateStoreProvider<IReadOnlySessionStore<K, V>, K, V> storeProvider,
            IQueryableStoreType<IReadOnlySessionStore<K, V>, K, V> storeType,
            string storeName)
        {
            this.storeProvider = storeProvider;
            this.storeType = storeType;
            this.storeName = storeName;
        }

        private IEnumerable<IReadOnlySessionStore<K, V>> Stores => storeProvider.Stores(storeName, storeType);

        // a key lives in one task: return the sessions of the first store which has some
        private IKeyValueEnumerator<Windowed<K>, V> FirstNonEmpty(Func<IReadOnlySessionStore<K, V>, IKeyValueEnumerator<Windowed<K>, V>> fetch)
        {
            try
            {
                foreach (var store in Stores)
                {
                    var enumerator = fetch(store);
                    if (!enumerator.MoveNext())
                        enumerator.Dispose();
                    else
                    {
                        enumerator.Reset();
                        return enumerator;
                    }
                }

                return new EmptyKeyValueEnumerator<Windowed<K>, V>();
            }
            catch (InvalidStateStoreException e)
            {
                throw new InvalidStateStoreException("State store is not available anymore and may have " +
                    "been migrated to another instance; please re-discover its location from the state metadata.", e);
            }
        }

        private IKeyValueEnumerator<Windowed<K>, V> AllStores(Func<IReadOnlySessionStore<K, V>, IKeyValueEnumerator<Windowed<K>, V>> fetch)
            => new CompositeKeyValueEnumerator<Windowed<K>, V, IReadOnlySessionStore<K, V>>(Stores, fetch);

        public IKeyValueEnumerator<Windowed<K>, V> FindSessions(K key, long earliestSessionEndTime, long latestSessionStartTime)
            => FirstNonEmpty(store => store.FindSessions(key, earliestSessionEndTime, latestSessionStartTime));

        public IKeyValueEnumerator<Windowed<K>, V> FindSessions(K keyFrom, K keyTo, long earliestSessionEndTime, long latestSessionStartTime)
            => AllStores(store => store.FindSessions(keyFrom, keyTo, earliestSessionEndTime, latestSessionStartTime));

        public IKeyValueEnumerator<Windowed<K>, V> BackwardFindSessions(K key, long earliestSessionEndTime, long latestSessionStartTime)
            => FirstNonEmpty(store => store.BackwardFindSessions(key, earliestSessionEndTime, latestSessionStartTime));

        public IKeyValueEnumerator<Windowed<K>, V> BackwardFindSessions(K keyFrom, K keyTo, long earliestSessionEndTime, long latestSessionStartTime)
            => AllStores(store => store.BackwardFindSessions(keyFrom, keyTo, earliestSessionEndTime, latestSessionStartTime));

        public V FetchSession(K key, long sessionStartTime, long sessionEndTime)
        {
            try
            {
                foreach (var store in Stores)
                {
                    var result = store.FetchSession(key, sessionStartTime, sessionEndTime);
                    if (result != null)
                        return result;
                }

                return default;
            }
            catch (InvalidStateStoreException e)
            {
                throw new InvalidStateStoreException("State store is not available anymore and may have " +
                    "been migrated to another instance; please re-discover its location from the state metadata.", e);
            }
        }

        public IKeyValueEnumerator<Windowed<K>, V> Fetch(K key)
            => FirstNonEmpty(store => store.Fetch(key));

        public IKeyValueEnumerator<Windowed<K>, V> Fetch(K keyFrom, K keyTo)
            => AllStores(store => store.Fetch(keyFrom, keyTo));

        public IKeyValueEnumerator<Windowed<K>, V> BackwardFetch(K key)
            => FirstNonEmpty(store => store.BackwardFetch(key));

        public IKeyValueEnumerator<Windowed<K>, V> BackwardFetch(K keyFrom, K keyTo)
            => AllStores(store => store.BackwardFetch(keyFrom, keyTo));
    }
}
