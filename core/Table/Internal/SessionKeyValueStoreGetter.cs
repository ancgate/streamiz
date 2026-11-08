using Streamiz.Kafka.Net.State;

namespace Streamiz.Kafka.Net.Table.Internal
{
    /// <summary>
    /// Value getter of a session windowed KTable: the aggregate of the session, with the session end as timestamp.
    /// </summary>
    internal class SessionKeyValueStoreGetter<K, V> : IKTableValueGetter<Windowed<K>, V>
    {
        private readonly string storeName;
        private ISessionStore<K, V> store;

        public SessionKeyValueStoreGetter(string storeName)
        {
            this.storeName = storeName;
        }

        public void Close() { }

        public ValueAndTimestamp<V> Get(Windowed<K> key)
        {
            var value = store.FetchSession(key.Key, key.Window.StartMs, key.Window.EndMs);
            return value == null ? null : ValueAndTimestamp<V>.Make(value, key.Window.EndMs);
        }

        public void Init(ProcessorContext context)
        {
            store = (ISessionStore<K, V>)context.GetStateStore(storeName);
        }
    }
}
