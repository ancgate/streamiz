using Streamiz.Kafka.Net.State;

namespace Streamiz.Kafka.Net.Table.Internal
{
    internal class KTableSourceValueGetterSupplier<K, V> : IKTableValueGetterSupplier<K, V>
    {
        internal class KTableSourceValueGetter : IKTableVersionedValueGetter<K, V>
        {
            private readonly string storeName;
            private KeyValueStoreWrapper<K, V> store;

            public KTableSourceValueGetter(string storeName)
            {
                this.storeName = storeName;
            }

            public void Close() { }

            public ValueAndTimestamp<V> Get(K key) => store.Get(key);

            public bool IsVersioned => store.IsVersioned;

            public ValueAndTimestamp<V> Get(K key, long asOfTimestamp) => store.Get(key, asOfTimestamp);

            public void Init(ProcessorContext context)
            {
                store = new KeyValueStoreWrapper<K, V>(context, storeName);
            }

        }

        private string queryableStoreName;

        public KTableSourceValueGetterSupplier(string queryableStoreName)
        {
            this.queryableStoreName = queryableStoreName;
        }

        public string[] StoreNames => new string[1] { queryableStoreName };

        public IKTableValueGetter<K, V> Get() => new KTableSourceValueGetter(this.queryableStoreName);
    }
}
