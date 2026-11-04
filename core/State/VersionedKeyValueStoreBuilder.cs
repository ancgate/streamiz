using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State.Logging;
using Streamiz.Kafka.Net.State.Metered;
using Streamiz.Kafka.Net.State.Supplier;

namespace Streamiz.Kafka.Net.State
{
    /// <summary>
    /// A store builder which builds versioned stores (history retention known by the changelog topic config).
    /// </summary>
    internal interface IVersionedStoreBuilder
    {
        long HistoryRetentionMs { get; }
    }

    /// <summary>
    /// Build a <see cref="IVersionedKeyValueStore{K, V}"/>: metered, with change logging (enabled by default).
    /// Versioned stores are never cached.
    /// </summary>
    /// <typeparam name="K">key type</typeparam>
    /// <typeparam name="V">value type</typeparam>
    public class VersionedKeyValueStoreBuilder<K, V>
        : AbstractStoreBuilder<K, V, IVersionedKeyValueStore<K, V>>, IVersionedStoreBuilder
    {
        private readonly IVersionedBytesStoreSupplier supplier;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="supplier">versioned store supplier</param>
        /// <param name="keySerde">key serdes</param>
        /// <param name="valueSerde">value serdes</param>
        public VersionedKeyValueStoreBuilder(IVersionedBytesStoreSupplier supplier, ISerDes<K> keySerde, ISerDes<V> valueSerde)
            : base(supplier.Name, keySerde, valueSerde)
        {
            this.supplier = supplier;
        }

        /// <summary>
        /// A versioned store is not a window store
        /// </summary>
        public override bool IsWindowStore => false;

        /// <summary>
        /// History retention of the store
        /// </summary>
        public override long RetentionMs => supplier.HistoryRetentionMs;

        long IVersionedStoreBuilder.HistoryRetentionMs => supplier.HistoryRetentionMs;

        /// <summary>
        /// Build the store
        /// </summary>
        /// <param name="config">stream configuration</param>
        /// <returns>a versioned store</returns>
        public override IVersionedKeyValueStore<K, V> Build(IStreamConfig config)
        {
            var store = supplier.GetVersionedStore();
            return new MeteredVersionedKeyValueStore<K, V>(
                LoggingEnabled ? new ChangeLoggingVersionedKeyValueBytesStore(store) : store,
                keySerdes,
                valueSerdes,
                supplier.MetricsScope);
        }
    }
}
