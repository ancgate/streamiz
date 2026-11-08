using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State.Cache;
using Streamiz.Kafka.Net.State.Logging;
using Streamiz.Kafka.Net.State.Metered;
using Streamiz.Kafka.Net.State.Supplier;

namespace Streamiz.Kafka.Net.State
{
    /// <summary>
    /// Build a <see cref="ISessionStore{K, AGG}"/>: metered, optionally cached and change-logged (enabled by default).
    /// The changelog topic is compacted and deleted after the retention (like window stores).
    /// </summary>
    /// <typeparam name="K">key type</typeparam>
    /// <typeparam name="V">aggregate type</typeparam>
    public class SessionStoreBuilder<K, V>
        : AbstractStoreBuilder<K, V, ISessionStore<K, V>>
    {
        private readonly ISessionBytesStoreSupplier supplier;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="supplier">supplier of the bytes session store</param>
        /// <param name="keySerde">key serdes</param>
        /// <param name="valueSerde">aggregate serdes</param>
        public SessionStoreBuilder(ISessionBytesStoreSupplier supplier, ISerDes<K> keySerde, ISerDes<V> valueSerde)
            : base(supplier.Name, keySerde, valueSerde)
        {
            this.supplier = supplier;
        }

        /// <summary>
        /// Session stores use the windowed changelog configuration (compact and delete, with the retention)
        /// </summary>
        public override bool IsWindowStore => true;

        /// <summary>
        /// Retention of the store (in milliseconds)
        /// </summary>
        public override long RetentionMs => supplier.Retention;

        /// <summary>
        /// Build the state store
        /// </summary>
        /// <param name="config">stream configuration</param>
        /// <returns>the session store</returns>
        public override ISessionStore<K, V> Build(IStreamConfig config)
        {
            var store = supplier.Get();

            return new MeteredSessionStore<K, V>(
                WrapCaching(WrapLogging(store), config),
                keySerdes,
                valueSerdes,
                supplier.MetricsScope);
        }

        private ISessionStore<Bytes, byte[]> WrapLogging(ISessionStore<Bytes, byte[]> inner)
            => LoggingEnabled ? new ChangeLoggingSessionBytesStore(inner) : inner;

        private ISessionStore<Bytes, byte[]> WrapCaching(ISessionStore<Bytes, byte[]> inner, IStreamConfig config)
            => MustWrapCache(config)
                ? new CachingSessionStore(inner, supplier.SegmentInterval, CacheSize)
                : inner;
    }
}
