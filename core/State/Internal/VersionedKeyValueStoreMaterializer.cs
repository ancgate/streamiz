using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.State.Supplier;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Kafka.Net.State.Internal
{
    /// <summary>
    /// Build a versioned store for a source KTable materialized with a <see cref="IVersionedBytesStoreSupplier"/>.
    /// Versioned stores are never cached: the caching flags of the <see cref="Materialized{K,V,S}"/> are ignored.
    /// </summary>
    internal class VersionedKeyValueStoreMaterializer<K, V>
    {
        private readonly Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materialized;

        public VersionedKeyValueStoreMaterializer(Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materialized)
        {
            this.materialized = materialized;
        }

        internal static bool IsVersioned(Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materialized)
            => materialized?.StoreSupplier is IVersionedBytesStoreSupplier;

        public IStoreBuilder<IVersionedKeyValueStore<K, V>> Materialize()
        {
            var supplier = (IVersionedBytesStoreSupplier) materialized.StoreSupplier;
            var builder = Stores.VersionedKeyValueStoreBuilder(supplier, materialized.KeySerdes, materialized.ValueSerdes);

            if (materialized.LoggingEnabled)
                builder.WithLoggingEnabled(materialized.TopicConfig);
            else
                builder.WithLoggingDisabled();

            return builder;
        }
    }
}
