using System;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.State.Supplier;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Kafka.Net.State.Internal
{
    /// <summary>
    /// Build the <see cref="ISessionStore{K, AGG}"/> of a session windowed aggregation from its <see cref="Materialized{K, V, S}"/>.
    /// </summary>
    internal class SessionStoreMaterializer<K, V>
    {
        private readonly long inactivityGapMs;
        private readonly long gracePeriodMs;
        private readonly Materialized<K, V, ISessionStore<Bytes, byte[]>> materializedInternal;

        /// <param name="inactivityGapMs">inactivity gap of the session windows</param>
        /// <param name="gracePeriodMs">grace period of the session windows</param>
        /// <param name="materializedInternal">materialization of the store</param>
        public SessionStoreMaterializer(long inactivityGapMs, long gracePeriodMs, Materialized<K, V, ISessionStore<Bytes, byte[]>> materializedInternal)
        {
            this.inactivityGapMs = inactivityGapMs;
            this.gracePeriodMs = gracePeriodMs;
            this.materializedInternal = materializedInternal;
        }

        public IStoreBuilder<ISessionStore<K, V>> Materialize()
        {
            var builder = Stores.SessionStoreBuilder(
                GetSessionSupplier(),
                materializedInternal.KeySerdes,
                materializedInternal.ValueSerdes);

            if (materializedInternal.LoggingEnabled)
                builder.WithLoggingEnabled(materializedInternal.TopicConfig);
            else
                builder.WithLoggingDisabled();

            if (materializedInternal.CachingEnabled)
                builder.WithCachingEnabled(materializedInternal.CacheSize);
            else
                builder.WithCachingDisabled();

            if (materializedInternal.ExplicitlySetted)
                builder.CacheExplicitlySet();

            return builder;
        }

        internal ISessionBytesStoreSupplier GetSessionSupplier()
        {
            var supplier = (ISessionBytesStoreSupplier) materializedInternal.StoreSupplier;
            // the retention must keep a session until it can't be merged anymore (inactivity gap) and until the grace period ends
            long retention = supplier?.Retention ?? (long) materializedInternal.Retention.TotalMilliseconds;

            if (inactivityGapMs + gracePeriodMs > retention)
                throw new ArgumentException(
                    $"The retention period of the session store {materializedInternal.StoreName} must be no smaller than the session inactivity gap plus the grace period. Got gap=[{inactivityGapMs}], grace=[{gracePeriodMs}], retention=[{retention}].");

            return supplier ?? Stores.DefaultSessionStore(materializedInternal.StoreName, TimeSpan.FromMilliseconds(retention));
        }
    }
}
