using System;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.State.Supplier;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Kafka.Net.State.Internal
{
    internal class WindowStoreMaterializer<K, V, W, WS>
        where WS : IStateStore
        where W : Window
    {
        protected readonly Materialized<K, V, IWindowStore<Bytes, byte[]>> materializedInternal;
        private readonly long windowSizeMs;
        private readonly long minimumRetentionMs;
        private readonly string minimumRetentionDescription;

        public WindowStoreMaterializer(WindowOptions<W> windowsOptions, Materialized<K, V, IWindowStore<Bytes, byte[]>> materializedInternal)
            : this(materializedInternal,
                windowsOptions.Size,
                windowsOptions.Size + windowsOptions.GracePeriodMs,
                $"its window size plus the grace period. Got size=[{windowsOptions.Size}], grace=[{windowsOptions.GracePeriodMs}]")
        {
        }

        /// <param name="materializedInternal">materialization of the store</param>
        /// <param name="windowSizeMs">size of the windows</param>
        /// <param name="minimumRetentionMs">the retention of the store can't be smaller</param>
        /// <param name="minimumRetentionDescription">how the minimum retention is computed, for the error message</param>
        protected WindowStoreMaterializer(Materialized<K, V, IWindowStore<Bytes, byte[]>> materializedInternal,
            long windowSizeMs,
            long minimumRetentionMs,
            string minimumRetentionDescription)
        {
            this.materializedInternal = materializedInternal;
            this.windowSizeMs = windowSizeMs;
            this.minimumRetentionMs = minimumRetentionMs;
            this.minimumRetentionDescription = minimumRetentionDescription;
        }

        public IStoreBuilder<WS> Materialize()
        {
            var supplier = GetWindowSupplier();
            var builder = GetWindowStoreBuilder(supplier);

            if (materializedInternal.LoggingEnabled)
                builder.WithLoggingEnabled(materializedInternal.TopicConfig);
            else
                builder.WithLoggingDisabled();

            if (materializedInternal.CachingEnabled)
                builder.WithCachingEnabled(materializedInternal.CacheSize);
            else
                builder.WithCachingDisabled();
            
            if(materializedInternal.ExplicitlySetted)
                builder.CacheExplicitlySet();

            return builder;
        }

        protected IWindowBytesStoreSupplier GetWindowSupplier()
        {
            IWindowBytesStoreSupplier supplier = (IWindowBytesStoreSupplier)materializedInternal.StoreSupplier;
            if (supplier == null)
            {
                if (minimumRetentionMs > materializedInternal.Retention.TotalMilliseconds)
                    throw new ArgumentException($"The retention period of the window store { materializedInternal.StoreName } must be no smaller than {minimumRetentionDescription}, retention=[{materializedInternal.Retention.TotalMilliseconds}].");

                supplier = Stores.DefaultWindowStore(
                    materializedInternal.StoreName,
                    materializedInternal.Retention,
                    TimeSpan.FromMilliseconds(windowSizeMs));
            }
            else
                supplier.WindowSize = !supplier.WindowSize.HasValue ? windowSizeMs : supplier.WindowSize.Value;

            return supplier;
        }

        protected virtual IStoreBuilder<WS> GetWindowStoreBuilder(IWindowBytesStoreSupplier supplier)
        {
            return (IStoreBuilder<WS>)
                Stores.WindowStoreBuilder(supplier, materializedInternal.KeySerdes, materializedInternal.ValueSerdes);
        }
    }
}