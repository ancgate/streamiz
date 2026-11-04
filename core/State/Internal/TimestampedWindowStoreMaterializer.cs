using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.State.Supplier;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;
using System;

namespace Streamiz.Kafka.Net.State.Internal
{
    internal class TimestampedWindowStoreMaterializer<K, V, W> : 
        WindowStoreMaterializer<K, V, W, ITimestampedWindowStore<K, V>>
        where W : Window
    {
        public TimestampedWindowStoreMaterializer(WindowOptions<W> windowsOptions, Materialized<K, V, IWindowStore<Bytes, byte[]>> materializedInternal)
            : base(windowsOptions, materializedInternal)
        {
            
        }

        /// <summary>
        /// Sliding windows: the windows are as large as the time difference, and the store must keep two time
        /// differences (a record's left window and the right window of the record before it) plus the grace period.
        /// </summary>
        public TimestampedWindowStoreMaterializer(SlidingWindowOptions slidingWindows, Materialized<K, V, IWindowStore<Bytes, byte[]>> materializedInternal)
            : base(materializedInternal,
                slidingWindows.TimeDifferenceMs,
                2 * slidingWindows.TimeDifferenceMs + slidingWindows.GracePeriodMs,
                $"2 * time difference plus the grace period. Got time difference=[{slidingWindows.TimeDifferenceMs}], grace=[{slidingWindows.GracePeriodMs}]")
        {
        }

        protected override IStoreBuilder<ITimestampedWindowStore<K, V>> GetWindowStoreBuilder(IWindowBytesStoreSupplier supplier)
            => Stores.TimestampedWindowStoreBuilder(supplier, materializedInternal.KeySerdes, materializedInternal.ValueSerdes);
   }
}
