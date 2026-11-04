using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Table.Internal;

namespace Streamiz.Kafka.Net.Stream.Internal.Graph
{
    internal class KStreamSlidingWindowAggregate<K, V, Agg> : IKStreamAggProcessorSupplier<K, Windowed<K>, V, Agg>
    {
        private readonly SlidingWindowOptions windows;
        private readonly string storeName;
        private readonly Initializer<Agg> initializer;
        private readonly Aggregator<K, V, Agg> aggregator;

        private bool sendOldValues = false;

        public KStreamSlidingWindowAggregate(SlidingWindowOptions windows, string storeName, Initializer<Agg> initializer, Aggregator<K, V, Agg> aggregator)
        {
            this.windows = windows;
            this.storeName = storeName;
            this.initializer = initializer;
            this.aggregator = aggregator;
        }

        public void EnableSendingOldValues()
        {
            sendOldValues = true;
        }

        public IProcessor<K, V> Get()
            => new KStreamSlidingWindowAggregateProcessor<K, V, Agg>(windows, initializer, aggregator, storeName, sendOldValues);

        // windows are stored by their start, like time windows
        public IKTableValueGetterSupplier<Windowed<K>, Agg> View()
            => new GenericKTableValueGetterSupplier<Windowed<K>, Agg>(
                new string[] { storeName },
                new WindowKeyValueStoreGetter<K, Agg>(storeName));
    }
}
