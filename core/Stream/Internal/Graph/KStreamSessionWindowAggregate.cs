using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Table.Internal;

namespace Streamiz.Kafka.Net.Stream.Internal.Graph
{
    internal class KStreamSessionWindowAggregate<K, V, Agg> : IKStreamAggProcessorSupplier<K, Windowed<K>, V, Agg>
    {
        private readonly string storeName;
        private readonly Initializer<Agg> initializer;
        private readonly Aggregator<K, V, Agg> aggregator;
        private readonly Merger<K, Agg> sessionMerger;

        private bool sendOldValues = false;

        public KStreamSessionWindowAggregate(SessionWindowOptions windows, string storeName, Initializer<Agg> initializer, Aggregator<K, V, Agg> aggregator, Merger<K, Agg> sessionMerger)
        {
            Windows = windows;
            this.storeName = storeName;
            this.initializer = initializer;
            this.aggregator = aggregator;
            this.sessionMerger = sessionMerger;
        }

        public SessionWindowOptions Windows { get; }

        public void EnableSendingOldValues()
        {
            sendOldValues = true;
        }

        public IProcessor<K, V> Get()
            => new KStreamSessionWindowAggregateProcessor<K, V, Agg>(Windows, storeName, initializer, aggregator, sessionMerger, sendOldValues);

        public IKTableValueGetterSupplier<Windowed<K>, Agg> View()
            => new GenericKTableValueGetterSupplier<Windowed<K>, Agg>(
                new string[] { storeName },
                new SessionKeyValueStoreGetter<K, Agg>(storeName));
    }
}
