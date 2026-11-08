using System.Collections.Generic;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.State.Internal;
using Streamiz.Kafka.Net.Stream.Internal.Graph;
using Streamiz.Kafka.Net.Stream.Internal.Graph.Nodes;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Kafka.Net.Stream.Internal
{
    /// <summary>
    /// Windowed aggregations with sliding windows (KIP-450): same API as the other time windows,
    /// with a sliding windows aggregate processor and store.
    /// </summary>
    internal class SlidingWindowedKStream<K, V> : TimeWindowedKStream<K, V, TimeWindow>
    {
        private readonly SlidingWindowOptions slidingWindows;

        public SlidingWindowedKStream(SlidingWindowOptions slidingWindows, GroupedStreamAggregateBuilder<K, V> aggBuilder, string name, ISerDes<K> keySerde, ISerDes<V> valSerde, List<string> sourceNodes, StreamGraphNode streamsGraphNode, InternalStreamBuilder builder)
            : base(aggBuilder, name, keySerde, valSerde, sourceNodes, streamsGraphNode, builder)
        {
            CheckIfParamNull(slidingWindows, "slidingWindows");
            this.slidingWindows = slidingWindows;
        }

        // windows are [start, start + time difference]
        protected override long WindowSize => slidingWindows.TimeDifferenceMs;

        protected override IKStreamAggProcessorSupplier<K, Windowed<K>, V, VR> AggregateSupplier<VR>(string storeName, Initializer<VR> initializer, Aggregator<K, V, VR> aggregator)
            => new KStreamSlidingWindowAggregate<K, V, VR>(slidingWindows, storeName, initializer, aggregator);

        protected override IStoreBuilder<ITimestampedWindowStore<K, VR>> Materialize<VR>(Materialized<K, VR, IWindowStore<Bytes, byte[]>> materialized)
            => new TimestampedWindowStoreMaterializer<K, VR, TimeWindow>(slidingWindows, materialized).Materialize();
    }
}
