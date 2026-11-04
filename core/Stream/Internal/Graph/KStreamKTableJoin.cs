using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Table.Internal;

namespace Streamiz.Kafka.Net.Stream.Internal.Graph
{
    internal class KStreamKTableJoin<K, R, V1, V2> : IProcessorSupplier<K, V1>
    {
        public class KeyValueMapper : IKeyValueMapper<K, V1, K>
        {
            public K Apply(K key, V1 value, IRecordContext context) => key;
        }

        private readonly IKTableValueGetterSupplier<K, V2> valueGetter;
        private readonly IValueJoiner<V1, V2, R> valueJoiner;
        private readonly bool leftJoin;
        private readonly long gracePeriodMs;
        private readonly string bufferStoreName;
        private readonly ISerDes<K> keySerdes;
        private readonly ISerDes<V1> valueSerdes;

        public KStreamKTableJoin(
            IKTableValueGetterSupplier<K, V2> valueGetter,
            IValueJoiner<V1, V2, R> valueJoiner,
            bool leftJoin)
            : this(valueGetter, valueJoiner, leftJoin, 0, null, null, null)
        {
        }

        /// <param name="gracePeriodMs">KIP-923: how long stream records are buffered before being joined</param>
        /// <param name="bufferStoreName">name of the buffer store, null without grace period</param>
        public KStreamKTableJoin(
            IKTableValueGetterSupplier<K, V2> valueGetter,
            IValueJoiner<V1, V2, R> valueJoiner,
            bool leftJoin,
            long gracePeriodMs,
            string bufferStoreName,
            ISerDes<K> keySerdes,
            ISerDes<V1> valueSerdes)
        {
            this.valueGetter = valueGetter;
            this.valueJoiner = valueJoiner;
            this.leftJoin = leftJoin;
            this.gracePeriodMs = gracePeriodMs;
            this.bufferStoreName = bufferStoreName;
            this.keySerdes = keySerdes;
            this.valueSerdes = valueSerdes;
        }

        public IProcessor<K, V1> Get()
            => new KStreamKTableJoinProcessor<K, K, V1, V2, R>(valueGetter.Get(), new KeyValueMapper(), valueJoiner, leftJoin,
                gracePeriodMs, bufferStoreName, keySerdes, valueSerdes);
    }
}
