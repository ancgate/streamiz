using System;
using Microsoft.Extensions.Logging;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.State.Internal;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table.Internal;

namespace Streamiz.Kafka.Net.Processors
{
    internal class KStreamKTableJoinProcessor<K1, K2, V1, V2, R> : AbstractProcessor<K1, V1>
    {
        private readonly IKTableValueGetter<K2, V2> valueGetter;
        private readonly IKeyValueMapper<K1, V1, K2> mapper;
        private readonly IValueJoiner<V1, V2, R> joiner;
        private readonly bool leftJoin;

        // KIP-923: stream records are buffered for the grace period, then joined with the table value as of their timestamp
        private readonly long gracePeriodMs;
        private readonly string bufferStoreName;
        private readonly ISerDes<K1> bufferKeySerdes;
        private readonly ISerDes<V1> bufferValueSerdes;
        private InMemoryTimeOrderedKeyValueBuffer<K1, V1> buffer;
        private IKTableVersionedValueGetter<K2, V2> versionedValueGetter;
        private long observedStreamTime = -1;

        public KStreamKTableJoinProcessor(IKTableValueGetter<K2, V2> valueGetter, IKeyValueMapper<K1, V1, K2> keyValueMapper, IValueJoiner<V1, V2, R> valueJoiner, bool leftJoin)
            : this(valueGetter, keyValueMapper, valueJoiner, leftJoin, 0, null, null, null)
        {
        }

        public KStreamKTableJoinProcessor(IKTableValueGetter<K2, V2> valueGetter, IKeyValueMapper<K1, V1, K2> keyValueMapper, IValueJoiner<V1, V2, R> valueJoiner, bool leftJoin,
            long gracePeriodMs, string bufferStoreName, ISerDes<K1> bufferKeySerdes, ISerDes<V1> bufferValueSerdes)
        {
            this.valueGetter = valueGetter;
            mapper = keyValueMapper;
            joiner = valueJoiner;
            this.leftJoin = leftJoin;
            this.gracePeriodMs = gracePeriodMs;
            this.bufferStoreName = bufferStoreName;
            this.bufferKeySerdes = bufferKeySerdes;
            this.bufferValueSerdes = bufferValueSerdes;
        }

        public override void Init(ProcessorContext context)
        {
            base.Init(context);
            valueGetter.Init(context);

            // KIP-914: with a versioned table, the stream record is joined with the table value as of its timestamp
            if (valueGetter is IKTableVersionedValueGetter<K2, V2> {IsVersioned: true} versioned)
                versionedValueGetter = versioned;

            if (bufferStoreName != null)
            {
                if (versionedValueGetter == null)
                    throw new StreamsException($"{logPrefix}The table must be versioned to use a grace period in a stream-table join");

                buffer = (InMemoryTimeOrderedKeyValueBuffer<K1, V1>) context.GetStateStore(bufferStoreName);
                buffer.SetSerdesIfNull(bufferKeySerdes, bufferValueSerdes);
            }
        }

        public override void Process(K1 key, V1 value)
        {
            // If the key or value is null we don't need to proceed
            if (key == null || value == null)
            {
                log.LogWarning($"Skipping record due to null key or value. key=[{key}] value=[{value}] topic=[{Context.Topic}] partition=[{Context.Partition}] offset=[{Context.Offset}]");
                droppedRecordsSensor.Record();
                return;
            }

            if (buffer == null)
            {
                DoJoin(key, value);
                return;
            }

            observedStreamTime = Math.Max(observedStreamTime, Context.Timestamp);
            long deadline = observedStreamTime - gracePeriodMs;

            // a record older than the grace period can't wait: it is joined right away
            if (Context.Timestamp < deadline)
                DoJoin(key, value);
            else
                buffer.Put(key, value, Context.RecordContext);

            buffer.EvictUntil(deadline, Emit);
        }

        private void Emit(K1 key, V1 value, IRecordContext recordContext)
        {
            var currentRecordContext = Context.RecordContext;
            try
            {
                Context.SetRecordMetaData(recordContext);
                DoJoin(key, value);
            }
            finally
            {
                Context.SetRecordMetaData(currentRecordContext);
            }
        }

        private void DoJoin(K1 key, V1 value)
        {
            K2 mappedKey = mapper.Apply(key, value, Context.RecordContext);
            V2 value2 = default;
            if (mappedKey != null)
            {
                value2 = ValueAndTimestamp.GetValueOrNull(versionedValueGetter != null
                    ? versionedValueGetter.Get(mappedKey, Context.Timestamp)
                    : valueGetter.Get(mappedKey));
            }

            if (leftJoin || value2 != null)
                Forward(key, joiner.Apply(value, value2));
        }
    }
}
