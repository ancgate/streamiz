using Microsoft.Extensions.Logging;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Table.Internal;

namespace Streamiz.Kafka.Net.Processors
{
    internal class KTableSourceProcessor<K, V> : AbstractProcessor<K, V>
    {
        private readonly string storeName;
        private readonly string queryableName;
        private readonly bool sendOldValues;

        private KeyValueStoreWrapper<K, V> store;
        private TimestampedTupleForwarder<K, V> tupleForwarder;

        public KTableSourceProcessor(string storeName, string queryableName, bool sendOldValues)
        {
            this.storeName = storeName;
            this.queryableName = queryableName;
            this.sendOldValues = sendOldValues;
        }

        public override void Init(ProcessorContext context)
        {
            base.Init(context);

            if (this.queryableName != null)
            {
                store = new KeyValueStoreWrapper<K, V>(context, queryableName);
                tupleForwarder = new TimestampedTupleForwarder<K, V>(
                    store.Store,
                    this,
                    kv => {
                        context.CurrentProcessor = this;
                        context.CurrentProcessor.Forward(kv.Key,
                            new Change<V>(sendOldValues ? (kv.Value.OldValue != null ? kv.Value.OldValue.Value : default) : default, kv.Value.NewValue.Value),
                            kv.Value.NewValue.Timestamp);
                    },
                    sendOldValues);
            }
        }

        public override void Process(K key, V value)
        {
            LogProcessingKeyValue(key, value);
            if (key == null)
            {
                log.LogWarning($"Skipping record due to null key. topic=[{Context.Topic}] partition=[{Context.Partition}] offset=[{Context.Offset}]");
                droppedRecordsSensor.Record();
                return;
            }

            if (queryableName != null)
            {
                 ValueAndTimestamp<V> oldValueAndTimestamp = store.Get(key);
                 V oldValue;
                if (oldValueAndTimestamp != null)
                {
                    oldValue = oldValueAndTimestamp.Value;
                    if (Context.Timestamp < oldValueAndTimestamp.Timestamp)
                    {
                        log.LogWarning($"Detected out-of-order KTable update for {store.Name} at offset {Context.Offset}, partition {Context.Partition}.");
                    }
                }
                else
                {
                    oldValue = default(V);
                }

                long putReturnCode = store.Put(key, value, Context.Timestamp);

                // versioned store (KIP-914): a record which was not put (older than the history retention), or which is not
                // the latest value of its key (out-of-order record, written in the history), doesn't change the table
                if (putReturnCode != KeyValueStoreWrapper<K, V>.PUT_RETURN_CODE_IS_LATEST)
                {
                    log.LogDebug(
                        "{LogPrefix}Record (topic=[{Topic}] partition=[{Partition}] offset=[{Offset}]) is not the latest value of its key in the versioned store {Store}, it is not forwarded",
                        logPrefix, Context.Topic, Context.Partition, Context.Offset, store.Name);
                    return;
                }

                tupleForwarder.MaybeForward(key, value, oldValue);
            }
            else
            {
                this.Forward<K, Change<V>>(key, new Change<V>(default(V), value));
            }
        }
    }
}
