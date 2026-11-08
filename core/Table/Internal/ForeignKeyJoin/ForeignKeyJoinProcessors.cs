using System;
using System.Linq;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Stream;

namespace Streamiz.Kafka.Net.Table.Internal.ForeignKeyJoin
{
    // Foreign-key join (KIP-213), ported from Kafka Streams:
    // left table --SubscriptionSend--> registration topic (key: FK) --SubscriptionReceive--> response topic (key: PK)
    // right table --ForeignTableJoin--> response topic
    // response topic --ResponseJoin--> result table

    /// <summary>
    /// Left side: for each change of the left table, sends a subscription to the foreign key (and unsubscribes the old one).
    /// Kafka Streams: ForeignJoinSubscriptionSendProcessorSupplier.
    /// </summary>
    internal class SubscriptionSendProcessor<K, V, KO> : AbstractProcessor<K, Change<V>>
    {
        private readonly Func<V, KO> foreignKeyExtractor;
        private readonly bool leftJoin;
        private readonly string foreignKeySerdesTopic;
        private readonly string valueSerdesTopic;
        private ISerDes<KO> foreignKeySerdes;
        private ISerDes<V> valueSerdes;
        private long[] recordHash;
        private bool recordHashComputed;

        public SubscriptionSendProcessor(Func<V, KO> foreignKeyExtractor, bool leftJoin,
            string foreignKeySerdesTopic, ISerDes<KO> foreignKeySerdes,
            string valueSerdesTopic, ISerDes<V> valueSerdes)
        {
            this.foreignKeyExtractor = foreignKeyExtractor;
            this.leftJoin = leftJoin;
            this.foreignKeySerdesTopic = foreignKeySerdesTopic;
            this.foreignKeySerdes = foreignKeySerdes;
            this.valueSerdesTopic = valueSerdesTopic;
            this.valueSerdes = valueSerdes;
        }

        public override void Init(ProcessorContext context)
        {
            base.Init(context);
            foreignKeySerdes = ForeignKeyJoinSerDesHelper.ResolveKeySerDes(foreignKeySerdes, context.SerDesContext, "foreign key");
            valueSerdes = ForeignKeyJoinSerDesHelper.ResolveValueSerDes(valueSerdes, context.SerDesContext, "left value");
        }

        public override void Process(K key, Change<V> change)
        {
            // KIP-914: an out-of-order record of a versioned table is not joined
            if (!change.IsLatest)
            {
                log.LogDebug(
                    "{LogPrefix}Skipping out-of-order record from versioned table while performing table-table join. topic=[{Topic}] partition=[{Partition}] offset=[{Offset}]",
                    logPrefix, Context.Topic, Context.Partition, Context.Offset);
                droppedRecordsSensor.Record();
                return;
            }

            // clear the hash of the previous record
            recordHash = null;
            recordHashComputed = false;

            if (leftJoin)
                LeftJoinInstructions(key, change);
            else
                DefaultJoinInstructions(key, change);
        }

        private void LeftJoinInstructions(K key, Change<V> change)
        {
            if (change.OldValue != null)
            {
                KO oldForeignKey = foreignKeyExtractor(change.OldValue);
                KO newForeignKey = change.NewValue == null ? default : foreignKeyExtractor(change.NewValue);
                if (oldForeignKey != null && !SameForeignKey(oldForeignKey, newForeignKey))
                    Send(key, change, oldForeignKey, SubscriptionInstruction.DELETE_KEY_NO_PROPAGATE);
                // a null foreign key is sent too: the left join emits joiner(value, null)
                Send(key, change, newForeignKey, SubscriptionInstruction.PROPAGATE_NULL_IF_NO_FK_VAL_AVAILABLE);
            }
            else if (change.NewValue != null)
            {
                KO newForeignKey = foreignKeyExtractor(change.NewValue);
                Send(key, change, newForeignKey, SubscriptionInstruction.PROPAGATE_NULL_IF_NO_FK_VAL_AVAILABLE);
            }
        }

        private void DefaultJoinInstructions(K key, Change<V> change)
        {
            if (change.OldValue != null)
            {
                KO oldForeignKey = foreignKeyExtractor(change.OldValue);
                if (oldForeignKey == null)
                {
                    LogSkippedRecordDueToNullForeignKey();
                    return;
                }

                if (change.NewValue != null)
                {
                    KO newForeignKey = foreignKeyExtractor(change.NewValue);
                    if (newForeignKey == null)
                    {
                        LogSkippedRecordDueToNullForeignKey();
                        return;
                    }

                    // different foreign key: remove the old subscription
                    if (!SameForeignKey(oldForeignKey, newForeignKey))
                        Send(key, change, oldForeignKey, SubscriptionInstruction.DELETE_KEY_NO_PROPAGATE);

                    // subscribe to the new foreign key, and propagate null if it has no value: the result of the
                    // previous foreign key must be "unset", for both inner and left joins
                    Send(key, change, newForeignKey, SubscriptionInstruction.PROPAGATE_NULL_IF_NO_FK_VAL_AVAILABLE);
                }
                else
                {
                    Send(key, change, oldForeignKey, SubscriptionInstruction.DELETE_KEY_AND_PROPAGATE);
                }
            }
            else if (change.NewValue != null)
            {
                KO newForeignKey = foreignKeyExtractor(change.NewValue);
                if (newForeignKey == null)
                    LogSkippedRecordDueToNullForeignKey();
                else
                    Send(key, change, newForeignKey, SubscriptionInstruction.PROPAGATE_ONLY_IF_FK_VAL_AVAILABLE);
            }
        }

        private bool SameForeignKey(KO foreignKey1, KO foreignKey2)
        {
            var bytes1 = SerializeForeignKey(foreignKey1);
            var bytes2 = SerializeForeignKey(foreignKey2);
            return bytes1 == null ? bytes2 == null : bytes2 != null && bytes1.SequenceEqual(bytes2);
        }

        private byte[] SerializeForeignKey(KO foreignKey)
            => foreignKey == null
                ? null
                : foreignKeySerdes.Serialize(foreignKey, new SerializationContext(MessageComponentType.Key, foreignKeySerdesTopic));

        private void Send(K key, Change<V> change, KO foreignKey, SubscriptionInstruction instruction)
        {
            var wrapper = new SubscriptionWrapper<K>(Hash(change), instruction, key, Context.Partition);
            Forward(foreignKey, wrapper);
        }

        private long[] Hash(Change<V> change)
        {
            if (!recordHashComputed)
            {
                recordHash = change.NewValue == null
                    ? null
                    : Murmur3.Hash128(valueSerdes.Serialize(change.NewValue,
                        new SerializationContext(MessageComponentType.Value, valueSerdesTopic)));
                recordHashComputed = true;
            }

            return recordHash;
        }

        private void LogSkippedRecordDueToNullForeignKey()
        {
            log.LogWarning(
                "{LogPrefix}Skipping record due to null foreign key. topic=[{Topic}] partition=[{Partition}] offset=[{Offset}]",
                logPrefix, Context.Topic, Context.Partition, Context.Offset);
            droppedRecordsSensor.Record();
        }
    }

    /// <summary>
    /// Right side, for each subscription: updates the subscription store, then looks up the foreign value and sends the
    /// response to the primary key. Kafka Streams: SubscriptionReceiveProcessorSupplier + SubscriptionJoinProcessorSupplier.
    /// </summary>
    internal class SubscriptionReceiveProcessor<KO, VO, K> : AbstractProcessor<KO, SubscriptionWrapper<K>>
    {
        private readonly string storeName;
        private readonly CombinedKeySchema<KO, K> keySchema;
        private readonly IKTableValueGetter<KO, VO> foreignValueGetter;
        private ITimestampedKeyValueStore<Bytes, SubscriptionWrapper<K>> store;

        public SubscriptionReceiveProcessor(string storeName, CombinedKeySchema<KO, K> keySchema,
            IKTableValueGetter<KO, VO> foreignValueGetter)
        {
            this.storeName = storeName;
            this.keySchema = keySchema;
            this.foreignValueGetter = foreignValueGetter;
        }

        public override void Init(ProcessorContext context)
        {
            base.Init(context);
            keySchema.Initialize(context.SerDesContext);
            store = (ITimestampedKeyValueStore<Bytes, SubscriptionWrapper<K>>) context.GetStateStore(storeName);
            foreignValueGetter.Init(context);
        }

        public override void Close()
        {
            base.Close();
            foreignValueGetter.Close();
        }

        public override void Process(KO foreignKey, SubscriptionWrapper<K> subscription)
        {
            // a null foreign key is only expected from a left join, which emits joiner(value, null)
            if (foreignKey == null &&
                subscription.Instruction != SubscriptionInstruction.PROPAGATE_NULL_IF_NO_FK_VAL_AVAILABLE)
            {
                log.LogWarning(
                    "{LogPrefix}Skipping record due to null foreign key. topic=[{Topic}] partition=[{Partition}] offset=[{Offset}]",
                    logPrefix, Context.Topic, Context.Partition, Context.Offset);
                droppedRecordsSensor.Record();
                return;
            }

            // the subscription store is used by the prefix scan of ForeignTableJoinProcessor
            if (foreignKey != null)
            {
                var subscriptionKey = keySchema.ToBytes(foreignKey, subscription.PrimaryKey);
                if (subscription.Instruction == SubscriptionInstruction.DELETE_KEY_AND_PROPAGATE ||
                    subscription.Instruction == SubscriptionInstruction.DELETE_KEY_NO_PROPAGATE)
                    store.Delete(subscriptionKey);
                else
                    store.Put(subscriptionKey, ValueAndTimestamp<SubscriptionWrapper<K>>.Make(subscription, Context.Timestamp));
            }

            var foreignValueAndTime = foreignKey == null ? null : foreignValueGetter.Get(foreignKey);
            long resultTimestamp = foreignValueAndTime == null
                ? Context.Timestamp
                : Math.Max(Context.Timestamp, foreignValueAndTime.Timestamp);

            switch (subscription.Instruction)
            {
                case SubscriptionInstruction.DELETE_KEY_AND_PROPAGATE:
                    SendResponse(subscription, default, resultTimestamp);
                    break;
                case SubscriptionInstruction.PROPAGATE_NULL_IF_NO_FK_VAL_AVAILABLE:
                    // sent for both inner and left joins: the foreign key changed and the previous result must be "unset"
                    SendResponse(subscription, foreignValueAndTime == null ? default : foreignValueAndTime.Value,
                        resultTimestamp);
                    break;
                case SubscriptionInstruction.PROPAGATE_ONLY_IF_FK_VAL_AVAILABLE:
                    if (foreignValueAndTime != null)
                        SendResponse(subscription, foreignValueAndTime.Value, resultTimestamp);
                    break;
                case SubscriptionInstruction.DELETE_KEY_NO_PROPAGATE:
                    break;
                default:
                    throw new InvalidOperationException($"Unhandled subscription instruction: {subscription.Instruction}");
            }
        }

        private void SendResponse(SubscriptionWrapper<K> subscription, VO foreignValue, long timestamp)
            => Forward(subscription.PrimaryKey,
                new SubscriptionResponseWrapper<VO>(subscription.Hash, foreignValue, subscription.PrimaryPartition),
                timestamp);
    }

    /// <summary>
    /// Right side, for each change of the right table: sends a response to every primary key subscribed to this foreign key.
    /// Kafka Streams: ForeignTableJoinProcessorSupplier.
    /// </summary>
    internal class ForeignTableJoinProcessor<KO, VO, K> : AbstractProcessor<KO, Change<VO>>
    {
        private readonly string storeName;
        private readonly CombinedKeySchema<KO, K> keySchema;
        private ITimestampedKeyValueStore<Bytes, SubscriptionWrapper<K>> store;

        public ForeignTableJoinProcessor(string storeName, CombinedKeySchema<KO, K> keySchema)
        {
            this.storeName = storeName;
            this.keySchema = keySchema;
        }

        public override void Init(ProcessorContext context)
        {
            base.Init(context);
            keySchema.Initialize(context.SerDesContext);
            store = (ITimestampedKeyValueStore<Bytes, SubscriptionWrapper<K>>) context.GetStateStore(storeName);
        }

        public override void Process(KO foreignKey, Change<VO> change)
        {
            if (foreignKey == null)
            {
                log.LogWarning(
                    "{LogPrefix}Skipping record due to null key. topic=[{Topic}] partition=[{Partition}] offset=[{Offset}]",
                    logPrefix, Context.Topic, Context.Partition, Context.Offset);
                droppedRecordsSensor.Record();
                return;
            }

            // KIP-914: an out-of-order record of a versioned table is not joined
            if (!change.IsLatest)
            {
                log.LogDebug(
                    "{LogPrefix}Skipping out-of-order record from versioned table while performing table-table join. topic=[{Topic}] partition=[{Partition}] offset=[{Offset}]",
                    logPrefix, Context.Topic, Context.Partition, Context.Offset);
                droppedRecordsSensor.Record();
                return;
            }

            long timestamp = Context.Timestamp;
            var prefix = keySchema.PrefixBytes(foreignKey);

            // the subscriptions are collected first: forwarding can't happen while the store is being read
            var subscriptions = PrefixScan(prefix).ToList();
            foreach (var subscription in subscriptions)
            {
                Forward(subscription.Value.PrimaryKey,
                    new SubscriptionResponseWrapper<VO>(subscription.Value.Hash, change.NewValue,
                        subscription.Value.PrimaryPartition),
                    Math.Max(timestamp, subscription.Timestamp));
            }
        }

        // all the keys starting with the prefix are in [prefix, prefix upper bound]: the range end is inclusive, so the
        // prefix is checked again (Kafka Streams does the same with Bytes.increment)
        private System.Collections.Generic.IEnumerable<ValueAndTimestamp<SubscriptionWrapper<K>>> PrefixScan(Bytes prefix)
        {
            var upperBound = CombinedKeySchema<KO, K>.PrefixUpperBound(prefix);
            if (upperBound == null)
            {
                foreach (var kv in store.All())
                    if (CombinedKeySchema<KO, K>.StartsWith(kv.Key.Get, prefix.Get) && kv.Value?.Value != null)
                        yield return kv.Value;
                yield break;
            }

            using var enumerator = store.Range(prefix, upperBound);
            while (enumerator.MoveNext())
            {
                var next = enumerator.Current;
                if (next.HasValue && CombinedKeySchema<KO, K>.StartsWith(next.Value.Key.Get, prefix.Get) &&
                    next.Value.Value?.Value != null)
                    yield return next.Value.Value;
            }
        }
    }

    /// <summary>
    /// Left side, for each response: joins the current left value with the foreign value, unless the left value changed
    /// since the subscription was sent (stale response). Kafka Streams: ResponseJoinProcessorSupplier.
    /// </summary>
    internal class ResponseJoinProcessor<K, V, VO, VR> : AbstractProcessor<K, SubscriptionResponseWrapper<VO>>
    {
        private readonly IKTableValueGetter<K, V> valueGetter;
        private readonly IValueJoiner<V, VO, VR> joiner;
        private readonly bool leftJoin;
        private readonly string valueSerdesTopic;
        private ISerDes<V> valueSerdes;

        public ResponseJoinProcessor(IKTableValueGetter<K, V> valueGetter, IValueJoiner<V, VO, VR> joiner, bool leftJoin,
            string valueSerdesTopic, ISerDes<V> valueSerdes)
        {
            this.valueGetter = valueGetter;
            this.joiner = joiner;
            this.leftJoin = leftJoin;
            this.valueSerdesTopic = valueSerdesTopic;
            this.valueSerdes = valueSerdes;
        }

        public override void Init(ProcessorContext context)
        {
            base.Init(context);
            valueSerdes = ForeignKeyJoinSerDesHelper.ResolveValueSerDes(valueSerdes, context.SerDesContext, "left value");
            valueGetter.Init(context);
        }

        public override void Close()
        {
            base.Close();
            valueGetter.Close();
        }

        public override void Process(K key, SubscriptionResponseWrapper<VO> response)
        {
            var currentValueWithTimestamp = valueGetter.Get(key);
            var currentValue = currentValueWithTimestamp == null ? default : currentValueWithTimestamp.Value;
            long[] currentHash = currentValueWithTimestamp == null || currentValue == null
                ? null
                : Murmur3.Hash128(valueSerdes.Serialize(currentValue,
                    new SerializationContext(MessageComponentType.Value, valueSerdesTopic)));

            // the left value changed since the subscription was sent: the response is stale
            if (!SubscriptionWrapper<K>.HashEquals(response.OriginalValueHash, currentHash))
            {
                log.LogDebug(
                    "{LogPrefix}Skipping stale foreign-key join response. topic=[{Topic}] partition=[{Partition}] offset=[{Offset}]",
                    logPrefix, Context.Topic, Context.Partition, Context.Offset);
                droppedRecordsSensor.Record();
                return;
            }

            VR result;
            if (response.ForeignValue == null && (!leftJoin || currentValue == null))
                result = default; // tombstone
            else
                result = joiner.Apply(currentValue, response.ForeignValue);

            Forward(key, result);
        }
    }

    /// <summary>
    /// Sends the response to the partition of the left record which sent the subscription.
    /// </summary>
    internal class SubscriptionResponsePartitioner<K, VO> : IStreamPartitioner<K, SubscriptionResponseWrapper<VO>>
    {
        public void Initialize(IStreamConfig config)
        {
        }

        public Partition Partition(string topic, K key, SubscriptionResponseWrapper<VO> value, Partition sourcePartition,
            int numPartitions)
            => value.PrimaryPartition >= 0 && value.PrimaryPartition < numPartitions
                ? new Partition(value.PrimaryPartition)
                : Confluent.Kafka.Partition.Any;
    }

    internal class WrappedProcessorSupplier<K, V> : IProcessorSupplier<K, V>
    {
        private readonly Func<IProcessor<K, V>> supplier;

        public WrappedProcessorSupplier(Func<IProcessor<K, V>> supplier)
        {
            this.supplier = supplier;
        }

        public IProcessor<K, V> Get() => supplier();
    }
}
