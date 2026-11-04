using System.Threading;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Metrics;
using Streamiz.Kafka.Net.Metrics.Internal;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State.Internal;
using static Streamiz.Kafka.Net.Crosscutting.ActionHelper;

namespace Streamiz.Kafka.Net.State.Metered
{
    /// <summary>
    /// Typed (key and value serdes) and metered versioned store.
    /// </summary>
    internal class MeteredVersionedKeyValueStore<K, V> :
        WrappedStateStore<IVersionedKeyValueStore<Bytes, byte[]>>,
        IVersionedKeyValueStore<K, V>,
        IRestoreSensorStore
    {
        private ISerDes<K> keySerdes;
        private ISerDes<V> valueSerdes;
        private readonly string metricScope;
        private bool initStoreSerdes;

        private Sensor putSensor = NoRunnableSensor.Empty;
        private Sensor getSensor = NoRunnableSensor.Empty;
        private Sensor deleteSensor = NoRunnableSensor.Empty;
        private Sensor flushSensor = NoRunnableSensor.Empty;

        public MeteredVersionedKeyValueStore(
            IVersionedKeyValueStore<Bytes, byte[]> wrapped,
            ISerDes<K> keySerdes,
            ISerDes<V> valueSerdes,
            string metricScope)
            : base(wrapped)
        {
            this.keySerdes = keySerdes;
            this.valueSerdes = valueSerdes;
            this.metricScope = metricScope;
        }

        public override bool IsCachedStore => false;

        public Sensor RestoreSensor { get; private set; }

        public override void Init(ProcessorContext context, IStateStore root)
        {
            // before the inner stores are initialized: global stores are restored during their initialization
            RestoreSensor = StateStoreMetrics.RestoreSensor(context.Id, metricScope, Name, context.Metrics);
            base.Init(context, root);
            InitStoreSerde(context);

            putSensor = StateStoreMetrics.PutSensor(context.Id, metricScope, Name, context.Metrics);
            getSensor = StateStoreMetrics.GetSensor(context.Id, metricScope, Name, context.Metrics);
            deleteSensor = StateStoreMetrics.DeleteSensor(context.Id, metricScope, Name, context.Metrics);
            flushSensor = StateStoreMetrics.FlushSensor(context.Id, metricScope, Name, context.Metrics);
        }

        private void InitStoreSerde(ProcessorContext context)
        {
            if (initStoreSerdes)
                return;

            keySerdes ??= context.Configuration.DefaultKeySerDes as ISerDes<K>;
            valueSerdes ??= context.Configuration.DefaultValueSerDes as ISerDes<V>;
            keySerdes?.Initialize(new SerDesContext(context.Configuration));
            valueSerdes?.Initialize(new SerDesContext(context.Configuration));
            initStoreSerdes = true;
        }

        #region Serdes

        private Bytes KeyBytes(K key)
        {
            if (keySerdes != null)
                return Bytes.Wrap(keySerdes.Serialize(key, GetSerializationContext(true)));
            throw new StreamsException($"The serializer is not compatible to the actual key (Key type: {typeof(K).FullName}). Change the default Serdes in StreamConfig or provide correct Serdes via method parameters(using the DSL)");
        }

        private byte[] ValueBytes(V value)
        {
            if (value == null)
                return null;
            if (valueSerdes != null)
                return valueSerdes.Serialize(value, GetSerializationContext(false));
            throw new StreamsException($"The serializer is not compatible to the actual value (Value type: {typeof(V).FullName}). Change the default Serdes in StreamConfig or provide correct Serdes via method parameters(using the DSL)");
        }

        private VersionedRecord<V> FromRecord(VersionedRecord<byte[]> record)
        {
            if (record == null)
                return null;
            if (valueSerdes != null)
                return new VersionedRecord<V>(
                    valueSerdes.Deserialize(record.Value, GetSerializationContext(false)),
                    record.Timestamp,
                    record.ValidTo);
            throw new StreamsException($"The serializer is not compatible to the actual value (Value type: {typeof(V).FullName}). Change the default Serdes in StreamConfig or provide correct Serdes via method parameters(using the DSL)");
        }

        #endregion

        public long Put(K key, V value, long timestamp)
        {
            try
            {
                return MeasureLatency(() => wrapped.Put(KeyBytes(key), ValueBytes(value), timestamp), putSensor);
            }
            catch (ProcessorStateException e)
            {
                throw new ProcessorStateException($"{e.Message}-key:{key}", e);
            }
        }

        public VersionedRecord<V> Delete(K key, long timestamp)
        {
            try
            {
                return MeasureLatency(() => FromRecord(wrapped.Delete(KeyBytes(key), timestamp)), deleteSensor);
            }
            catch (ProcessorStateException e)
            {
                throw new ProcessorStateException($"{e.Message}-key:{key}", e);
            }
        }

        public VersionedRecord<V> Get(K key)
            => MeasureLatency(() => FromRecord(wrapped.Get(KeyBytes(key))), getSensor);

        public VersionedRecord<V> Get(K key, long asOfTimestamp)
            => MeasureLatency(() => FromRecord(wrapped.Get(KeyBytes(key), asOfTimestamp)), getSensor);

        public override void Flush()
            => MeasureLatency(() => base.Flush(), flushSensor);

        public override void Close()
        {
            try
            {
                base.Close();
            }
            finally
            {
                context.Metrics.RemoveStoreSensors(
                    Thread.CurrentThread.Name,
                    context.Id.ToString(),
                    Name);
            }
        }
    }
}
