using System;
using System.Collections.Generic;
using System.Threading;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Metrics;
using Streamiz.Kafka.Net.Metrics.Internal;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State.Cache;
using Streamiz.Kafka.Net.State.Enumerator;
using Streamiz.Kafka.Net.State.Internal;
using Streamiz.Kafka.Net.Table.Internal;
using static Streamiz.Kafka.Net.Crosscutting.ActionHelper;

namespace Streamiz.Kafka.Net.State.Metered
{
    /// <summary>
    /// Typed session store: serializes keys and aggregates for the inner bytes store, and records the store metrics.
    /// </summary>
    internal class MeteredSessionStore<K, AGG> :
        WrappedStateStore<ISessionStore<Bytes, byte[]>>,
        ISessionStore<K, AGG>,
        ICachedStateStore<Windowed<K>, AGG>,
        IRestoreSensorStore
    {
        private ISerDes<K> keySerdes;
        private ISerDes<AGG> valueSerdes;
        private readonly string metricScope;
        private bool initStoreSerdes;

        private Sensor putSensor = NoRunnableSensor.Empty;
        private Sensor fetchSensor = NoRunnableSensor.Empty;
        private Sensor flushSensor = NoRunnableSensor.Empty;
        private Sensor removeSensor = NoRunnableSensor.Empty;

        public MeteredSessionStore(
            ISessionStore<Bytes, byte[]> wrapped,
            ISerDes<K> keySerdes,
            ISerDes<AGG> valueSerdes,
            string metricScope)
            : base(wrapped)
        {
            this.keySerdes = keySerdes;
            this.valueSerdes = valueSerdes;
            this.metricScope = metricScope;
        }

        public override bool IsCachedStore => wrapped is IWrappedStateStore { IsCachedStore: true };

        public Sensor RestoreSensor { get; private set; }

        public override void Init(ProcessorContext context, IStateStore root)
        {
            // before the inner stores are initialized: global stores are restored during their initialization
            RestoreSensor = StateStoreMetrics.RestoreSensor(context.Id, metricScope, Name, context.Metrics);
            base.Init(context, root);
            InitStoreSerde(context);

            putSensor = StateStoreMetrics.PutSensor(context.Id, metricScope, Name, context.Metrics);
            fetchSensor = StateStoreMetrics.FetchSensor(context.Id, metricScope, Name, context.Metrics);
            flushSensor = StateStoreMetrics.FlushSensor(context.Id, metricScope, Name, context.Metrics);
            removeSensor = StateStoreMetrics.RemoveSensor(context.Id, metricScope, Name, context.Metrics);
        }

        private void InitStoreSerde(ProcessorContext context)
        {
            if (initStoreSerdes)
                return;

            keySerdes ??= context.Configuration.DefaultKeySerDes as ISerDes<K>;
            valueSerdes ??= context.Configuration.DefaultValueSerDes as ISerDes<AGG>;
            keySerdes?.Initialize(new SerDesContext(context.Configuration));
            valueSerdes?.Initialize(new SerDesContext(context.Configuration));
            initStoreSerdes = true;
        }

        public bool SetFlushListener(Action<KeyValuePair<Windowed<K>, Change<AGG>>> listener, bool sendOldChanges)
        {
            if (wrapped is ICachedStateStore<byte[], byte[]> store)
            {
                return store.SetFlushListener(
                    kv =>
                    {
                        var key = SessionKeySchema.From(kv.Key, keySerdes, changelogTopic);
                        var newValue = kv.Value.NewValue != null ? FromValue(kv.Value.NewValue) : default;
                        var oldValue = kv.Value.OldValue != null ? FromValue(kv.Value.OldValue) : default;
                        listener(new KeyValuePair<Windowed<K>, Change<AGG>>(key, new Change<AGG>(oldValue, newValue)));
                    }, sendOldChanges);
            }

            return false;
        }

        #region Serdes

        private Bytes KeyBytes(K key)
        {
            if (key == null)
                return null;
            if (keySerdes != null)
                return Bytes.Wrap(keySerdes.Serialize(key, GetSerializationContext(true)));
            throw new StreamsException($"The serializer is not compatible to the actual key (Key type: {typeof(K).FullName}). Change the default Serdes in StreamConfig or provide correct Serdes via method parameters(using the DSL)");
        }

        private Windowed<Bytes> KeyBytes(Windowed<K> sessionKey)
            => new(KeyBytes(sessionKey.Key), sessionKey.Window);

        private K FromKey(byte[] key)
        {
            if (keySerdes != null)
                return keySerdes.Deserialize(key, GetSerializationContext(true));
            throw new StreamsException($"The serializer is not compatible to the actual key (Key type: {typeof(K).FullName}). Change the default Serdes in StreamConfig or provide correct Serdes via method parameters(using the DSL)");
        }

        private byte[] ValueBytes(AGG value)
        {
            if (value == null)
                return null;
            if (valueSerdes != null)
                return valueSerdes.Serialize(value, GetSerializationContext(false));
            throw new StreamsException($"The serializer is not compatible to the actual value (Value type: {typeof(AGG).FullName}). Change the default Serdes in StreamConfig or provide correct Serdes via method parameters(using the DSL)");
        }

        private AGG FromValue(byte[] value)
        {
            if (valueSerdes != null)
                return value != null ? valueSerdes.Deserialize(value, GetSerializationContext(false)) : default;
            throw new StreamsException($"The serializer is not compatible to the actual value (Value type: {typeof(AGG).FullName}). Change the default Serdes in StreamConfig or provide correct Serdes via method parameters(using the DSL)");
        }

        private IKeyValueEnumerator<Windowed<K>, AGG> Typed(IKeyValueEnumerator<Windowed<Bytes>, byte[]> enumerator)
            => new MeteredWindowedKeyValueEnumerator<K, AGG>(enumerator, FromKey, FromValue, fetchSensor);

        #endregion

        public void Put(Windowed<K> sessionKey, AGG aggregate)
        {
            try
            {
                MeasureLatency(() => wrapped.Put(KeyBytes(sessionKey), ValueBytes(aggregate)), putSensor);
            }
            catch (ProcessorStateException e)
            {
                throw new ProcessorStateException($"{e.Message}-key:{sessionKey.Key}", e);
            }
        }

        public void Remove(Windowed<K> sessionKey)
        {
            try
            {
                MeasureLatency(() => wrapped.Remove(KeyBytes(sessionKey)), removeSensor);
            }
            catch (ProcessorStateException e)
            {
                throw new ProcessorStateException($"{e.Message}-key:{sessionKey.Key}", e);
            }
        }

        public AGG FetchSession(K key, long sessionStartTime, long sessionEndTime)
            => MeasureLatency(() => FromValue(wrapped.FetchSession(KeyBytes(key), sessionStartTime, sessionEndTime)), fetchSensor);

        public IKeyValueEnumerator<Windowed<K>, AGG> FindSessions(K key, long earliestSessionEndTime, long latestSessionStartTime)
            => Typed(wrapped.FindSessions(KeyBytes(key), earliestSessionEndTime, latestSessionStartTime));

        public IKeyValueEnumerator<Windowed<K>, AGG> FindSessions(K keyFrom, K keyTo, long earliestSessionEndTime, long latestSessionStartTime)
            => Typed(wrapped.FindSessions(KeyBytes(keyFrom), KeyBytes(keyTo), earliestSessionEndTime, latestSessionStartTime));

        public IKeyValueEnumerator<Windowed<K>, AGG> BackwardFindSessions(K key, long earliestSessionEndTime, long latestSessionStartTime)
            => Typed(wrapped.BackwardFindSessions(KeyBytes(key), earliestSessionEndTime, latestSessionStartTime));

        public IKeyValueEnumerator<Windowed<K>, AGG> BackwardFindSessions(K keyFrom, K keyTo, long earliestSessionEndTime, long latestSessionStartTime)
            => Typed(wrapped.BackwardFindSessions(KeyBytes(keyFrom), KeyBytes(keyTo), earliestSessionEndTime, latestSessionStartTime));

        public IKeyValueEnumerator<Windowed<K>, AGG> Fetch(K key)
            => Typed(wrapped.Fetch(KeyBytes(key)));

        public IKeyValueEnumerator<Windowed<K>, AGG> Fetch(K keyFrom, K keyTo)
            => Typed(wrapped.Fetch(KeyBytes(keyFrom), KeyBytes(keyTo)));

        public IKeyValueEnumerator<Windowed<K>, AGG> BackwardFetch(K key)
            => Typed(wrapped.BackwardFetch(KeyBytes(key)));

        public IKeyValueEnumerator<Windowed<K>, AGG> BackwardFetch(K keyFrom, K keyTo)
            => Typed(wrapped.BackwardFetch(KeyBytes(keyFrom), KeyBytes(keyTo)));

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
