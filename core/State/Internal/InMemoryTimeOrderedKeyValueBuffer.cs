using System;
using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.SerDes;

namespace Streamiz.Kafka.Net.State.Internal
{
    /// <summary>
    /// Buffer of records ordered by timestamp, which keeps every record (several records can have the same key and the
    /// same timestamp, they keep their arrival order). Used by stream-table joins with a grace period (KIP-923).
    /// <para>
    /// Changelog: key = timestamp (8 bytes) | sequence number (8 bytes), big-endian;
    /// value = key length (4) | key | value length (4, -1 if null) | value | record context.
    /// Evicted records are logged as tombstones.
    /// </para>
    /// </summary>
    internal class InMemoryTimeOrderedKeyValueBuffer<K, V> : IStateStore
    {
        private class BufferedRecord
        {
            public byte[] Key { get; set; }
            public byte[] Value { get; set; }
            public byte[] RecordContext { get; set; }
        }

        private readonly SortedDictionary<(long Timestamp, long Sequence), BufferedRecord> records = new();
        private readonly bool loggingEnabled;
        private ISerDes<K> keySerdes;
        private ISerDes<V> valueSerdes;
        private ProcessorContext context;
        private string changelogTopic;
        private long sequence;

        public InMemoryTimeOrderedKeyValueBuffer(string name, bool loggingEnabled, ISerDes<K> keySerdes, ISerDes<V> valueSerdes)
        {
            Name = name;
            this.loggingEnabled = loggingEnabled;
            this.keySerdes = keySerdes;
            this.valueSerdes = valueSerdes;
        }

        public string Name { get; }
        public bool Persistent => false;
        public bool IsLocally => true;
        public bool IsOpen { get; private set; }

        public int NumRecords => records.Count;

        public long MinTimestamp => records.Count == 0 ? long.MaxValue : records.Keys.First().Timestamp;

        public void Init(ProcessorContext context, IStateStore root)
        {
            this.context = context;
            changelogTopic = context.ChangelogFor(Name);
            context.Register(root, Restore, RestoreBatch);
            IsOpen = true;
        }

        public void SetSerdesIfNull(ISerDes<K> contextKeySerdes, ISerDes<V> contextValueSerdes)
        {
            keySerdes ??= contextKeySerdes;
            valueSerdes ??= contextValueSerdes;

            if (keySerdes == null && context.Configuration.DefaultKeySerDes is ISerDes<K> defaultKeySerDes)
                keySerdes = defaultKeySerDes;

            if (valueSerdes == null && context.Configuration.DefaultValueSerDes is ISerDes<V> defaultValueSerDes)
                valueSerdes = defaultValueSerDes;

            if (keySerdes == null || valueSerdes == null)
                throw new StreamsException(
                    $"Incorrect {(valueSerdes == null ? "value" : "key")} serdes configuration for the buffer {Name} of the stream-table join. Please set explicit serdes for the stream (key and value)");

            keySerdes.Initialize(context.SerDesContext);
            valueSerdes.Initialize(context.SerDesContext);
        }

        /// <summary>
        /// Buffer a record with its record context (timestamp, headers, topic, partition, offset).
        /// </summary>
        public void Put(K key, V value, IRecordContext recordContext)
        {
            var keyBytes = keySerdes.Serialize(key, new SerializationContext(MessageComponentType.Key, changelogTopic, recordContext.Headers));
            var valueBytes = value == null
                ? null
                : valueSerdes.Serialize(value, new SerializationContext(MessageComponentType.Value, changelogTopic, recordContext.Headers));

            var record = new BufferedRecord
            {
                Key = keyBytes,
                Value = valueBytes,
                RecordContext = recordContext.Serialize()
            };
            var bufferKey = (recordContext.Timestamp, sequence++);
            records.Add(bufferKey, record);

            if (loggingEnabled)
                context.Log(Name, ToChangelogKey(bufferKey), ToChangelogValue(record), recordContext.Timestamp);
        }

        /// <summary>
        /// Remove the records whose timestamp is lower or equal than <paramref name="maxTimestamp"/>, in timestamp order,
        /// and give each one to <paramref name="emit"/>.
        /// </summary>
        public void EvictUntil(long maxTimestamp, Action<K, V, IRecordContext> emit)
        {
            while (records.Count > 0)
            {
                var first = records.First();
                if (first.Key.Timestamp > maxTimestamp)
                    break;

                records.Remove(first.Key);
                if (loggingEnabled)
                    context.Log(Name, ToChangelogKey(first.Key), null, first.Key.Timestamp);

                var recordContext = RecordContext.Deserialize(ByteBuffer.Build(first.Value.RecordContext, true));
                var key = keySerdes.Deserialize(first.Value.Key,
                    new SerializationContext(MessageComponentType.Key, changelogTopic, recordContext.Headers));
                var value = first.Value.Value == null
                    ? default
                    : valueSerdes.Deserialize(first.Value.Value,
                        new SerializationContext(MessageComponentType.Value, changelogTopic, recordContext.Headers));
                emit(key, value, recordContext);
            }
        }

        public void Flush()
        {
            // records are logged when they are put or evicted
        }

        public void Close()
        {
            records.Clear();
            IsOpen = false;
        }

        #region Changelog

        private static Bytes ToChangelogKey((long Timestamp, long Sequence) bufferKey)
        {
            using var buffer = ByteBuffer.Build(2 * sizeof(long), true);
            return Bytes.Wrap(buffer.PutLong(bufferKey.Timestamp).PutLong(bufferKey.Sequence).ToArray());
        }

        private static (long Timestamp, long Sequence) FromChangelogKey(Bytes key)
        {
            using var buffer = ByteBuffer.Build(key.Get, true);
            return (buffer.GetLong(0), buffer.GetLong(sizeof(long)));
        }

        private static byte[] ToChangelogValue(BufferedRecord record)
        {
            using var buffer = ByteBuffer.Build(
                sizeof(int) + record.Key.Length + sizeof(int) + (record.Value?.Length ?? 0) + record.RecordContext.Length,
                true);
            buffer.PutInt(record.Key.Length).Put(record.Key);
            if (record.Value == null)
                buffer.PutInt(-1);
            else
                buffer.PutInt(record.Value.Length).Put(record.Value);
            buffer.Put(record.RecordContext);
            return buffer.ToArray();
        }

        private static BufferedRecord FromChangelogValue(byte[] value)
        {
            using var buffer = ByteBuffer.Build(value, true);
            var key = buffer.GetBytes(buffer.GetInt());
            var valueBytes = buffer.GetNullableSizePrefixedArray();
            int offset = sizeof(int) + key.Length + sizeof(int) + (valueBytes?.Length ?? 0);
            var recordContext = buffer.GetBytes(offset, value.Length - offset);
            return new BufferedRecord {Key = key, Value = valueBytes, RecordContext = recordContext};
        }

        private void Restore(Bytes key, byte[] value, long timestamp)
        {
            var bufferKey = FromChangelogKey(key);
            if (value == null)
                records.Remove(bufferKey);
            else
                records[bufferKey] = FromChangelogValue(value);
            sequence = Math.Max(sequence, bufferKey.Sequence + 1);
        }

        private void RestoreBatch(IReadOnlyList<(Bytes Key, byte[] Value, long Timestamp)> batch)
        {
            foreach (var record in batch)
                Restore(record.Key, record.Value, record.Timestamp);
        }

        #endregion
    }

    internal class InMemoryTimeOrderedKeyValueBufferBuilder<K, V>
        : AbstractStoreBuilder<K, V, InMemoryTimeOrderedKeyValueBuffer<K, V>>
    {
        public InMemoryTimeOrderedKeyValueBufferBuilder(string name, ISerDes<K> keySerde, ISerDes<V> valueSerde)
            : base(name, keySerde, valueSerde)
        {
            WithCachingDisabled();
        }

        public override bool IsWindowStore => false;
        public override long RetentionMs => -1;

        public override InMemoryTimeOrderedKeyValueBuffer<K, V> Build(IStreamConfig config)
            => new(Name, LoggingEnabled, keySerdes, valueSerdes);
    }
}
