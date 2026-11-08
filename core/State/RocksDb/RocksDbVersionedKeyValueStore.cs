using System;
using System.IO;
using System.Threading;
using Microsoft.Extensions.Logging;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Metrics;
using Streamiz.Kafka.Net.Metrics.Internal;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.State.Internal;

namespace Streamiz.Kafka.Net.State
{
    /// <summary>
    /// RocksDB versioned key-value store.
    /// <para>
    /// Layout: the records of a key form a chain ordered by timestamp, each record (tombstones included) being valid until the
    /// timestamp of the next one.
    /// - The latest record of a key, when it's not a tombstone, is in the "latest" store: key -> timestamp | value.
    /// - The older records are in time segments chosen by their validTo, so a segment can be dropped once all its records are
    ///   no longer valid within the history retention: length(key) | key | timestamp -> validTo | flag | value.
    /// - A latest tombstone is kept in the segment of its own timestamp, with validTo = its timestamp (no record is valid after it).
    /// </para>
    /// </summary>
    internal class RocksDbVersionedKeyValueStore : IVersionedKeyValueStore<Bytes, byte[]>
    {
        private const byte TOMBSTONE = 0;
        private const byte VALUE = 1;
        private const int HISTORY_VALUE_HEADER = sizeof(long) + 1;

        private readonly ILogger logger = Logger.GetLogger(typeof(RocksDbVersionedKeyValueStore));
        private readonly long historyRetention;
        private readonly RocksDbMetricsRecorder metricsRecorder;
        private readonly RocksDbKeyValueStore latestStore;
        private readonly RocksDbVersionedSegments segments;

        private ProcessorContext context;
        private Sensor expiredRecordSensor;
        private long observedStreamTime = -1;

        public RocksDbVersionedKeyValueStore(string name, long historyRetention, long segmentInterval, string metricsScope)
        {
            Name = name;
            this.historyRetention = historyRetention;
            metricsRecorder = new RocksDbMetricsRecorder(metricsScope, name);
            latestStore = new RocksDbKeyValueStore($"{name}.latest", $"{name}.latestValues", metricsRecorder);
            segments = new RocksDbVersionedSegments(name, historyRetention, segmentInterval, metricsRecorder);
        }

        public string Name { get; }
        public bool Persistent => true;
        public bool IsLocally => true;
        public bool IsOpen { get; private set; }

        public void Init(ProcessorContext context, IStateStore root)
        {
            this.context = context;
            expiredRecordSensor = TaskMetrics.DroppedRecordsSensor(
                Thread.CurrentThread.Name,
                context.Id,
                context.Metrics);

            metricsRecorder.Init(context.Metrics, context.Id);
            latestStore.OpenDatabase(context);
            segments.OpenExisting(context, observedStreamTime);

            context.Register(root,
                (key, value, timestamp) => Put(key, value, timestamp),
                records =>
                {
                    foreach (var record in records)
                        Put(record.Key, record.Value, record.Timestamp);
                });

            IsOpen = true;
        }

        #region IVersionedKeyValueStore

        public long Put(Bytes key, byte[] value, long timestamp)
        {
            if (timestamp < observedStreamTime - historyRetention)
            {
                RecordExpired("put");
                return VersionedKeyValueStoreReturnCodes.PUT_RETURN_CODE_NOT_PUT;
            }

            observedStreamTime = Math.Max(observedStreamTime, timestamp);
            return DoPut(key, value, timestamp);
        }

        public VersionedRecord<byte[]> Delete(Bytes key, long timestamp)
        {
            if (timestamp < observedStreamTime - historyRetention)
            {
                RecordExpired("delete");
                return null;
            }

            var existing = Get(key, timestamp);
            observedStreamTime = Math.Max(observedStreamTime, timestamp);
            DoPut(key, null, timestamp);
            return existing;
        }

        public VersionedRecord<byte[]> Get(Bytes key)
        {
            var latest = GetLatest(key);
            return latest.HasValue ? new VersionedRecord<byte[]>(latest.Value.Value, latest.Value.Timestamp) : null;
        }

        public VersionedRecord<byte[]> Get(Bytes key, long asOfTimestamp)
        {
            if (asOfTimestamp < observedStreamTime - historyRetention)
                return null;

            var latest = GetLatest(key);
            if (latest.HasValue && latest.Value.Timestamp <= asOfTimestamp)
                return new VersionedRecord<byte[]>(latest.Value.Value, latest.Value.Timestamp);

            var record = FindValidRecord(key, asOfTimestamp);
            if (record == null || record.Value == null)
                return null;

            return new VersionedRecord<byte[]>(record.Value, record.Timestamp, record.ValidTo);
        }

        public void Flush()
        {
            latestStore.Flush();
            segments.Flush();
        }

        public void Close()
        {
            IsOpen = false;
            latestStore.Close();
            segments.Close();
        }

        #endregion

        #region Put

        private long DoPut(Bytes key, byte[] value, long timestamp)
        {
            var latest = GetLatest(key);
            if (latest.HasValue)
            {
                if (timestamp > latest.Value.Timestamp)
                {
                    // the latest record becomes an older version, valid until the new record
                    PutHistory(key, latest.Value.Timestamp, timestamp, latest.Value.Value);
                    PutLatest(key, value, timestamp);
                    return VersionedKeyValueStoreReturnCodes.PUT_RETURN_CODE_VALID_TO_UNDEFINED;
                }

                if (timestamp == latest.Value.Timestamp)
                {
                    PutLatest(key, value, timestamp);
                    return VersionedKeyValueStoreReturnCodes.PUT_RETURN_CODE_VALID_TO_UNDEFINED;
                }

                return PutOutOfOrder(key, value, timestamp, latest.Value.Timestamp);
            }

            // no latest value: the key has no record, or its latest record is a tombstone
            var latestTombstone = FindLatestTombstone(key);
            if (latestTombstone == null)
            {
                PutLatest(key, value, timestamp);
                return VersionedKeyValueStoreReturnCodes.PUT_RETURN_CODE_VALID_TO_UNDEFINED;
            }

            if (timestamp > latestTombstone.Timestamp)
            {
                // the tombstone becomes an older version, valid until the new record
                latestTombstone.Segment.Delete(HistoryKey(key, latestTombstone.Timestamp));
                PutHistory(key, latestTombstone.Timestamp, timestamp, null);
                PutLatest(key, value, timestamp);
                return VersionedKeyValueStoreReturnCodes.PUT_RETURN_CODE_VALID_TO_UNDEFINED;
            }

            if (timestamp == latestTombstone.Timestamp)
            {
                if (value != null)
                {
                    latestTombstone.Segment.Delete(HistoryKey(key, latestTombstone.Timestamp));
                    PutLatest(key, value, timestamp);
                }
                return VersionedKeyValueStoreReturnCodes.PUT_RETURN_CODE_VALID_TO_UNDEFINED;
            }

            return PutOutOfOrder(key, value, timestamp, latestTombstone.Timestamp);
        }

        // A newer record exists: insert the record in the history and return its validTo.
        private long PutOutOfOrder(Bytes key, byte[] value, long timestamp, long latestTimestamp)
        {
            var previous = FindValidRecord(key, timestamp);
            if (previous != null)
            {
                if (previous.Timestamp == timestamp)
                {
                    // same timestamp: replace the value, the validity doesn't change
                    previous.Segment.Put(HistoryKey(key, timestamp), HistoryValue(previous.ValidTo, value));
                    return previous.ValidTo;
                }

                // the previous record is now valid until the new one, which is valid until the previous validTo
                previous.Segment.Delete(HistoryKey(key, previous.Timestamp));
                PutHistory(key, previous.Timestamp, timestamp, previous.Value);
                PutHistory(key, timestamp, previous.ValidTo, value);
                return previous.ValidTo;
            }

            // the record is older than all the retained records: it's valid until the oldest one
            long oldest = FindOldestTimestamp(key) ?? latestTimestamp;
            PutHistory(key, timestamp, oldest, value);
            return oldest;
        }

        private void PutLatest(Bytes key, byte[] value, long timestamp)
        {
            if (value != null)
            {
                latestStore.Put(key, LatestValue(timestamp, value));
                return;
            }

            // a latest tombstone is kept in the segment of its timestamp, with validTo = its timestamp
            latestStore.Delete(key);
            var segment = segments.GetOrCreateSegmentIfLive(segments.SegmentId(timestamp), context, observedStreamTime);
            segment?.Put(HistoryKey(key, timestamp), HistoryValue(timestamp, null));
        }

        private void PutHistory(Bytes key, long timestamp, long validTo, byte[] value)
        {
            // a record which is not valid anymore within the history retention is not needed
            var segment = segments.GetOrCreateSegmentIfLive(segments.SegmentId(validTo), context, observedStreamTime);
            segment?.Put(HistoryKey(key, timestamp), HistoryValue(validTo, value));
        }

        #endregion

        #region Read

        private class HistoryRecord
        {
            public RocksDbVersionedSegment Segment { get; set; }
            public long Timestamp { get; set; }
            public long ValidTo { get; set; }
            public byte[] Value { get; set; }
        }

        private (long Timestamp, byte[] Value)? GetLatest(Bytes key)
        {
            var bytes = latestStore.Get(key);
            if (bytes == null)
                return null;

            long timestamp = ReadLong(bytes, 0);
            var value = new byte[bytes.Length - sizeof(long)];
            Array.Copy(bytes, sizeof(long), value, 0, value.Length);
            return (timestamp, value);
        }

        // the older record of the key valid at timestamp (timestamp <= t < validTo), latest tombstones excluded
        private HistoryRecord FindValidRecord(Bytes key, long timestamp)
        {
            // its validTo is after timestamp, so it's in a segment from the one of timestamp + 1
            foreach (var segment in segments.Segments(timestamp + 1, long.MaxValue, true))
            {
                using var enumerator = segment.ReverseRange(HistoryKey(key, 0), HistoryKey(key, timestamp));
                if (!enumerator.MoveNext() || !enumerator.Current.HasValue)
                    continue;

                // the greatest timestamp <= timestamp of the segment
                var record = ToHistoryRecord(segment, enumerator.Current.Value.Key, enumerator.Current.Value.Value);
                if (record.ValidTo > timestamp)
                    return record;
            }

            return null;
        }

        private HistoryRecord FindLatestTombstone(Bytes key)
        {
            // the latest record of a key without latest value is a tombstone in the most recent segment holding the key
            foreach (var segment in segments.AllSegments(false))
            {
                using var enumerator = segment.ReverseRange(HistoryKey(key, 0), HistoryKey(key, long.MaxValue));
                if (!enumerator.MoveNext() || !enumerator.Current.HasValue)
                    continue;

                var record = ToHistoryRecord(segment, enumerator.Current.Value.Key, enumerator.Current.Value.Value);
                return record.ValidTo == record.Timestamp && record.Value == null ? record : null;
            }

            return null;
        }

        private long? FindOldestTimestamp(Bytes key)
        {
            // the validTo of the records grows with their timestamp: the oldest record is in the oldest segment holding the key
            foreach (var segment in segments.AllSegments(true))
            {
                using var enumerator = segment.Range(HistoryKey(key, 0), HistoryKey(key, long.MaxValue));
                if (enumerator.MoveNext() && enumerator.Current.HasValue)
                    return ExtractTimestamp(enumerator.Current.Value.Key.Get);
            }

            return null;
        }

        #endregion

        #region Encoding

        // length(key) (4) | key | timestamp (8), big-endian: the records of a key are ordered by timestamp
        internal static Bytes HistoryKey(Bytes key, long timestamp)
        {
            var keyBytes = key.Get;
            var bytes = new byte[sizeof(int) + keyBytes.Length + sizeof(long)];
            WriteInt(bytes, 0, keyBytes.Length);
            Array.Copy(keyBytes, 0, bytes, sizeof(int), keyBytes.Length);
            WriteLong(bytes, sizeof(int) + keyBytes.Length, timestamp);
            return Bytes.Wrap(bytes);
        }

        private static long ExtractTimestamp(byte[] historyKey)
            => ReadLong(historyKey, historyKey.Length - sizeof(long));

        // validTo (8) | flag (1) | value
        private static byte[] HistoryValue(long validTo, byte[] value)
        {
            var bytes = new byte[HISTORY_VALUE_HEADER + (value?.Length ?? 0)];
            WriteLong(bytes, 0, validTo);
            bytes[sizeof(long)] = value == null ? TOMBSTONE : VALUE;
            if (value != null)
                Array.Copy(value, 0, bytes, HISTORY_VALUE_HEADER, value.Length);
            return bytes;
        }

        private static HistoryRecord ToHistoryRecord(RocksDbVersionedSegment segment, Bytes historyKey, byte[] historyValue)
        {
            byte[] value = null;
            if (historyValue[sizeof(long)] == VALUE)
            {
                value = new byte[historyValue.Length - HISTORY_VALUE_HEADER];
                Array.Copy(historyValue, HISTORY_VALUE_HEADER, value, 0, value.Length);
            }

            return new HistoryRecord
            {
                Segment = segment,
                Timestamp = ExtractTimestamp(historyKey.Get),
                ValidTo = ReadLong(historyValue, 0),
                Value = value
            };
        }

        // timestamp (8) | value
        private static byte[] LatestValue(long timestamp, byte[] value)
        {
            var bytes = new byte[sizeof(long) + value.Length];
            WriteLong(bytes, 0, timestamp);
            Array.Copy(value, 0, bytes, sizeof(long), value.Length);
            return bytes;
        }

        private static void WriteLong(byte[] bytes, int offset, long value)
        {
            for (int i = 7; i >= 0; --i)
            {
                bytes[offset + i] = (byte) value;
                value >>= 8;
            }
        }

        private static long ReadLong(byte[] bytes, int offset)
        {
            long value = 0;
            for (int i = 0; i < 8; ++i)
                value = (value << 8) | bytes[offset + i];
            return value;
        }

        private static void WriteInt(byte[] bytes, int offset, int value)
        {
            for (int i = 3; i >= 0; --i)
            {
                bytes[offset + i] = (byte) value;
                value >>= 8;
            }
        }

        #endregion

        private void RecordExpired(string operation)
        {
            expiredRecordSensor?.Record(1.0, context?.Timestamp ?? DateTime.Now.GetMilliseconds());
            logger.LogWarning("Skipping record for expired {Operation} in versioned store {Name}", operation, Name);
        }
    }
}
