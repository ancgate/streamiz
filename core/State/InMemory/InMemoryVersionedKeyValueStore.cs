using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Microsoft.Extensions.Logging;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Metrics;
using Streamiz.Kafka.Net.Metrics.Internal;
using Streamiz.Kafka.Net.Processors;

namespace Streamiz.Kafka.Net.State.InMemory
{
    /// <summary>
    /// In-memory versioned key-value store. Each key keeps its records (versions) ordered by timestamp, tombstones included:
    /// a record is valid until the timestamp of the next one. Records older than stream time minus the history retention
    /// are not put, and older versions which are no longer valid within the history retention are removed.
    /// </summary>
    internal class InMemoryVersionedKeyValueStore : IVersionedKeyValueStore<Bytes, byte[]>
    {
        private readonly ILogger logger = Logger.GetLogger(typeof(InMemoryVersionedKeyValueStore));
        private readonly object @lock = new();
        private readonly long historyRetention;

        // key -> timestamp -> value (null = tombstone)
        private readonly Dictionary<Bytes, SortedList<long, byte[]>> versions = new();

        private ProcessorContext context;
        private Sensor expiredRecordSensor;
        private long observedStreamTime = -1;
        private long lastFullCleanup = -1;

        public InMemoryVersionedKeyValueStore(string name, long historyRetention)
        {
            Name = name;
            this.historyRetention = historyRetention;
        }

        public string Name { get; }
        public bool Persistent => false;
        public bool IsLocally => true;
        public bool IsOpen { get; private set; }

        public void Init(ProcessorContext context, IStateStore root)
        {
            this.context = context;
            expiredRecordSensor = TaskMetrics.DroppedRecordsSensor(
                Thread.CurrentThread.Name,
                context.Id,
                context.Metrics);

            if (root != null)
            {
                context.Register(root,
                    (key, value, timestamp) => Put(key, value, timestamp),
                    records =>
                    {
                        foreach (var record in records)
                            Put(record.Key, record.Value, record.Timestamp);
                    });
            }

            IsOpen = true;
        }

        public long Put(Bytes key, byte[] value, long timestamp)
        {
            lock (@lock)
            {
                if (timestamp < observedStreamTime - historyRetention)
                {
                    RecordExpired("put");
                    return VersionedKeyValueStoreReturnCodes.PUT_RETURN_CODE_NOT_PUT;
                }

                observedStreamTime = Math.Max(observedStreamTime, timestamp);
                return DoPut(key, value, timestamp);
            }
        }

        public VersionedRecord<byte[]> Delete(Bytes key, long timestamp)
        {
            lock (@lock)
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
        }

        public VersionedRecord<byte[]> Get(Bytes key)
        {
            lock (@lock)
            {
                if (!versions.TryGetValue(key, out var keyVersions) || keyVersions.Count == 0)
                    return null;

                int last = keyVersions.Count - 1;
                var value = keyVersions.Values[last];
                return value == null ? null : new VersionedRecord<byte[]>(value, keyVersions.Keys[last]);
            }
        }

        public VersionedRecord<byte[]> Get(Bytes key, long asOfTimestamp)
        {
            lock (@lock)
            {
                // older than the history retention
                if (asOfTimestamp < observedStreamTime - historyRetention)
                    return null;

                if (!versions.TryGetValue(key, out var keyVersions))
                    return null;

                int index = IndexAtOrBefore(keyVersions, asOfTimestamp);
                if (index < 0 || keyVersions.Values[index] == null)
                    return null;

                long? validTo = index + 1 < keyVersions.Count ? keyVersions.Keys[index + 1] : null;
                return new VersionedRecord<byte[]>(keyVersions.Values[index], keyVersions.Keys[index], validTo);
            }
        }

        public void Flush()
        {
        }

        public void Close()
        {
            lock (@lock)
            {
                versions.Clear();
                IsOpen = false;
            }
        }

        private long DoPut(Bytes key, byte[] value, long timestamp)
        {
            if (!versions.TryGetValue(key, out var keyVersions))
            {
                keyVersions = new SortedList<long, byte[]>();
                versions.Add(key, keyVersions);
            }

            keyVersions[timestamp] = value;
            int index = keyVersions.IndexOfKey(timestamp);
            long validTo = index + 1 < keyVersions.Count
                ? keyVersions.Keys[index + 1]
                : VersionedKeyValueStoreReturnCodes.PUT_RETURN_CODE_VALID_TO_UNDEFINED;

            Cleanup(key, keyVersions);
            if (observedStreamTime - lastFullCleanup >= Math.Max(historyRetention, 1))
            {
                foreach (var kv in versions.ToList())
                    Cleanup(kv.Key, kv.Value);
                lastFullCleanup = observedStreamTime;
            }

            return validTo;
        }

        // remove the versions which are not valid anymore within the history retention
        private void Cleanup(Bytes key, SortedList<long, byte[]> keyVersions)
        {
            long minValidTimestamp = observedStreamTime - historyRetention;
            // a version is needed while its validTo (the next timestamp) is after the oldest timestamp which can be queried
            while (keyVersions.Count > 1 && keyVersions.Keys[1] <= minValidTimestamp)
                keyVersions.RemoveAt(0);

            // a latest tombstone older than the history retention is not needed anymore
            if (keyVersions.Count == 1 && keyVersions.Values[0] == null && keyVersions.Keys[0] < minValidTimestamp)
                keyVersions.RemoveAt(0);

            if (keyVersions.Count == 0)
                versions.Remove(key);
        }

        // index of the greatest timestamp <= timestamp, -1 if none
        private static int IndexAtOrBefore(SortedList<long, byte[]> keyVersions, long timestamp)
        {
            var keys = keyVersions.Keys;
            int low = 0, high = keys.Count - 1, result = -1;
            while (low <= high)
            {
                int middle = low + (high - low) / 2;
                if (keys[middle] <= timestamp)
                {
                    result = middle;
                    low = middle + 1;
                }
                else
                    high = middle - 1;
            }

            return result;
        }

        private void RecordExpired(string operation)
        {
            expiredRecordSensor?.Record(1.0, context?.Timestamp ?? DateTime.Now.GetMilliseconds());
            logger.LogWarning("Skipping record for expired {Operation} in versioned store {Name}", operation, Name);
        }
    }
}
