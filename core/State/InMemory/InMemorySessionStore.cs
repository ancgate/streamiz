using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Microsoft.Extensions.Logging;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Metrics;
using Streamiz.Kafka.Net.Metrics.Internal;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.State.Enumerator;
using Streamiz.Kafka.Net.State.Internal;
using Streamiz.Kafka.Net.Stream;

namespace Streamiz.Kafka.Net.State.InMemory
{
    /// <summary>
    /// In-memory session store. Sessions are kept ordered by end time, then key, then start time,
    /// and are dropped once their end time is older than the stream time minus the retention.
    /// Enumerators work on a snapshot of the matching sessions.
    /// </summary>
    internal class InMemorySessionStore : ISessionStore<Bytes, byte[]>
    {
        private static readonly IComparer<Bytes> bytesComparer = new BytesComparer();
        private readonly ILogger logger = Logger.GetLogger(typeof(InMemorySessionStore));

        private readonly object @lock = new();
        private readonly long retentionPeriod;

        // end time -> key -> start time -> aggregate
        private readonly SortedDictionary<long, SortedDictionary<Bytes, SortedDictionary<long, byte[]>>> endTimeMap = new();

        private ProcessorContext context;
        private Sensor expiredRecordSensor;
        private long observedStreamTime = -1;

        public InMemorySessionStore(string name, long retentionPeriod)
        {
            Name = name;
            this.retentionPeriod = retentionPeriod;
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
                    (key, value, _) => Put(SessionKeySchema.From(key), value),
                    records =>
                    {
                        foreach (var record in records)
                            Put(SessionKeySchema.From(record.Key), record.Value);
                    });
            }

            IsOpen = true;
        }

        public void Put(Windowed<Bytes> sessionKey, byte[] aggregate)
        {
            lock (@lock)
            {
                long windowEndTimestamp = sessionKey.Window.EndMs;
                observedStreamTime = Math.Max(observedStreamTime, windowEndTimestamp);

                if (windowEndTimestamp <= observedStreamTime - retentionPeriod)
                {
                    // the session is already expired: put does nothing, remove has nothing to remove
                    if (aggregate != null)
                    {
                        expiredRecordSensor.Record(1.0, context.Timestamp);
                        logger.LogWarning("Skipping record for expired session");
                    }
                    return;
                }

                if (aggregate != null)
                {
                    if (!endTimeMap.TryGetValue(windowEndTimestamp, out var keyMap))
                    {
                        keyMap = new SortedDictionary<Bytes, SortedDictionary<long, byte[]>>(bytesComparer);
                        endTimeMap.Add(windowEndTimestamp, keyMap);
                    }

                    if (!keyMap.TryGetValue(sessionKey.Key, out var startTimeMap))
                    {
                        startTimeMap = new SortedDictionary<long, byte[]>();
                        keyMap.Add(sessionKey.Key, startTimeMap);
                    }

                    startTimeMap[sessionKey.Window.StartMs] = aggregate;
                }
                else if (endTimeMap.TryGetValue(windowEndTimestamp, out var keyMap) &&
                         keyMap.TryGetValue(sessionKey.Key, out var startTimeMap))
                {
                    startTimeMap.Remove(sessionKey.Window.StartMs);
                    if (startTimeMap.Count == 0)
                    {
                        keyMap.Remove(sessionKey.Key);
                        if (keyMap.Count == 0)
                            endTimeMap.Remove(windowEndTimestamp);
                    }
                }

                RemoveExpiredSegments();
            }
        }

        public void Remove(Windowed<Bytes> sessionKey)
            => Put(sessionKey, null);

        public byte[] FetchSession(Bytes key, long sessionStartTime, long sessionEndTime)
        {
            lock (@lock)
            {
                RemoveExpiredSegments();

                if (endTimeMap.TryGetValue(sessionEndTime, out var keyMap) &&
                    keyMap.TryGetValue(key, out var startTimeMap) &&
                    startTimeMap.TryGetValue(sessionStartTime, out var value))
                    return value;

                return null;
            }
        }

        public IKeyValueEnumerator<Windowed<Bytes>, byte[]> FindSessions(Bytes key, long earliestSessionEndTime, long latestSessionStartTime)
            => Collect(key, key, earliestSessionEndTime, latestSessionStartTime, true);

        public IKeyValueEnumerator<Windowed<Bytes>, byte[]> FindSessions(Bytes keyFrom, Bytes keyTo, long earliestSessionEndTime, long latestSessionStartTime)
            => Collect(keyFrom, keyTo, earliestSessionEndTime, latestSessionStartTime, true);

        public IKeyValueEnumerator<Windowed<Bytes>, byte[]> BackwardFindSessions(Bytes key, long earliestSessionEndTime, long latestSessionStartTime)
            => Collect(key, key, earliestSessionEndTime, latestSessionStartTime, false);

        public IKeyValueEnumerator<Windowed<Bytes>, byte[]> BackwardFindSessions(Bytes keyFrom, Bytes keyTo, long earliestSessionEndTime, long latestSessionStartTime)
            => Collect(keyFrom, keyTo, earliestSessionEndTime, latestSessionStartTime, false);

        public IKeyValueEnumerator<Windowed<Bytes>, byte[]> Fetch(Bytes key)
            => FindSessions(key, 0, long.MaxValue);

        public IKeyValueEnumerator<Windowed<Bytes>, byte[]> Fetch(Bytes keyFrom, Bytes keyTo)
            => FindSessions(keyFrom, keyTo, 0, long.MaxValue);

        public IKeyValueEnumerator<Windowed<Bytes>, byte[]> BackwardFetch(Bytes key)
            => BackwardFindSessions(key, 0, long.MaxValue);

        public IKeyValueEnumerator<Windowed<Bytes>, byte[]> BackwardFetch(Bytes keyFrom, Bytes keyTo)
            => BackwardFindSessions(keyFrom, keyTo, 0, long.MaxValue);

        public void Flush()
        {
        }

        public void Close()
        {
            lock (@lock)
            {
                endTimeMap.Clear();
                IsOpen = false;
            }
        }

        private IKeyValueEnumerator<Windowed<Bytes>, byte[]> Collect(Bytes keyFrom, Bytes keyTo,
            long earliestSessionEndTime, long latestSessionStartTime, bool forward)
        {
            if (keyFrom != null && keyTo != null && bytesComparer.Compare(keyFrom, keyTo) > 0)
            {
                logger.LogWarning("Returning empty iterator for fetch with invalid key range: from > to. " +
                                  "This may be due to serdes that don't preserve ordering when lexicographically comparing the serialized bytes.");
                return new EmptyKeyValueEnumerator<Windowed<Bytes>, byte[]>();
            }

            var results = new List<KeyValuePair<Windowed<Bytes>, byte[]>>();
            lock (@lock)
            {
                RemoveExpiredSegments();

                foreach (var endEntry in endTimeMap.Where(e => e.Key >= earliestSessionEndTime))
                {
                    foreach (var keyEntry in endEntry.Value)
                    {
                        if (keyFrom != null && bytesComparer.Compare(keyEntry.Key, keyFrom) < 0)
                            continue;
                        if (keyTo != null && bytesComparer.Compare(keyEntry.Key, keyTo) > 0)
                            break;

                        foreach (var startEntry in keyEntry.Value)
                        {
                            if (startEntry.Key > latestSessionStartTime)
                                break;

                            results.Add(new KeyValuePair<Windowed<Bytes>, byte[]>(
                                new Windowed<Bytes>(keyEntry.Key, new SessionWindow(startEntry.Key, endEntry.Key)),
                                startEntry.Value));
                        }
                    }
                }
            }

            if (!forward)
                results.Reverse();

            return new WrapEnumerableKeyValueEnumerator<Windowed<Bytes>, byte[]>(results);
        }

        private void RemoveExpiredSegments()
        {
            long minLiveTime = Math.Max(0L, observedStreamTime - retentionPeriod + 1);
            foreach (var endTime in endTimeMap.Keys.TakeWhile(t => t < minLiveTime).ToList())
                endTimeMap.Remove(endTime);
        }
    }
}
