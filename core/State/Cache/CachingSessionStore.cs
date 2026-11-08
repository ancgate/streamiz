using System;
using System.Collections.Generic;
using System.Linq;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Metrics;
using Streamiz.Kafka.Net.Metrics.Internal;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.State.Cache.Internal;
using Streamiz.Kafka.Net.State.Enumerator;
using Streamiz.Kafka.Net.State.Internal;
using Streamiz.Kafka.Net.Table;
using Streamiz.Kafka.Net.Table.Internal;

namespace Streamiz.Kafka.Net.State.Cache
{
    /// <summary>
    /// Session store with a cache in front of the wrapped store. Writes stay in the cache until the cache is flushed
    /// or evicts them: then they are written to the wrapped store and given to the flush listener (to forward them downstream).
    /// <para>
    /// Reads merge the wrapped store with the cache. The cache entries win, and a null cache entry hides a removed session.
    /// The merged result is built eagerly: session queries usually return few sessions (the ones close to a record).
    /// </para>
    /// </summary>
    internal class CachingSessionStore :
        WrappedStateStore<ISessionStore<Bytes, byte[]>>,
        ISessionStore<Bytes, byte[]>,
        ICachedStateStore<byte[], byte[]>
    {
        private static readonly BytesComparer bytesComparer = new();
        private readonly CacheSize cacheSize;
        private readonly SessionKeySchema keySchema = new();

        private MemoryCache<Bytes, CacheEntryValue> cache;
        private bool cachingEnabled;
        private bool sendOldValue;
        private Action<KeyValuePair<byte[], Change<byte[]>>> flushListener;

        private Sensor hitRatioSensor = NoRunnableSensor.Empty;
        private Sensor totalCacheSizeSensor = NoRunnableSensor.Empty;

        public CachingSessionStore(ISessionStore<Bytes, byte[]> wrapped, long segmentInterval, CacheSize cacheSize)
            : base(wrapped)
        {
            this.cacheSize = cacheSize;
            SegmentCacheFunction = new SegmentedCacheFunction(keySchema, segmentInterval);
            MaxObservedTimestamp = -1;
        }

        internal MemoryCache<Bytes, CacheEntryValue> Cache => cache;
        internal long MaxObservedTimestamp { get; private set; }
        internal SegmentedCacheFunction SegmentCacheFunction { get; }

        public override bool IsCachedStore => cachingEnabled;

        public override void Init(ProcessorContext context, IStateStore root)
        {
            base.Init(context, root);
            CreateCache(context);
            if (cachingEnabled)
            {
                hitRatioSensor = CachingMetrics.HitRatioSensor(context.Id, "cache-session-store", Name, context.Metrics);
                totalCacheSizeSensor = CachingMetrics.TotalCacheSizeBytesSensor(context.Id, "cache-session-store", Name, context.Metrics);
            }
        }

        // internal for testing
        internal void CreateCache(ProcessorContext context)
        {
            cachingEnabled = context.Configuration.DefaultStateStoreCacheMaxBytes > 0 ||
                             cacheSize is { CacheSizeBytes: > 0 };
            if (cachingEnabled)
                cache = new MemoryCache<Bytes, CacheEntryValue>(new MemoryCacheOptions
                {
                    SizeLimit = cacheSize is { CacheSizeBytes: > 0 }
                        ? cacheSize.CacheSizeBytes
                        : context.Configuration.DefaultStateStoreCacheMaxBytes,
                    CompactionPercentage = .20
                }, bytesComparer);
        }

        #region Write

        public void Put(Windowed<Bytes> sessionKey, byte[] aggregate)
        {
            if (!cachingEnabled)
            {
                wrapped.Put(sessionKey, aggregate);
                return;
            }

            var binaryKey = SessionKeySchema.ToBinary(sessionKey);
            var cacheEntry = new CacheEntryValue(
                aggregate,
                context.RecordContext.Headers,
                context.Offset,
                context.Timestamp,
                context.Partition,
                context.Topic);

            PutInternal(SegmentCacheFunction.CacheKey(binaryKey), cacheEntry);
            MaxObservedTimestamp = Math.Max(sessionKey.Window.EndMs, MaxObservedTimestamp);
        }

        public void Remove(Windowed<Bytes> sessionKey)
            => Put(sessionKey, null);

        public override void Flush()
        {
            if (cachingEnabled)
            {
                cache.Compact(1); // evict (and write) everything
                base.Flush();
            }
            else
                wrapped.Flush();
        }

        public bool SetFlushListener(Action<KeyValuePair<byte[], Change<byte[]>>> listener, bool sendOldChanges)
        {
            flushListener = listener;
            sendOldValue = sendOldChanges;
            return true;
        }

        private void PutInternal(Bytes cacheKey, CacheEntryValue entry)
        {
            long totalSize = cacheKey.Get.LongLength + entry.Size;

            var memoryCacheEntryOptions = new MemoryCacheEntryOptions<Bytes, CacheEntryValue>()
                .SetSize(totalSize)
                .RegisterPostEvictionCallback(CacheEntryEviction, cache);

            cache.Set(cacheKey, entry, memoryCacheEntryOptions, EvictionReason.Setted);
            totalCacheSizeSensor.Record(cache.Size);
        }

        private void CacheEntryEviction(Bytes cacheKey, CacheEntryValue value, EvictionReason reason, MemoryCache<Bytes, CacheEntryValue> state)
        {
            if (reason is EvictionReason.Replaced or EvictionReason.None) return;

            var binaryKey = SegmentCacheFunction.Key(cacheKey);
            var sessionKey = SessionKeySchema.From(binaryKey);
            var currentContext = context.RecordContext;

            if (flushListener != null)
            {
                byte[] rawNewValue = value.Value;
                byte[] rawOldValue = rawNewValue == null || sendOldValue
                    ? wrapped.FetchSession(sessionKey.Key, sessionKey.Window.StartMs, sessionKey.Window.EndMs)
                    : null;

                // nothing to write or forward if the session is neither in the cache nor in the wrapped store
                if (rawNewValue != null || rawOldValue != null)
                {
                    context.SetRecordMetaData(value.Context);
                    wrapped.Put(sessionKey, rawNewValue);
                    flushListener(new KeyValuePair<byte[], Change<byte[]>>(
                        binaryKey.Get,
                        new Change<byte[]>(sendOldValue ? rawOldValue : null, rawNewValue)));
                    context.SetRecordMetaData(currentContext);
                }
            }
            else
            {
                context.SetRecordMetaData(value.Context);
                wrapped.Put(sessionKey, value.Value);
                context.SetRecordMetaData(currentContext);
            }

            totalCacheSizeSensor.Record(cache.Size);
        }

        #endregion

        #region Read

        public byte[] FetchSession(Bytes key, long sessionStartTime, long sessionEndTime)
        {
            if (cachingEnabled)
            {
                var cacheKey = SegmentCacheFunction.CacheKey(SessionKeySchema.ToBinary(key, sessionStartTime, sessionEndTime));
                bool found = cache.TryGetValue(cacheKey, out var entry);
                UpdateRatioSensor();
                // a null cached value is a removed session: don't read the wrapped store
                if (found)
                    return entry.Value;
            }

            return wrapped.FetchSession(key, sessionStartTime, sessionEndTime);
        }

        public IKeyValueEnumerator<Windowed<Bytes>, byte[]> FindSessions(Bytes key, long earliestSessionEndTime, long latestSessionStartTime)
            => cachingEnabled
                ? Merge(wrapped.FindSessions(key, earliestSessionEndTime, latestSessionStartTime), key, key, earliestSessionEndTime, latestSessionStartTime, true)
                : wrapped.FindSessions(key, earliestSessionEndTime, latestSessionStartTime);

        public IKeyValueEnumerator<Windowed<Bytes>, byte[]> FindSessions(Bytes keyFrom, Bytes keyTo, long earliestSessionEndTime, long latestSessionStartTime)
            => cachingEnabled
                ? Merge(wrapped.FindSessions(keyFrom, keyTo, earliestSessionEndTime, latestSessionStartTime), keyFrom, keyTo, earliestSessionEndTime, latestSessionStartTime, true)
                : wrapped.FindSessions(keyFrom, keyTo, earliestSessionEndTime, latestSessionStartTime);

        public IKeyValueEnumerator<Windowed<Bytes>, byte[]> BackwardFindSessions(Bytes key, long earliestSessionEndTime, long latestSessionStartTime)
            => cachingEnabled
                ? Merge(wrapped.BackwardFindSessions(key, earliestSessionEndTime, latestSessionStartTime), key, key, earliestSessionEndTime, latestSessionStartTime, false)
                : wrapped.BackwardFindSessions(key, earliestSessionEndTime, latestSessionStartTime);

        public IKeyValueEnumerator<Windowed<Bytes>, byte[]> BackwardFindSessions(Bytes keyFrom, Bytes keyTo, long earliestSessionEndTime, long latestSessionStartTime)
            => cachingEnabled
                ? Merge(wrapped.BackwardFindSessions(keyFrom, keyTo, earliestSessionEndTime, latestSessionStartTime), keyFrom, keyTo, earliestSessionEndTime, latestSessionStartTime, false)
                : wrapped.BackwardFindSessions(keyFrom, keyTo, earliestSessionEndTime, latestSessionStartTime);

        public IKeyValueEnumerator<Windowed<Bytes>, byte[]> Fetch(Bytes key)
            => FindSessions(key, 0, long.MaxValue);

        public IKeyValueEnumerator<Windowed<Bytes>, byte[]> Fetch(Bytes keyFrom, Bytes keyTo)
            => FindSessions(keyFrom, keyTo, 0, long.MaxValue);

        public IKeyValueEnumerator<Windowed<Bytes>, byte[]> BackwardFetch(Bytes key)
            => BackwardFindSessions(key, 0, long.MaxValue);

        public IKeyValueEnumerator<Windowed<Bytes>, byte[]> BackwardFetch(Bytes keyFrom, Bytes keyTo)
            => BackwardFindSessions(keyFrom, keyTo, 0, long.MaxValue);

        // Sessions of the wrapped store, overridden by the cache entries in the same range. Ordered by cache key
        // (segment, key, end, start), which is also the order of the RocksDB and in-memory session stores for one key.
        private IKeyValueEnumerator<Windowed<Bytes>, byte[]> Merge(
            IKeyValueEnumerator<Windowed<Bytes>, byte[]> storeEnumerator,
            Bytes keyFrom,
            Bytes keyTo,
            long earliestSessionEndTime,
            long latestSessionStartTime,
            bool forward)
        {
            var merged = new SortedDictionary<Bytes, KeyValuePair<Windowed<Bytes>, byte[]>>(bytesComparer);

            using (storeEnumerator)
            {
                while (storeEnumerator.MoveNext())
                {
                    var kv = storeEnumerator.Current.Value;
                    merged[SegmentCacheFunction.CacheKey(SessionKeySchema.ToBinary(kv.Key))] = kv;
                }
            }

            foreach (var cacheKey in CacheKeys(keyFrom, keyTo, earliestSessionEndTime, latestSessionStartTime))
            {
                var binaryKey = SegmentCacheFunction.Key(cacheKey);
                if (!Matches(binaryKey.Get, keyFrom, keyTo, earliestSessionEndTime, latestSessionStartTime)
                    || !cache.TryGetValue(cacheKey, out var entry))
                    continue;

                if (entry.Value == null)
                    merged.Remove(cacheKey);
                else
                    merged[cacheKey] = new KeyValuePair<Windowed<Bytes>, byte[]>(SessionKeySchema.From(binaryKey), entry.Value);
            }

            UpdateRatioSensor();

            var results = merged.Values.ToList();
            if (!forward)
                results.Reverse();
            return new WrapEnumerableKeyValueEnumerator<Windowed<Bytes>, byte[]>(results);
        }

        private IEnumerable<Bytes> CacheKeys(Bytes keyFrom, Bytes keyTo, long earliestSessionEndTime, long latestSessionStartTime)
        {
            if (keyFrom == null || keyTo == null)
                return cache.KeySetEnumerable(true).ToList();

            var binaryFrom = keyFrom.Equals(keyTo)
                ? keySchema.LowerRangeFixedSize(keyFrom, earliestSessionEndTime)
                : keySchema.LowerRange(keyFrom, earliestSessionEndTime);
            var binaryTo = keyFrom.Equals(keyTo)
                ? keySchema.UpperRangeFixedSize(keyTo, latestSessionStartTime)
                : keySchema.UpperRange(keyTo, latestSessionStartTime);

            // segments from the earliest session end to the latest end seen in the cache
            long firstSegment = SegmentCacheFunction.SegmentId(earliestSessionEndTime);
            long lastSegment = SegmentCacheFunction.SegmentId(Math.Max(MaxObservedTimestamp, earliestSessionEndTime));

            if (lastSegment - firstSegment > MaxSegmentsSearchedOneByOne)
            {
                // too many segments (small segment interval or a large time range): one range, filtered afterwards
                return CacheKeyRange(
                    SegmentCacheFunction.CacheKey(binaryFrom, firstSegment),
                    SegmentCacheFunction.CacheKey(binaryTo, lastSegment));
            }

            // one key range per segment: only the cache entries of the searched keys are read
            var keys = new List<Bytes>();
            for (long segment = firstSegment; segment <= lastSegment; ++segment)
                keys.AddRange(CacheKeyRange(
                    SegmentCacheFunction.CacheKey(binaryFrom, segment),
                    SegmentCacheFunction.CacheKey(binaryTo, segment)));
            return keys;
        }

        private const long MaxSegmentsSearchedOneByOne = 64;

        private List<Bytes> CacheKeyRange(Bytes from, Bytes to)
            => bytesComparer.Compare(from, to) > 0
                ? new List<Bytes>()
                : cache.KeyRange(from, to, true, true).ToList();

        private static bool Matches(byte[] binaryKey, Bytes keyFrom, Bytes keyTo, long earliestSessionEndTime, long latestSessionStartTime)
        {
            var key = Bytes.Wrap(SessionKeySchema.ExtractKeyBytes(binaryKey));
            return (keyFrom == null || bytesComparer.Compare(key, keyFrom) >= 0)
                   && (keyTo == null || bytesComparer.Compare(key, keyTo) <= 0)
                   && SessionKeySchema.ExtractEndTimestamp(binaryKey) >= earliestSessionEndTime
                   && SessionKeySchema.ExtractStartTimestamp(binaryKey) <= latestSessionStartTime;
        }

        private void UpdateRatioSensor()
        {
            var currentStat = cache.GetCurrentStatistics();
            long total = currentStat.TotalMisses + currentStat.TotalHits;
            if (total > 0)
                hitRatioSensor.Record((double) currentStat.TotalHits / total);
        }

        #endregion
    }
}
