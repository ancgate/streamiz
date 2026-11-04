using System;
using System.Collections.Generic;
using Microsoft.Extensions.Logging;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.State.Cache;
using Streamiz.Kafka.Net.State.Internal;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table.Internal;

namespace Streamiz.Kafka.Net.Processors
{
    /// <summary>
    /// Session windows aggregation, ported from Kafka Streams' KStreamSessionWindowAggregate.
    /// For each record, the sessions of its key which are at most the inactivity gap away are merged with the record
    /// into one session [min start, max end]: the merged sessions are removed (a tombstone is forwarded for each),
    /// then the new session is stored and forwarded. Results are forwarded with the session end as timestamp.
    /// </summary>
    internal class KStreamSessionWindowAggregateProcessor<K, V, Agg> : AbstractProcessor<K, V>
    {
        private readonly SessionWindowOptions windows;
        private readonly string storeName;
        private readonly Initializer<Agg> initializer;
        private readonly Aggregator<K, V, Agg> aggregator;
        private readonly Merger<K, Agg> sessionMerger;
        private readonly bool sendOldValues;

        private ISessionStore<K, Agg> store;
        private bool cachingEnabled;
        private long observedStreamTime = -1;

        public KStreamSessionWindowAggregateProcessor(SessionWindowOptions windows,
            string storeName,
            Initializer<Agg> initializer,
            Aggregator<K, V, Agg> aggregator,
            Merger<K, Agg> sessionMerger,
            bool sendOldValues)
        {
            this.windows = windows;
            this.storeName = storeName;
            this.initializer = initializer;
            this.aggregator = aggregator;
            this.sessionMerger = sessionMerger;
            this.sendOldValues = sendOldValues;
        }

        public override void Init(ProcessorContext context)
        {
            base.Init(context);
            store = (ISessionStore<K, Agg>) context.GetStateStore(storeName);

            // with a cached store, the results are forwarded when the cache is flushed
            cachingEnabled = store is IWrappedStateStore { IsCachedStore: true }
                                 and ICachedStateStore<Windowed<K>, Agg> cachedStore &&
                             cachedStore.SetFlushListener(kv =>
                             {
                                 context.CurrentProcessor = this;
                                 Forward(kv.Key,
                                     new Change<Agg>(sendOldValues ? kv.Value.OldValue : default, kv.Value.NewValue),
                                     kv.Key.Window.EndMs);
                             }, sendOldValues);
        }

        public override void Process(K key, V value)
        {
            // if the key is null, the record can't be aggregated in a session
            if (key == null)
            {
                log.LogWarning(
                    "{LogPrefix}Skipping record due to null key. topic=[{Topic}] partition=[{Partition}] offset=[{Offset}]",
                    logPrefix, Context.Topic, Context.Partition, Context.Offset);
                droppedRecordsSensor.Record();
                return;
            }

            long timestamp = Context.Timestamp;
            observedStreamTime = Math.Max(observedStreamTime, timestamp);
            // a session can still be extended by a record up to the inactivity gap after its end
            long windowCloseTime = observedStreamTime - windows.GracePeriodMs - windows.InactivityGapMs;

            var merged = new List<KeyValuePair<Windowed<K>, Agg>>();
            var newSessionWindow = new SessionWindow(timestamp, timestamp);
            SessionWindow mergedWindow = newSessionWindow;
            Agg agg = initializer.Apply();

            using (var enumerator = store.FindSessions(key,
                       timestamp - windows.InactivityGapMs,
                       timestamp + windows.InactivityGapMs))
            {
                while (enumerator.MoveNext())
                {
                    var next = enumerator.Current;
                    if (!next.HasValue)
                        continue;

                    merged.Add(next.Value);
                    agg = sessionMerger.Apply(key, agg, next.Value.Value);
                    mergedWindow = MergeSessionWindow(mergedWindow, (SessionWindow) next.Value.Key.Window);
                }
            }

            if (mergedWindow.EndMs < windowCloseTime)
            {
                log.LogDebug(
                    "{LogPrefix}Skipping record for expired window. topic=[{Topic}] partition=[{Partition}] offset=[{Offset}] timestamp=[{Timestamp}] window=[{WindowStart},{WindowEnd}] expiration=[{Expiration}] streamTime=[{StreamTime}]",
                    logPrefix, Context.Topic, Context.Partition, Context.Offset, timestamp, mergedWindow.StartMs,
                    mergedWindow.EndMs, windowCloseTime, observedStreamTime);
                droppedRecordsSensor.Record();
                return;
            }

            if (!SameWindow(mergedWindow, newSessionWindow))
            {
                foreach (var session in merged)
                {
                    store.Remove(session.Key);
                    MaybeForward(session.Key, default, session.Value);
                }
            }

            agg = aggregator.Apply(key, value, agg);
            var sessionKey = new Windowed<K>(key, mergedWindow);
            store.Put(sessionKey, agg);
            MaybeForward(sessionKey, agg, default);
        }

        private void MaybeForward(Windowed<K> windowedKey, Agg newValue, Agg oldValue)
        {
            if (!cachingEnabled)
                Forward(windowedKey, new Change<Agg>(sendOldValues ? oldValue : default, newValue),
                    windowedKey.Window.EndMs);
        }

        private static bool SameWindow(SessionWindow one, SessionWindow two)
            => one.StartMs == two.StartMs && one.EndMs == two.EndMs;

        private static SessionWindow MergeSessionWindow(SessionWindow one, SessionWindow two)
            => new(Math.Min(one.StartMs, two.StartMs), Math.Max(one.EndMs, two.EndMs));
    }
}
