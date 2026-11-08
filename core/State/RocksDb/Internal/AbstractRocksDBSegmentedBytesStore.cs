using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.State.Enumerator;
using Streamiz.Kafka.Net.State.Internal;
using System;
using System.Collections.Generic;
using System.Threading;
using Microsoft.Extensions.Logging;
using Streamiz.Kafka.Net.Metrics;
using Streamiz.Kafka.Net.Metrics.Internal;

namespace Streamiz.Kafka.Net.State.Internal
{
    internal class AbstractRocksDBSegmentedBytesStore<S> : ISegmentedBytesStore
        where S : ISegment
    {
        protected readonly ILogger logger = null;

        private readonly BytesComparer bytesComparer = new BytesComparer();
        private readonly ISegments<S> segments;
        private readonly IKeySchema keySchema;
        private ProcessorContext context;
        private Sensor expiredRecordSensor;

        private bool isOpen = false;
        private long observedStreamTime = -1;

        public AbstractRocksDBSegmentedBytesStore(
            string name,
            IKeySchema keySchema,
            ISegments<S> segments)
        {
            this.keySchema = keySchema;
            this.segments = segments;
            Name = name;
            logger = Logger.GetLogger(GetType());
        }

        #region ISegmentedBytesStore Impl

        public string Name { get; }

        public bool Persistent => true;
        public bool IsLocally => true;

        public bool IsOpen => isOpen;

        public IKeyValueEnumerator<Bytes, byte[]> All()
        {
            var _segments = segments.AllSegments(true);
            return new SegmentEnumerator<S>(
                            _segments,
                            keySchema.HasNextCondition(null, null, 0, long.MaxValue),
                            null,
                            null,
                            true);
        }

        public void Close()
        {
            isOpen = false;
            segments.Close();
        }

        public IKeyValueEnumerator<Bytes, byte[]> Fetch(Bytes key, long from, long to)
            => Fetch(key, from, to, true);

        public IKeyValueEnumerator<Bytes, byte[]> Fetch(Bytes fromKey, Bytes toKey, long from, long to)
            => Fetch(fromKey, toKey, from, to, true);

        public IKeyValueEnumerator<Bytes, byte[]> FetchAll(long from, long to)
        {
            var _segments = segments.Segments(from, to, true);
            return new SegmentEnumerator<S>(
                                    _segments,
                                    keySchema.HasNextCondition(null, null, from, to),
                                    null,
                                    null,
                                    true);
        }

        public void Flush()
            => segments.Flush();

        public byte[] Get(Bytes key)
        {
            var seg = segments.GetSegmentForTimestamp(keySchema.SegmentTimestamp(key));
            return seg != null ? seg.Get(key) : null;
        }

        public void Init(ProcessorContext context, IStateStore root)
        {
            this.context = context;
            expiredRecordSensor = TaskMetrics.DroppedRecordsSensor(
                Thread.CurrentThread.Name,
                context.Id,
                context.Metrics);
            
            segments.OpenExisting(context, observedStreamTime);
            context.Register(root, (k, v, t) => Put(k, v), RestoreBatch);

            isOpen = true;
        }

        // Same result as calling Put for each record, but consecutive records of the same segment are
        // written in one batch. The pending batch is written before another segment is looked up, because
        // that lookup can drop segments which are no longer live.
        internal void RestoreBatch(IReadOnlyList<(Bytes Key, byte[] Value, long Timestamp)> records)
        {
            S currentSegment = default;
            long currentSegmentId = -1;
            var pending = new List<KeyValuePair<Bytes, byte[]>>();

            void WritePending()
            {
                if (pending.Count > 0)
                    currentSegment.PutAll(pending);
                pending = new List<KeyValuePair<Bytes, byte[]>>();
                currentSegment = default;
            }

            foreach (var record in records)
            {
                long ts = keySchema.SegmentTimestamp(record.Key);
                observedStreamTime = Math.Max(ts, observedStreamTime);
                long segId = segments.SegmentId(ts);

                if (currentSegment != null && segId != currentSegmentId)
                    WritePending();

                var segment = segments.GetOrCreateSegmentIfLive(segId, context, observedStreamTime);
                if (segment == null)
                {
                    expiredRecordSensor.Record(1.0, context.Timestamp);
                    logger.LogWarning("Skipping record for expired segment");
                }
                else
                {
                    currentSegment = segment;
                    currentSegmentId = segId;
                    pending.Add(new KeyValuePair<Bytes, byte[]>(record.Key, record.Value));
                }
            }

            if (currentSegment != null)
                WritePending();
        }

        public void Put(Bytes key, byte[] value)
        {
            long ts = keySchema.SegmentTimestamp(key);
            observedStreamTime = Math.Max(ts, observedStreamTime);
            long segId = segments.SegmentId(ts);
            var segment = segments.GetOrCreateSegmentIfLive(segId, context, observedStreamTime);
            if (segment == null)
            {
                expiredRecordSensor.Record(1.0, context.Timestamp);
                logger.LogWarning("Skipping record for expired segment");
            }
            else
            {
                segment.Put(key, value);
            }
        }

        public void Remove(Bytes key)
        {
            long ts = keySchema.SegmentTimestamp(key);
            observedStreamTime = Math.Max(observedStreamTime, ts);
            var segment = segments.GetSegmentForTimestamp(ts);

            if (segment != null)
            {
                segment.Delete(key);
            }
        }

        public IKeyValueEnumerator<Bytes, byte[]> ReverseAll()
        {
            var _segments = segments.AllSegments(false);
            return new SegmentEnumerator<S>(
                          _segments,
                          keySchema.HasNextCondition(null, null, 0, long.MaxValue),
                          null,
                          null,
                          false);
        }

        public IKeyValueEnumerator<Bytes, byte[]> ReverseFetch(Bytes key, long from, long to)
            => Fetch(key, from, to, false);

        public IKeyValueEnumerator<Bytes, byte[]> ReverseFetch(Bytes fromKey, Bytes toKey, long from, long to)
            => Fetch(fromKey, toKey, from, to, false);

        public IKeyValueEnumerator<Bytes, byte[]> ReverseFetchAll(long from, long to)
        {
            var _segments = segments.Segments(from, to, false);
            return new SegmentEnumerator<S>(
                                    _segments,
                                    keySchema.HasNextCondition(null, null, from, to),
                                    null,
                                    null,
                                    false);
        }

        #endregion

        private IKeyValueEnumerator<Bytes, byte[]> Fetch(Bytes keyFrom, Bytes keyTo, long from, long to, bool forward)
        {
            // a null bound means no bound (session stores' key ranges)
            if (keyFrom != null && keyTo != null && bytesComparer.Compare(keyFrom, keyTo) > 0)
            {
                logger.LogWarning("Returning empty iterator for fetch with invalid key range: from > to. " +
                               "This may be due to range arguments set in the wrong order, " +
                               "or serdes that don't preserve ordering when lexicographically comparing the serialized bytes. " +
                               "Note that the built-in numerical serdes do not follow this for negative numbers");
                return new EmptyKeyValueEnumerator<Bytes, byte[]>();
            }

            var _segments = keySchema.SegmentsToSearch(segments, from, to, forward);
            var binaryFrom = keySchema.LowerRange(keyFrom, from);
            var binaryTo = keySchema.UpperRange(keyTo, to);

            return new SegmentEnumerator<S>(
                           _segments,
                           keySchema.HasNextCondition(keyFrom, keyTo, from, to),
                           binaryFrom,
                           binaryTo,
                           forward);
        }

        private IKeyValueEnumerator<Bytes, byte[]> Fetch(Bytes key, long from, long to, bool forward)
        {
            var _segments = keySchema.SegmentsToSearch(segments, from, to, forward);

            Bytes binaryFrom = keySchema.LowerRangeFixedSize(key, from);
            Bytes binaryTo = keySchema.UpperRangeFixedSize(key, to);

            return new SegmentEnumerator<S>(
                            _segments,
                            keySchema.HasNextCondition(key, key, from, to),
                            binaryFrom,
                            binaryTo,
                            forward);
        }
    }
}
