using Streamiz.Kafka.Net.Metrics.Internal;
using Streamiz.Kafka.Net.Processors;

namespace Streamiz.Kafka.Net.State.Internal
{
    /// <summary>
    /// A segment of a RocksDB versioned store: a RocksDB key-value store with the default (bytes) key order.
    /// </summary>
    internal class RocksDbVersionedSegment : RocksDbKeyValueStore, ISegment
    {
        public RocksDbVersionedSegment(string segmentName, string storeName, RocksDbMetricsRecorder recorder)
            : base(segmentName, storeName, recorder)
        {
        }

        internal void OpenDB(ProcessorContext context)
            => OpenDatabase(context);

        public void Destroy()
            => DbDir.Delete(true);
    }

    internal class RocksDbVersionedSegments : AbstractSegments<RocksDbVersionedSegment>
    {
        private readonly RocksDbMetricsRecorder metricsRecorder;

        public RocksDbVersionedSegments(string name, long retention, long segmentInterval, RocksDbMetricsRecorder metricsRecorder)
            : base(name, retention, segmentInterval)
        {
            this.metricsRecorder = metricsRecorder;
        }

        public override RocksDbVersionedSegment GetOrCreateSegment(long segmentId, ProcessorContext context)
        {
            if (segments.TryGetValue(segmentId, out var segment))
                return segment;

            segment = new RocksDbVersionedSegment(SegmentName(segmentId), name, metricsRecorder);
            segments.Add(segmentId, segment);
            segment.OpenDB(context);
            return segment;
        }
    }
}
