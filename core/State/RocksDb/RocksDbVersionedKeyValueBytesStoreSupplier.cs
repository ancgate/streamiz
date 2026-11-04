using System;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.State.Supplier;

namespace Streamiz.Kafka.Net.State
{
    /// <summary>
    /// A store supplier which creates RocksDB versioned key-value stores.
    /// </summary>
    public class RocksDbVersionedKeyValueBytesStoreSupplier : IVersionedBytesStoreSupplier
    {
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="name">name of the store</param>
        /// <param name="historyRetention">how long the older versions are kept (it's also the grace period of the store)</param>
        /// <param name="segmentInterval">size of the segments of older versions (milliseconds). Default: max(history retention / 3, 2 seconds)</param>
        public RocksDbVersionedKeyValueBytesStoreSupplier(string name, TimeSpan historyRetention, long? segmentInterval = null)
        {
            Name = name;
            HistoryRetentionMs = (long) historyRetention.TotalMilliseconds;
            SegmentIntervalMs = segmentInterval ?? Math.Max(HistoryRetentionMs / 3, 2000L);
            if (SegmentIntervalMs <= 0)
                throw new ArgumentException("segmentInterval must be strictly positive");
        }

        /// <summary>
        /// Name of the store
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Metrics scope of the store
        /// </summary>
        public string MetricsScope => "rocksdb-versioned";

        /// <summary>
        /// History retention (milliseconds)
        /// </summary>
        public long HistoryRetentionMs { get; }

        /// <summary>
        /// Size of the segments of older versions (milliseconds)
        /// </summary>
        public long SegmentIntervalMs { get; }

        /// <summary>
        /// Create a new versioned store
        /// </summary>
        public IVersionedKeyValueStore<Bytes, byte[]> GetVersionedStore()
            => new RocksDbVersionedKeyValueStore(Name, HistoryRetentionMs, SegmentIntervalMs, MetricsScope);

        /// <summary>
        /// Not supported: a versioned store is not a plain key-value store, use <see cref="GetVersionedStore"/>.
        /// </summary>
        public IKeyValueStore<Bytes, byte[]> Get()
            => throw new NotSupportedException(
                $"{Name} is a versioned store: it can't be used as a plain key-value store. Versioned stores are only supported to materialize source KTables (StreamBuilder.Table).");
    }
}
