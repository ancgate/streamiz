using System;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.State.Internal;
using Streamiz.Kafka.Net.State.Supplier;

namespace Streamiz.Kafka.Net.State
{
    /// <summary>
    /// A supplier of persistent (RocksDB) session stores.
    /// </summary>
    public class RocksDbSessionBytesStoreSupplier : ISessionBytesStoreSupplier
    {
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="storeName">name of the store</param>
        /// <param name="retention">how long sessions are kept, at least the inactivity gap plus the grace period</param>
        /// <param name="segmentInterval">size of the segments in milliseconds; default: half of the retention, with a minimum of one minute (like Kafka Streams)</param>
        public RocksDbSessionBytesStoreSupplier(string storeName, TimeSpan retention, long? segmentInterval = null)
        {
            Name = storeName;
            Retention = (long) retention.TotalMilliseconds;
            SegmentInterval = segmentInterval ?? Math.Max(Retention / 2, 60_000L);
        }

        /// <summary>
        /// Name of the store
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Metrics scope of the store
        /// </summary>
        public string MetricsScope => "rocksdb-session";

        /// <summary>
        /// Retention (in milliseconds)
        /// </summary>
        public long Retention { get; set; }

        /// <summary>
        /// Size of the segments (in milliseconds)
        /// </summary>
        public long SegmentInterval { get; set; }

        /// <summary>
        /// Build the session store
        /// </summary>
        /// <returns>a new session store</returns>
        public ISessionStore<Bytes, byte[]> Get()
            => new RocksDbSessionStore(
                new RocksDbSegmentedBytesStore(
                    Name,
                    MetricsScope,
                    Retention,
                    SegmentInterval,
                    new SessionKeySchema()));
    }
}
