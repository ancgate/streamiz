using System;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.State.Supplier;

namespace Streamiz.Kafka.Net.State.InMemory
{
    /// <summary>
    /// A supplier of in-memory session stores.
    /// </summary>
    public class InMemorySessionBytesStoreSupplier : ISessionBytesStoreSupplier
    {
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="storeName">name of the store</param>
        /// <param name="retention">how long sessions are kept, at least the inactivity gap plus the grace period</param>
        public InMemorySessionBytesStoreSupplier(string storeName, TimeSpan retention)
        {
            Name = storeName;
            Retention = (long) retention.TotalMilliseconds;
        }

        /// <summary>
        /// Name of the store
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Metrics scope of the store
        /// </summary>
        public string MetricsScope => "in-memory-session";

        /// <summary>
        /// Retention (in milliseconds)
        /// </summary>
        public long Retention { get; set; }

        /// <summary>
        /// Not used by in-memory stores, except as the segment size of the cache. Default: 1
        /// </summary>
        public long SegmentInterval { get; set; } = 1;

        /// <summary>
        /// Build the session store
        /// </summary>
        /// <returns>a new session store</returns>
        public ISessionStore<Bytes, byte[]> Get()
            => new InMemorySessionStore(Name, Retention);
    }
}
