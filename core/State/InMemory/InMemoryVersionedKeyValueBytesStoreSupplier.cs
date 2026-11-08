using System;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.State.Supplier;

namespace Streamiz.Kafka.Net.State.InMemory
{
    /// <summary>
    /// A store supplier which creates in-memory versioned key-value stores.
    /// </summary>
    public class InMemoryVersionedKeyValueBytesStoreSupplier : IVersionedBytesStoreSupplier
    {
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="name">name of the store</param>
        /// <param name="historyRetention">how long the older versions are kept (it's also the grace period of the store)</param>
        public InMemoryVersionedKeyValueBytesStoreSupplier(string name, TimeSpan historyRetention)
        {
            Name = name;
            HistoryRetentionMs = (long) historyRetention.TotalMilliseconds;
        }

        /// <summary>
        /// Name of the store
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Metrics scope of the store
        /// </summary>
        public string MetricsScope => "in-memory-versioned";

        /// <summary>
        /// History retention (milliseconds)
        /// </summary>
        public long HistoryRetentionMs { get; }

        /// <summary>
        /// Create a new versioned store
        /// </summary>
        public IVersionedKeyValueStore<Bytes, byte[]> GetVersionedStore()
            => new InMemoryVersionedKeyValueStore(Name, HistoryRetentionMs);

        /// <summary>
        /// Not supported: a versioned store is not a plain key-value store, use <see cref="GetVersionedStore"/>.
        /// </summary>
        public IKeyValueStore<Bytes, byte[]> Get()
            => throw new NotSupportedException(
                $"{Name} is a versioned store: it can't be used as a plain key-value store. Versioned stores are only supported to materialize source KTables (StreamBuilder.Table).");
    }
}
