using Streamiz.Kafka.Net.Crosscutting;

namespace Streamiz.Kafka.Net.State.Supplier
{
    /// <summary>
    /// A store supplier which creates versioned key-value stores (<see cref="IVersionedKeyValueStore{K,V}"/>).
    /// It is a <see cref="IKeyValueBytesStoreSupplier"/>, so it can be given to <c>Materialized.Create(...)</c> to
    /// materialize a source KTable (<c>StreamBuilder.Table(...)</c>) as a versioned store.
    /// <see cref="IStoreSupplier{T}.Get"/> is not supported: versioned stores are created with <see cref="GetVersionedStore"/>.
    /// </summary>
    public interface IVersionedBytesStoreSupplier : IKeyValueBytesStoreSupplier
    {
        /// <summary>
        /// How long (in milliseconds) the older versions are kept. It's also the grace period: records older than
        /// stream time minus the history retention are not put.
        /// </summary>
        long HistoryRetentionMs { get; }

        /// <summary>
        /// Create a new versioned bytes store.
        /// </summary>
        /// <returns>a versioned store</returns>
        IVersionedKeyValueStore<Bytes, byte[]> GetVersionedStore();
    }
}
