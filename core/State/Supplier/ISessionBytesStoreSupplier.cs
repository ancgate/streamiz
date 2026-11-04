using Streamiz.Kafka.Net.Crosscutting;

namespace Streamiz.Kafka.Net.State.Supplier
{
    /// <summary>
    /// A store supplier that can be used to create one or more <see cref="ISessionStore{K, AGG}"/> instances of type &lt;Bytes, byte[]&gt;.
    /// </summary>
    public interface ISessionBytesStoreSupplier : IStoreSupplier<ISessionStore<Bytes, byte[]>>
    {
        /// <summary>
        /// The time period (in milliseconds) for which the <see cref="ISessionStore{K, AGG}"/> retains sessions,
        /// it must be at least the inactivity gap plus the grace period.
        /// </summary>
        long Retention { get; set; }

        /// <summary>
        /// The size of the segments (in milliseconds) of the store. Not used by in-memory stores.
        /// </summary>
        long SegmentInterval { get; set; }
    }
}
