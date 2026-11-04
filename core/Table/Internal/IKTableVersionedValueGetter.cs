using Streamiz.Kafka.Net.State;

namespace Streamiz.Kafka.Net.Table.Internal
{
    /// <summary>
    /// A value getter which can also read the value of a key as of a timestamp, when the table is materialized as a
    /// versioned store (KIP-889). Used by joins which look up the table at the record timestamp (KIP-923).
    /// </summary>
    internal interface IKTableVersionedValueGetter<K, V> : IKTableValueGetter<K, V>
    {
        /// <summary>
        /// True if the table is materialized as a versioned store, so <see cref="Get(K, long)"/> can be used.
        /// </summary>
        bool IsVersioned { get; }

        /// <summary>
        /// Value of the key as of <paramref name="asOfTimestamp"/> (versioned tables only).
        /// </summary>
        ValueAndTimestamp<V> Get(K key, long asOfTimestamp);
    }
}
