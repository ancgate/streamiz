using Streamiz.Kafka.Net.Processors;

namespace Streamiz.Kafka.Net.State
{
    /// <summary>
    /// A version of a record in a <see cref="IVersionedKeyValueStore{K,V}"/>: its value, its timestamp and,
    /// for a record which is not the latest one, the timestamp until which it is valid (exclusive).
    /// </summary>
    /// <typeparam name="V">value type</typeparam>
    public class VersionedRecord<V>
    {
        /// <summary>
        /// Create a record
        /// </summary>
        /// <param name="value">value of the record (not null)</param>
        /// <param name="timestamp">timestamp of the record</param>
        /// <param name="validTo">timestamp of the next newer record, null if this record is the latest one</param>
        public VersionedRecord(V value, long timestamp, long? validTo = null)
        {
            Value = value;
            Timestamp = timestamp;
            ValidTo = validTo;
        }

        /// <summary>
        /// Value of the record
        /// </summary>
        public V Value { get; }

        /// <summary>
        /// Timestamp of the record
        /// </summary>
        public long Timestamp { get; }

        /// <summary>
        /// The record is valid in [<see cref="Timestamp"/>, <see cref="ValidTo"/>). Null if the record is the latest one.
        /// </summary>
        public long? ValidTo { get; }

        /// <inheritdoc/>
        public override string ToString() => $"VersionedRecord{{value={Value}, timestamp={Timestamp}, validTo={ValidTo}}}";
    }

    /// <summary>
    /// Return codes of <see cref="IVersionedKeyValueStore{K,V}.Put"/>.
    /// </summary>
    public static class VersionedKeyValueStoreReturnCodes
    {
        /// <summary>
        /// The record was put and it is the latest record of its key (its validTo is undefined).
        /// </summary>
        public const long PUT_RETURN_CODE_VALID_TO_UNDEFINED = -1;

        /// <summary>
        /// The record was not put, because its timestamp is older than the history retention (stream time - history retention).
        /// </summary>
        public const long PUT_RETURN_CODE_NOT_PUT = long.MinValue;
    }

    /// <summary>
    /// A read-only versioned key-value store (KIP-889): each key can have several versions (records), valid
    /// from their timestamp to the timestamp of the next newer record.
    /// </summary>
    /// <typeparam name="K">key type</typeparam>
    /// <typeparam name="V">value type</typeparam>
    public interface IReadOnlyVersionedKeyValueStore<K, V>
    {
        /// <summary>
        /// Get the latest record of a key.
        /// </summary>
        /// <param name="key">key</param>
        /// <returns>the latest record, or null if the key doesn't exist or its latest record is a tombstone</returns>
        VersionedRecord<V> Get(K key);

        /// <summary>
        /// Get the record of a key which was valid at <paramref name="asOfTimestamp"/>: the record with the greatest
        /// timestamp which is smaller than or equal to <paramref name="asOfTimestamp"/>.
        /// </summary>
        /// <param name="key">key</param>
        /// <param name="asOfTimestamp">timestamp</param>
        /// <returns>the record, or null if there is no such record, if it is a tombstone, or if <paramref name="asOfTimestamp"/>
        /// is older than the history retention (stream time - history retention)</returns>
        VersionedRecord<V> Get(K key, long asOfTimestamp);
    }

    /// <summary>
    /// A versioned key-value store (KIP-889): it keeps the history of each key for the history retention, so the
    /// value of a key can be read as of a timestamp. Records older than the history retention are not put.
    /// </summary>
    /// <typeparam name="K">key type</typeparam>
    /// <typeparam name="V">value type</typeparam>
    public interface IVersionedKeyValueStore<K, V> : IStateStore, IReadOnlyVersionedKeyValueStore<K, V>
    {
        /// <summary>
        /// Add a record (a new version) for a key. A null value is a tombstone: the key has no value from <paramref name="timestamp"/>.
        /// A record with the same key and timestamp as an existing one replaces it.
        /// </summary>
        /// <param name="key">key</param>
        /// <param name="value">value, or null for a tombstone</param>
        /// <param name="timestamp">timestamp of the record</param>
        /// <returns>
        /// <see cref="VersionedKeyValueStoreReturnCodes.PUT_RETURN_CODE_VALID_TO_UNDEFINED"/> if the record is the latest of its key,
        /// <see cref="VersionedKeyValueStoreReturnCodes.PUT_RETURN_CODE_NOT_PUT"/> if it was not put (older than the history retention),
        /// otherwise the timestamp of the next newer record (the validTo of the record which was put).
        /// </returns>
        long Put(K key, V value, long timestamp);

        /// <summary>
        /// Delete the value of a key from <paramref name="timestamp"/>: it is the same as putting a tombstone.
        /// </summary>
        /// <param name="key">key</param>
        /// <param name="timestamp">timestamp of the tombstone</param>
        /// <returns>the record which was valid at <paramref name="timestamp"/> before the delete, or null</returns>
        VersionedRecord<V> Delete(K key, long timestamp);
    }
}
