using System;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.State.InMemory;

namespace Streamiz.Kafka.Net.Table
{
    /// <summary>
    /// <see cref="InMemoryVersioned"/> is a helper class to build a <see cref="Materialized{K, V, S}"/> for an in-memory
    /// versioned key-value store (KIP-889). Versioned stores are supported for source KTables (<c>StreamBuilder.Table</c>).
    /// </summary>
    public static class InMemoryVersioned
    {
        /// <summary>
        /// Materialize an in-memory versioned key-value store with the given name.
        /// </summary>
        /// <typeparam name="K">key type</typeparam>
        /// <typeparam name="V">value type</typeparam>
        /// <param name="storeName">the name of the state store</param>
        /// <param name="historyRetention">how long the older versions are kept; it's also the grace period of the store</param>
        /// <returns>a new materialized instance with the given storeName</returns>
        public static Materialized<K, V, IKeyValueStore<Bytes, byte[]>> @As<K, V>(string storeName, TimeSpan historyRetention)
            => Materialized<K, V, IKeyValueStore<Bytes, byte[]>>
                .Create(new InMemoryVersionedKeyValueBytesStoreSupplier(storeName, historyRetention))
                .WithName(storeName);

        /// <summary>
        /// Materialize an in-memory versioned key-value store with the given name.
        /// </summary>
        /// <typeparam name="K">key type</typeparam>
        /// <typeparam name="V">value type</typeparam>
        /// <typeparam name="KS">New serializer for <typeparamref name="K"/> type</typeparam>
        /// <typeparam name="VS">New serializer for <typeparamref name="V"/> type</typeparam>
        /// <param name="storeName">the name of the state store</param>
        /// <param name="historyRetention">how long the older versions are kept; it's also the grace period of the store</param>
        /// <returns>a new materialized instance with the given storeName</returns>
        public static Materialized<K, V, IKeyValueStore<Bytes, byte[]>> @As<K, V, KS, VS>(string storeName, TimeSpan historyRetention)
            where KS : ISerDes<K>, new()
            where VS : ISerDes<V>, new()
            => Materialized<K, V, IKeyValueStore<Bytes, byte[]>>
                .Create<KS, VS>(new InMemoryVersionedKeyValueBytesStoreSupplier(storeName, historyRetention))
                .WithName(storeName);
    }

    /// <summary>
    /// <see cref="RocksDbVersioned"/> is a helper class to build a <see cref="Materialized{K, V, S}"/> for a RocksDB
    /// versioned key-value store (KIP-889). Versioned stores are supported for source KTables (<c>StreamBuilder.Table</c>).
    /// </summary>
    public static class RocksDbVersioned
    {
        /// <summary>
        /// Materialize a RocksDB versioned key-value store with the given name.
        /// </summary>
        /// <typeparam name="K">key type</typeparam>
        /// <typeparam name="V">value type</typeparam>
        /// <param name="storeName">the name of the state store</param>
        /// <param name="historyRetention">how long the older versions are kept; it's also the grace period of the store</param>
        /// <param name="segmentInterval">size of the segments of older versions, in milliseconds (default: max(history retention / 3, 2 seconds))</param>
        /// <returns>a new materialized instance with the given storeName</returns>
        public static Materialized<K, V, IKeyValueStore<Bytes, byte[]>> @As<K, V>(string storeName, TimeSpan historyRetention, long? segmentInterval = null)
            => Materialized<K, V, IKeyValueStore<Bytes, byte[]>>
                .Create(new RocksDbVersionedKeyValueBytesStoreSupplier(storeName, historyRetention, segmentInterval))
                .WithName(storeName);

        /// <summary>
        /// Materialize a RocksDB versioned key-value store with the given name.
        /// </summary>
        /// <typeparam name="K">key type</typeparam>
        /// <typeparam name="V">value type</typeparam>
        /// <typeparam name="KS">New serializer for <typeparamref name="K"/> type</typeparam>
        /// <typeparam name="VS">New serializer for <typeparamref name="V"/> type</typeparam>
        /// <param name="storeName">the name of the state store</param>
        /// <param name="historyRetention">how long the older versions are kept; it's also the grace period of the store</param>
        /// <param name="segmentInterval">size of the segments of older versions, in milliseconds (default: max(history retention / 3, 2 seconds))</param>
        /// <returns>a new materialized instance with the given storeName</returns>
        public static Materialized<K, V, IKeyValueStore<Bytes, byte[]>> @As<K, V, KS, VS>(string storeName, TimeSpan historyRetention, long? segmentInterval = null)
            where KS : ISerDes<K>, new()
            where VS : ISerDes<V>, new()
            => Materialized<K, V, IKeyValueStore<Bytes, byte[]>>
                .Create<KS, VS>(new RocksDbVersionedKeyValueBytesStoreSupplier(storeName, historyRetention, segmentInterval))
                .WithName(storeName);
    }
}
