using System;

namespace Streamiz.Kafka.Net.State.Cache.Internal
{
    /// <summary>
    /// An entry evicted from the cache, given to <see cref="MemoryCache{K,V}.BatchEvictionCallback"/>.
    /// </summary>
    internal readonly struct EvictedCacheEntry<K, V>
        where K : class, IComparable<K>
        where V : class
    {
        public EvictedCacheEntry(K key, V value, EvictionReason reason)
        {
            Key = key;
            Value = value;
            Reason = reason;
        }

        public K Key { get; }
        public V Value { get; }
        public EvictionReason Reason { get; }
    }
}
