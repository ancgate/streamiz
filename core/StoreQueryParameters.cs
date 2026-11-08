using Streamiz.Kafka.Net.State.Internal;

namespace Streamiz.Kafka.Net
{
    /// <summary>
    /// Allows you to create <see cref="StoreQueryParameters{T, K, V}"/> you can later use to  pass a variety of parameters when fetching a store for interactive query.
    /// </summary>
    public static class StoreQueryParameters
    {
        /// <summary>
        /// Creates <see cref="StoreQueryParameters{T, K, V}"/> with specified storeName and queryableStoreType
        /// </summary>
        /// <typeparam name="T">The type of the Store to be fetched</typeparam>
        /// <typeparam name="K">The type of key</typeparam>
        /// <typeparam name="V">The type of value</typeparam>
        /// <param name="storeName">The name of the state store that should be queried.</param>
        /// <param name="queryableStoreType">The <see cref="IQueryableStoreType{T, K, V}"/> for which key is queried by the user.</param>
        /// <returns><see cref="StoreQueryParameters{T, K, V}"/> with provided <code>name</code> and <code>queryableStoreType</code></returns>
        public static StoreQueryParameters<T, K, V> FromNameAndType<T, K, V>(string storeName, IQueryableStoreType<T, K, V> queryableStoreType) where T : class
        {
            return new StoreQueryParameters<T, K, V>(storeName, queryableStoreType, null, false);
        }
    }

    /// <summary>
    /// Allows you to pass a variety of parameters when fetching a store for interactive query.
    /// </summary>
    /// <typeparam name="T">The type of the Store to be fetched</typeparam>
    /// <typeparam name="K">Key type</typeparam>
    /// <typeparam name="V">Value type</typeparam>
    public class StoreQueryParameters<T, K, V> 
        where T : class
    {
        /// <summary>
        /// The partition to query, or null to query all the partitions of the local instance.
        /// </summary>
        public int? Partition { get; private set; }

        /// <summary>
        /// True if the stores of tasks which are not running (e.g. restoring) can be queried, which can return stale data.
        /// </summary>
        public bool StaleStores { get; private set; }

        /// <summary>
        /// The name of the state store that should be queried.
        /// </summary>
        public string StoreName { get; private set; }

        /// <summary>
        /// The <see cref="IQueryableStoreType{T, K, V}"/> for which key is queried by the user.
        /// </summary>
        public IQueryableStoreType<T, K, V> QueryableStoreType { get; private set; }

        internal StoreQueryParameters(string storeName, IQueryableStoreType<T, K, V> queryableStoreType, int? partition, bool staleStores)
        {
            StoreName = storeName;
            QueryableStoreType = queryableStoreType;
            Partition = partition;
            StaleStores = staleStores;
        }

        /// <summary>
        /// Query only the store of the task of <paramref name="partition"/> (see <see cref="KafkaStream.QueryMetadataForKey{K}(string, K, SerDes.ISerDes{K})"/>
        /// to find the partition of a key).
        /// </summary>
        /// <param name="partition">The partition to query</param>
        /// <returns>New parameters with the partition</returns>
        public StoreQueryParameters<T, K, V> WithPartition(int partition)
        {
            return new StoreQueryParameters<T, K, V>(StoreName, QueryableStoreType, partition, StaleStores);
        }

        /// <summary>
        /// Allow querying the stores while the stream threads are not running (e.g. while a task restores its stores after
        /// a rebalance). The results can be stale.
        /// </summary>
        /// <returns>New parameters with stale stores enabled</returns>
        public StoreQueryParameters<T, K, V> EnableStaleStores()
        {
            return new StoreQueryParameters<T, K, V>(StoreName, QueryableStoreType, Partition, true);
        }
    }
}
