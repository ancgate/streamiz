using System;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Azure.Cosmos.RemoteStorage.Internal
{
    /// <summary>
    /// Calls made by <see cref="CosmosStore"/> to the container of a store (replaced by an in-memory container in tests)
    /// </summary>
    internal interface ICosmosOperations : IDisposable
    {
        /// <summary>
        /// Create the database and the container (partition key path <c>/t</c>) if they don't exist
        /// </summary>
        void EnsureContainer();

        /// <summary>
        /// The item, or null if it doesn't exist
        /// </summary>
        CosmosItem Read(string partition, string id);

        /// <summary>
        /// Create or replace the item
        /// </summary>
        void Upsert(CosmosItem item);

        /// <summary>
        /// Delete the item, nothing happens if it doesn't exist
        /// </summary>
        void Delete(string partition, string id);

        /// <summary>
        /// Read and delete the item in one transactional batch, null (and nothing deleted) if it doesn't exist
        /// </summary>
        CosmosItem ReadAndDelete(string partition, string id);

        /// <summary>
        /// Upsert the items (at most 100, all in <paramref name="partition"/>) in one transactional batch: all or none
        /// </summary>
        void UpsertBatch(string partition, IReadOnlyList<CosmosItem> items);

        /// <summary>
        /// Items of the partition with <paramref name="fromHex"/> &lt;= k &lt;= <paramref name="toHex"/> (a null bound is open),
        /// sorted by k, read page by page when enumerated
        /// </summary>
        IEnumerable<CosmosItem> Query(string partition, string fromHex, string toHex, bool descending);

        /// <summary>
        /// Number of items of the partition
        /// </summary>
        long Count(string partition);
    }
}
