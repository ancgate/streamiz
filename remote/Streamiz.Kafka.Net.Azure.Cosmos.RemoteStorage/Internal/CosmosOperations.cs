using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using Microsoft.Azure.Cosmos;
using Streamiz.Kafka.Net.Errors;

namespace Streamiz.Kafka.Net.Azure.Cosmos.RemoteStorage.Internal
{
    /// <summary>
    /// <see cref="ICosmosOperations"/> over the Cosmos DB SDK. The store is synchronous, so each call waits for the SDK's task.
    /// Throttled requests (HTTP 429) are retried by the SDK (see <see cref="CosmosRemoteStorageOptions.MaxRetryAttemptsOnRateLimitedRequests"/>).
    /// </summary>
    internal class CosmosOperations : ICosmosOperations
    {
        internal const string PartitionKeyPath = "/t";

        private readonly CosmosRemoteStorageOptions options;
        private readonly string containerName;
        private readonly CosmosClient client;
        private Container container;

        public CosmosOperations(CosmosRemoteStorageOptions options, string containerName)
        {
            this.options = options;
            this.containerName = containerName;
            client = CreateClient(options);
            container = client.GetContainer(options.DatabaseName, containerName);
        }

        private static CosmosClient CreateClient(CosmosRemoteStorageOptions options)
        {
            var clientOptions = new CosmosClientOptions
            {
                ApplicationName = "streamiz",
                MaxRetryAttemptsOnRateLimitedRequests = options.MaxRetryAttemptsOnRateLimitedRequests
            };
            if (!string.IsNullOrEmpty(options.ConsistencyLevel))
                clientOptions.ConsistencyLevel = (ConsistencyLevel) Enum.Parse(typeof(ConsistencyLevel), options.ConsistencyLevel, true);
            options.ConfigureClientOptions?.Invoke(clientOptions);

            if (!string.IsNullOrEmpty(options.ConnectionString))
                return new CosmosClient(options.ConnectionString, clientOptions);
            if (!string.IsNullOrEmpty(options.AccountKey))
                return new CosmosClient(options.AccountEndpoint, options.AccountKey, clientOptions);
            return new CosmosClient(options.AccountEndpoint, options.Credential, clientOptions);
        }

        public void EnsureContainer()
        {
            var database = client.CreateDatabaseIfNotExistsAsync(options.DatabaseName)
                .GetAwaiter().GetResult().Database;
            // default indexing policy: every path has a range index, so ORDER BY c.k and the range filters on c.k use it
            container = database.CreateContainerIfNotExistsAsync(
                    new ContainerProperties(containerName, PartitionKeyPath), options.Throughput)
                .GetAwaiter().GetResult().Container;
        }

        public CosmosItem Read(string partition, string id)
        {
            try
            {
                return container.ReadItemAsync<CosmosItem>(id, new PartitionKey(partition))
                    .GetAwaiter().GetResult().Resource;
            }
            catch (CosmosException e) when (e.StatusCode == HttpStatusCode.NotFound)
            {
                return null;
            }
        }

        public void Upsert(CosmosItem item)
            => container.UpsertItemAsync(item, new PartitionKey(item.T)).GetAwaiter().GetResult();

        public void Delete(string partition, string id)
        {
            try
            {
                container.DeleteItemAsync<CosmosItem>(id, new PartitionKey(partition)).GetAwaiter().GetResult();
            }
            catch (CosmosException e) when (e.StatusCode == HttpStatusCode.NotFound)
            {
                // already deleted
            }
        }

        public CosmosItem ReadAndDelete(string partition, string id)
        {
            using var response = container.CreateTransactionalBatch(new PartitionKey(partition))
                .ReadItem(id)
                .DeleteItem(id)
                .ExecuteAsync()
                .GetAwaiter().GetResult();

            if (response.IsSuccessStatusCode)
                return response.GetOperationResultAtIndex<CosmosItem>(0).Resource;

            // the read fails when the item doesn't exist, then nothing is deleted
            if (response.Count > 0 && response[0].StatusCode == HttpStatusCode.NotFound)
                return null;

            throw new ProcessorStateException(
                $"Cosmos DB batch delete of item {id} failed in container {containerName}: {response.StatusCode} {response.ErrorMessage}");
        }

        public void UpsertBatch(string partition, IReadOnlyList<CosmosItem> items)
        {
            var batch = container.CreateTransactionalBatch(new PartitionKey(partition));
            foreach (var item in items)
                batch.UpsertItem(item);

            using var response = batch.ExecuteAsync().GetAwaiter().GetResult();
            if (!response.IsSuccessStatusCode)
                throw new ProcessorStateException(
                    $"Cosmos DB transactional batch of {items.Count} items failed in container {containerName}: {response.StatusCode} {response.ErrorMessage}");
        }

        public IEnumerable<CosmosItem> Query(string partition, string fromHex, string toHex, bool descending)
        {
            var sql = "SELECT c.k, c.v FROM c WHERE c.t = @t";
            if (fromHex != null)
                sql += " AND c.k >= @from";
            if (toHex != null)
                sql += " AND c.k <= @to";
            sql += descending ? " ORDER BY c.k DESC" : " ORDER BY c.k ASC";

            var query = new QueryDefinition(sql).WithParameter("@t", partition);
            if (fromHex != null)
                query = query.WithParameter("@from", fromHex);
            if (toHex != null)
                query = query.WithParameter("@to", toHex);

            return ReadAll<CosmosItem>(query, partition);
        }

        public long Count(string partition)
            => ReadAll<long>(new QueryDefinition("SELECT VALUE COUNT(1) FROM c"), partition).FirstOrDefault();

        // results are read page by page, when they are enumerated
        private IEnumerable<T> ReadAll<T>(QueryDefinition query, string partition)
        {
            using var iterator = container.GetItemQueryIterator<T>(query,
                requestOptions: new QueryRequestOptions {PartitionKey = new PartitionKey(partition)});
            while (iterator.HasMoreResults)
            {
                foreach (var item in iterator.ReadNextAsync().GetAwaiter().GetResult())
                    yield return item;
            }
        }

        public void Dispose()
            => client.Dispose();
    }
}
