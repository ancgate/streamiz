using Streamiz.Kafka.Net.Processors;
using System.Linq;
using System.Collections.Generic;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Processors.Internal;

namespace Streamiz.Kafka.Net.State.Internal
{
    internal class StreamThreadStateStoreProvider
    {
        private readonly IThread streamThread;
        private readonly InternalTopologyBuilder internalTopologyBuilder;

        public StreamThreadStateStoreProvider(IThread streamThread, InternalTopologyBuilder internalTopologyBuilder)
        {
            this.streamThread = streamThread;
            this.internalTopologyBuilder = internalTopologyBuilder;
        }

        public IEnumerable<T> Stores<T, K, V>(StoreQueryParameters<T, K, V> storeQueryParameters) 
            where T : class
        {
            if (this.streamThread.State == ThreadState.DEAD)
            {
                return Enumerable.Empty<T>();
            }

            // stale stores: the thread can be starting, rebalancing or restoring its tasks, as long as it's alive
            bool staleStoresAllowed = storeQueryParameters.StaleStores &&
                                      this.streamThread.State != ThreadState.PENDING_SHUTDOWN &&
                                      this.streamThread.State != ThreadState.CREATED;
            if (!(this.streamThread.State == ThreadState.RUNNING) && !staleStoresAllowed)
            {
                throw new InvalidStateStoreException($"Cannot get state store {storeQueryParameters.StoreName} because " +
                    $"the stream thread is {streamThread.State}, not RUNNING");
            }

            var tasks = streamThread.ActiveTasks;
            if (storeQueryParameters.Partition.HasValue)
                tasks = tasks.Where(t => t.Id.Partition == storeQueryParameters.Partition.Value);

            List<T> stores = new List<T>();
            foreach (var streamTask in tasks)
            {
                IStateStore store = streamTask.GetStore(storeQueryParameters.StoreName);
                if (store != null && storeQueryParameters.QueryableStoreType.Accepts(store))
                {
                    if (!store.IsOpen)
                    {
                        throw new InvalidStateStoreException($"Cannot get state store {storeQueryParameters.StoreName} for task {streamTask} because the " +
                            $"store is not open. The state store may have migrated to another instances.");
                    }
                    else if (store is ITimestampedWindowStore<K, V> windowStore && storeQueryParameters.QueryableStoreType is WindowStoreType<K, V>) {
                        T t = new ReadOnlyWindowStoreFacade<K, V>(windowStore) as T;
                        stores.Add(t);
                    }
                    if (store is ITimestampedKeyValueStore<K, V> valueStore && storeQueryParameters.QueryableStoreType is KeyValueStoreType<K, V>)
                    {
                        T t = new ReadOnlyKeyValueStoreFacade<K, V>(valueStore) as T;
                        stores.Add(t);
                    }
                    else if (store is T store1)
                        stores.Add(store1);
                }
            }
            return stores;
        }
    }
}