using System.Collections.Generic;
using System.Linq;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Processors.Internal;

namespace Streamiz.Kafka.Net.State.Internal
{
    /// <summary>
    /// Stores of the standby replicas of this instance : they can only be queried with stale stores
    /// (<see cref="StoreQueryParameters{T,K,V}.EnableStaleStores"/>), since they may lag behind the active tasks.
    /// </summary>
    internal class StandbyTasksStateStoreProvider : StreamThreadStateStoreProvider
    {
        private readonly StandbyTasksManager standbyTasks;

        public StandbyTasksStateStoreProvider(StandbyTasksManager standbyTasks, InternalTopologyBuilder internalTopologyBuilder)
            : base(internalTopologyBuilder)
        {
            this.standbyTasks = standbyTasks;
        }

        public override IEnumerable<T> Stores<T, K, V>(StoreQueryParameters<T, K, V> storeQueryParameters)
        {
            if (!storeQueryParameters.StaleStores)
                return Enumerable.Empty<T>();

            return StoresOf(standbyTasks.StandbyTasks.Where(t => !t.IsClosed).Cast<ITask>(), storeQueryParameters);
        }
    }
}
