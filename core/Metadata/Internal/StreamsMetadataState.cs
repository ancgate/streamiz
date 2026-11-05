using System;
using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.SerDes;

namespace Streamiz.Kafka.Net.Processors.Internal
{
    /// <summary>
    /// The metadata of all the instances of the application : the local instance (always up to date, from its threads) and the
    /// other instances, as read from the metadata topic.
    /// </summary>
    internal class StreamsMetadataState
    {
        private readonly object @lock = new();
        private readonly string localInstanceId;
        private readonly HostInfo localHost;
        private readonly Func<InstanceMetadata> localSnapshot;
        private readonly TimeSpan expiry;
        private readonly Func<string, int?> partitionCount;
        private readonly Func<long> now;
        private readonly IDictionary<int, (ISet<string> SourceTopics, ISet<string> StateStores)> subTopologies;
        private readonly ISet<string> globalStores;
        private readonly Dictionary<string, InstanceMetadata> remoteInstances = new();

        /// <param name="builder">topology of the application (the same on every instance)</param>
        /// <param name="localInstanceId">id of this instance</param>
        /// <param name="localHost">endpoint of this instance</param>
        /// <param name="localSnapshot">current metadata of this instance</param>
        /// <param name="expiry">an instance whose last metadata is older is considered dead</param>
        /// <param name="partitionCount">number of partitions of a topic, null if unknown</param>
        /// <param name="now">wall clock (unix ms)</param>
        internal StreamsMetadataState(
            InternalTopologyBuilder builder,
            string localInstanceId,
            HostInfo localHost,
            Func<InstanceMetadata> localSnapshot,
            TimeSpan expiry,
            Func<string, int?> partitionCount,
            Func<long> now = null)
        {
            this.localInstanceId = localInstanceId;
            this.localHost = localHost;
            this.localSnapshot = localSnapshot;
            this.expiry = expiry;
            this.partitionCount = partitionCount;
            this.now = now ?? (() => DateTime.Now.GetMilliseconds());
            subTopologies = builder.SubTopologiesSourceTopicsAndStores();
            globalStores = new HashSet<string>(builder.GlobalStateStoreNames);
        }

        /// <summary>
        /// A record of the metadata topic : the metadata of an instance, or a tombstone (null) when it stopped
        /// </summary>
        internal void OnRecord(string instanceId, InstanceMetadata metadata)
        {
            if (instanceId == null || instanceId == localInstanceId)
                return;

            lock (@lock)
            {
                if (metadata == null)
                    remoteInstances.Remove(instanceId);
                else if (metadata.Version <= InstanceMetadata.CURRENT_VERSION)
                    remoteInstances[instanceId] = metadata;
            }
        }

        /// <summary>
        /// The local instance and the remote instances which are alive
        /// </summary>
        private List<InstanceMetadata> Instances()
        {
            var instances = new List<InstanceMetadata>();
            var local = localSnapshot();
            if (local != null)
                instances.Add(local);

            long limit = now() - (long) expiry.TotalMilliseconds;
            lock (@lock)
            {
                instances.AddRange(remoteInstances.Values.Where(m => m.Timestamp >= limit));
            }

            return instances;
        }

        private HostInfo HostOf(InstanceMetadata instance)
            => instance.InstanceId == localInstanceId ? localHost : HostInfo.Parse(instance.ApplicationServer);

        private ISet<string> StoresOf(IEnumerable<TaskInfo> tasks)
            => new HashSet<string>(tasks.SelectMany(t =>
                subTopologies.TryGetValue(t.SubTopology, out var subTopology)
                    ? subTopology.StateStores
                    : Enumerable.Empty<string>()));

        private StreamsMetadata ToStreamsMetadata(InstanceMetadata instance)
            => new(
                instance.InstanceId,
                HostOf(instance),
                StoresOf(instance.ActiveTasks),
                instance.ActiveTasks.SelectMany(t => t.TopicPartitions).Select(t => t.ToTopicPartition()),
                StoresOf(instance.StandbyTasks),
                instance.StandbyTasks.SelectMany(t => t.TopicPartitions).Select(t => t.ToTopicPartition()));

        internal IEnumerable<StreamsMetadata> AllMetadata()
            => Instances().Select(ToStreamsMetadata).ToList();

        internal IEnumerable<StreamsMetadata> AllMetadataForStore(string storeName)
        {
            // global stores are on every instance
            if (globalStores.Contains(storeName))
                return AllMetadata();

            return AllMetadata()
                .Where(m => m.StateStoreNames.Contains(storeName) || m.StandbyStateStoreNames.Contains(storeName))
                .ToList();
        }

        internal KeyQueryMetadata QueryMetadataForKey<K>(string storeName, K key, ISerDes<K> keySerializer, Partitioner? partitioner)
        {
            if (keySerializer == null)
                throw new ArgumentNullException(nameof(keySerializer));

            return QueryMetadataForKey(storeName, (topic, numPartitions) =>
            {
                var keyBytes = keySerializer.Serialize(key, new SerializationContext(MessageComponentType.Key, topic));
                return KeyPartitioner.Partition(partitioner, keyBytes, numPartitions);
            });
        }

        internal KeyQueryMetadata QueryMetadataForKey<K>(string storeName, K key, Func<string, K, int, int> partitioner)
        {
            if (partitioner == null)
                throw new ArgumentNullException(nameof(partitioner));

            return QueryMetadataForKey(storeName, (topic, numPartitions) => partitioner(topic, key, numPartitions));
        }

        /// <param name="storeName">store</param>
        /// <param name="partitionFor">partition of the key for (source topic, number of partitions)</param>
        private KeyQueryMetadata QueryMetadataForKey(string storeName, Func<string, int, int?> partitionFor)
        {
            // a global store is on every instance
            if (globalStores.Contains(storeName))
                return new KeyQueryMetadata(localHost, new HashSet<HostInfo>(), -1);

            var subTopology = subTopologies.FirstOrDefault(s => s.Value.StateStores.Contains(storeName));
            if (subTopology.Value.StateStores == null)
                return KeyQueryMetadata.NotAvailable;

            // the source topics of a sub-topology are co-partitioned : the first one gives the partition
            var sourceTopic = subTopology.Value.SourceTopics.OrderBy(t => t).FirstOrDefault();
            if (sourceTopic == null)
                return KeyQueryMetadata.NotAvailable;

            var numPartitions = partitionCount(sourceTopic);
            if (!numPartitions.HasValue || numPartitions.Value <= 0)
                return KeyQueryMetadata.NotAvailable;

            var partition = partitionFor(sourceTopic, numPartitions.Value);
            if (!partition.HasValue || partition.Value < 0)
                return KeyQueryMetadata.NotAvailable;

            var instances = Instances();
            bool HasTask(IEnumerable<TaskInfo> tasks)
                => tasks.Any(t => t.SubTopology == subTopology.Key && t.Partition == partition.Value);

            var active = instances.FirstOrDefault(i => HasTask(i.ActiveTasks));
            var standbys = new HashSet<HostInfo>(instances
                .Where(i => HasTask(i.StandbyTasks))
                .Select(HostOf));

            return new KeyQueryMetadata(active != null ? HostOf(active) : HostInfo.Unavailable, standbys,
                partition.Value);
        }
    }
}
