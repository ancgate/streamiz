using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka;

namespace Streamiz.Kafka.Net
{
    /// <summary>
    /// What a Streamiz instance hosts : its endpoint (<see cref="IStreamConfig.ApplicationServer"/>), the state stores and the
    /// partitions of its active tasks, and those of its standby tasks.
    /// </summary>
    public sealed class StreamsMetadata
    {
        internal StreamsMetadata(
            string processId,
            HostInfo hostInfo,
            IEnumerable<string> stateStoreNames,
            IEnumerable<TopicPartition> topicPartitions,
            IEnumerable<string> standbyStateStoreNames,
            IEnumerable<TopicPartition> standbyTopicPartitions)
        {
            ProcessId = processId;
            HostInfo = hostInfo;
            StateStoreNames = new HashSet<string>(stateStoreNames);
            TopicPartitions = new HashSet<TopicPartition>(topicPartitions);
            StandbyStateStoreNames = new HashSet<string>(standbyStateStoreNames);
            StandbyTopicPartitions = new HashSet<TopicPartition>(standbyTopicPartitions);
        }

        /// <summary>
        /// Id of the instance (its process id)
        /// </summary>
        public string ProcessId { get; }

        /// <summary>
        /// Endpoint of the instance, <see cref="HostInfo.Unavailable"/> if it has no <see cref="IStreamConfig.ApplicationServer"/>
        /// </summary>
        public HostInfo HostInfo { get; }

        /// <summary>
        /// State stores of the active tasks of the instance (global stores excluded)
        /// </summary>
        public ISet<string> StateStoreNames { get; }

        /// <summary>
        /// Source partitions of the active tasks of the instance
        /// </summary>
        public ISet<TopicPartition> TopicPartitions { get; }

        /// <summary>
        /// State stores of the standby tasks of the instance
        /// </summary>
        public ISet<string> StandbyStateStoreNames { get; }

        /// <summary>
        /// Source partitions of the standby tasks of the instance
        /// </summary>
        public ISet<TopicPartition> StandbyTopicPartitions { get; }

        /// <inheritdoc/>
        public override string ToString()
            => $"StreamsMetadata {{ProcessId={ProcessId}, HostInfo={HostInfo}, StateStoreNames=[{string.Join(",", StateStoreNames.OrderBy(s => s))}], " +
               $"TopicPartitions=[{string.Join(",", TopicPartitions.Select(t => $"{t.Topic}-{t.Partition.Value}").OrderBy(s => s))}], " +
               $"StandbyStateStoreNames=[{string.Join(",", StandbyStateStoreNames.OrderBy(s => s))}], " +
               $"StandbyTopicPartitions=[{string.Join(",", StandbyTopicPartitions.Select(t => $"{t.Topic}-{t.Partition.Value}").OrderBy(s => s))}]}}";
    }

    /// <summary>
    /// Where a key of a state store is hosted : the instance of the active task, the instances of its standby tasks, and the
    /// partition of the key.
    /// </summary>
    public sealed class KeyQueryMetadata
    {
        /// <summary>
        /// Returned when the metadata is not available (not running yet, rebalancing, unknown store, partition count unknown)
        /// </summary>
        public static readonly KeyQueryMetadata NotAvailable =
            new(HostInfo.Unavailable, new HashSet<HostInfo>(), -1);

        internal KeyQueryMetadata(HostInfo activeHost, ISet<HostInfo> standbyHosts, int partition)
        {
            ActiveHost = activeHost;
            StandbyHosts = standbyHosts;
            Partition = partition;
        }

        /// <summary>
        /// Instance of the active task which hosts the key, <see cref="HostInfo.Unavailable"/> if it's unknown
        /// </summary>
        public HostInfo ActiveHost { get; }

        /// <summary>
        /// Instances of the standby tasks which host the key
        /// </summary>
        public ISet<HostInfo> StandbyHosts { get; }

        /// <summary>
        /// Partition of the key, -1 if it's unknown (or for a global store, which is on every instance)
        /// </summary>
        public int Partition { get; }

        /// <inheritdoc/>
        public override string ToString()
            => $"KeyQueryMetadata {{ActiveHost={ActiveHost}, StandbyHosts=[{string.Join(",", StandbyHosts)}], Partition={Partition}}}";
    }

    /// <summary>
    /// A task of a local stream thread
    /// </summary>
    public sealed class TaskMetadata
    {
        internal TaskMetadata(string taskId, IEnumerable<TopicPartition> topicPartitions)
        {
            TaskId = taskId;
            TopicPartitions = new HashSet<TopicPartition>(topicPartitions);
        }

        /// <summary>
        /// Id of the task (sub-topology id - partition)
        /// </summary>
        public string TaskId { get; }

        /// <summary>
        /// Source partitions of the task
        /// </summary>
        public ISet<TopicPartition> TopicPartitions { get; }
    }

    /// <summary>
    /// A local stream thread and its tasks
    /// </summary>
    public sealed class ThreadMetadata
    {
        internal ThreadMetadata(string threadName, string threadState, IEnumerable<TaskMetadata> activeTasks,
            IEnumerable<TaskMetadata> standbyTasks)
        {
            ThreadName = threadName;
            ThreadState = threadState;
            ActiveTasks = activeTasks.ToList();
            StandbyTasks = standbyTasks.ToList();
        }

        /// <summary>
        /// Name of the thread
        /// </summary>
        public string ThreadName { get; }

        /// <summary>
        /// State of the thread
        /// </summary>
        public string ThreadState { get; }

        /// <summary>
        /// Active tasks of the thread
        /// </summary>
        public IReadOnlyCollection<TaskMetadata> ActiveTasks { get; }

        /// <summary>
        /// Standby tasks of the thread
        /// </summary>
        public IReadOnlyCollection<TaskMetadata> StandbyTasks { get; }
    }
}
