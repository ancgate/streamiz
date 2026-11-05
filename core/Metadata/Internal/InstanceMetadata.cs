using System.Collections.Generic;
using System.Linq;
using System.Text;
using Confluent.Kafka;
using Newtonsoft.Json;

namespace Streamiz.Kafka.Net.Processors.Internal
{
    /// <summary>
    /// What an instance publishes in the metadata topic, keyed by its instance id. JSON, with a version for evolutions.
    /// </summary>
    internal class InstanceMetadata
    {
        internal const int CURRENT_VERSION = 1;

        [JsonProperty("version")] public int Version { get; set; } = CURRENT_VERSION;
        [JsonProperty("instanceId")] public string InstanceId { get; set; }
        [JsonProperty("applicationServer")] public string ApplicationServer { get; set; }

        /// <summary>
        /// Wall clock time (unix ms) of the publication, used to expire the instances which stopped without a tombstone
        /// </summary>
        [JsonProperty("timestamp")] public long Timestamp { get; set; }

        [JsonProperty("threads")] public List<ThreadInfo> Threads { get; set; } = new();

        internal IEnumerable<TaskInfo> ActiveTasks => Threads.SelectMany(t => t.ActiveTasks);
        internal IEnumerable<TaskInfo> StandbyTasks => Threads.SelectMany(t => t.StandbyTasks);

        internal byte[] Serialize() => Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(this));

        internal static InstanceMetadata Deserialize(byte[] bytes)
            => bytes == null ? null : JsonConvert.DeserializeObject<InstanceMetadata>(Encoding.UTF8.GetString(bytes));

        /// <summary>
        /// Same content, ignoring the publication timestamp
        /// </summary>
        internal bool SameContent(InstanceMetadata other)
        {
            if (other == null)
                return false;
            var mine = JsonConvert.SerializeObject(new { InstanceId, ApplicationServer, Threads });
            var theirs = JsonConvert.SerializeObject(new { other.InstanceId, other.ApplicationServer, other.Threads });
            return mine == theirs;
        }
    }

    internal class ThreadInfo
    {
        [JsonProperty("threadName")] public string ThreadName { get; set; }
        [JsonProperty("state")] public string State { get; set; }
        [JsonProperty("activeTasks")] public List<TaskInfo> ActiveTasks { get; set; } = new();

        /// <summary>
        /// Empty for now, filled when standby replicas are supported
        /// </summary>
        [JsonProperty("standbyTasks")] public List<TaskInfo> StandbyTasks { get; set; } = new();
    }

    internal class TaskInfo
    {
        /// <summary>
        /// Sub-topology id
        /// </summary>
        [JsonProperty("subTopology")] public int SubTopology { get; set; }

        /// <summary>
        /// Partition of the task
        /// </summary>
        [JsonProperty("partition")] public int Partition { get; set; }

        [JsonProperty("topicPartitions")] public List<TaskTopicPartition> TopicPartitions { get; set; } = new();

        internal string TaskId => $"{SubTopology}-{Partition}";
    }

    internal class TaskTopicPartition
    {
        [JsonProperty("topic")] public string Topic { get; set; }
        [JsonProperty("partition")] public int Partition { get; set; }

        internal TopicPartition ToTopicPartition() => new(Topic, Partition);
    }
}
