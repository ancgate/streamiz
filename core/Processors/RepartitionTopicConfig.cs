using System.Collections.Generic;
using Streamiz.Kafka.Net.Crosscutting;

namespace Streamiz.Kafka.Net.Processors
{
    internal class RepartitionTopicConfig : InternalTopicConfig
    {
        /// <summary>
        /// Topics which must have the same number of partitions as this one (e.g. the subscription topics of a
        /// foreign-key join). When it is set, the number of partitions comes from these topics instead of the upstream ones.
        /// </summary>
        internal IList<string> CopartitionedWithTopics { get; set; } = new List<string>();

        public static IDictionary<string, string> REPARTITION_TOPIC_DEFAULT_OVERRIDES = new Dictionary<string, string>(INTERNAL_TOPIC_DEFAULT_OVERRIDES)
        {
            { InternalTopicConfigCst.CLEANUP_POLICY_CONFIG, "delete"},
            { InternalTopicConfigCst.SEGMENT_BYTES_CONFIG, "52428800"}, // 50MB
            { InternalTopicConfigCst.RETENTION_MS_CONFIG, "-1"}, // Infinity
        };
        
        public override IDictionary<string, string> GetProperties(IDictionary<string, string> defaultConfigs, long additionalRetentionMs)
        {
            IDictionary<string, string> topicConfig = new Dictionary<string, string>(REPARTITION_TOPIC_DEFAULT_OVERRIDES);
            topicConfig.AddRange(defaultConfigs);
            topicConfig.AddRange(Configs);
            return topicConfig;
        }
    }
}