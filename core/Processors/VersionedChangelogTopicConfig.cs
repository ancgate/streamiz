using System.Collections.Generic;
using Streamiz.Kafka.Net.Crosscutting;

namespace Streamiz.Kafka.Net.Processors
{
    /// <summary>
    /// Changelog topic of a versioned store: compacted, and the records are not compacted before the history retention
    /// (plus the additional retention) has passed, so the older versions can be restored.
    /// </summary>
    internal class VersionedChangelogTopicConfig : InternalTopicConfig
    {
        internal const string MIN_COMPACTION_LAG_MS_CONFIG = "min.compaction.lag.ms";

        public static IDictionary<string, string> VERSIONED_STORE_CHANGELOG_TOPIC_DEFAULT_OVERRIDES = new Dictionary<string, string>(INTERNAL_TOPIC_DEFAULT_OVERRIDES)
        {
            { InternalTopicConfigCst.CLEANUP_POLICY_CONFIG, "compact"}
        };

        public long HistoryRetentionMs { get; set; }

        public override IDictionary<string, string> GetProperties(IDictionary<string, string> defaultConfigs, long additionalRetentionMs)
        {
            IDictionary<string, string> topicConfig = new Dictionary<string, string>(VERSIONED_STORE_CHANGELOG_TOPIC_DEFAULT_OVERRIDES);
            topicConfig.AddRange(defaultConfigs);
            topicConfig.AddRange(Configs);

            if (Configs == null || !Configs.ContainsKey(MIN_COMPACTION_LAG_MS_CONFIG))
            {
                long minCompactionLag = HistoryRetentionMs > long.MaxValue - additionalRetentionMs
                    ? long.MaxValue
                    : HistoryRetentionMs + additionalRetentionMs;
                topicConfig.AddOrUpdate(MIN_COMPACTION_LAG_MS_CONFIG, minCompactionLag.ToString());
            }

            return topicConfig;
        }
    }
}
