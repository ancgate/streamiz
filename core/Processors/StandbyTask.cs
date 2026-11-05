using System;
using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Metrics;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.Processors.Public;
using Streamiz.Kafka.Net.Stream.Internal;

namespace Streamiz.Kafka.Net.Processors
{
    /// <summary>
    /// Standby replica of a task : a copy of its state stores (the ones backed by a changelog topic), kept up to date from the
    /// changelogs by the <see cref="StandbyTasksManager"/>. It processes no record. Its stores are in the state directory of
    /// the task, so when the task becomes active on this instance, its restoration resumes from the standby's checkpoint.
    /// </summary>
    internal class StandbyTask : AbstractTask
    {
        private readonly string threadId;
        private readonly StreamMetricsRegistry metricsRegistry;

        // records applied since the last checkpoint : only then the checkpoint changes
        private bool dirty;

        internal StandbyTask(
            string threadId,
            TaskId id,
            IEnumerable<TopicPartition> partitions,
            ProcessorTopology topology,
            IStreamConfig configuration,
            IChangelogRegister changelogRegister,
            StreamMetricsRegistry metricsRegistry)
            : base(id, partitions, topology, null, configuration, changelogRegister)
        {
            this.threadId = threadId;
            this.metricsRegistry = metricsRegistry;
            Context = new ProcessorContext(this, configuration, stateMgr, metricsRegistry);
        }

        /// <summary>
        /// Offset of the next changelog record to read for this changelog partition, null to start from the beginning
        /// </summary>
        internal long? NextOffset(TopicPartition changelogPartition)
        {
            var metadata = stateMgr.GetStoreMetadata(changelogPartition);
            if (metadata?.Offset == null)
                return null;
            // after loading the checkpoint, the offset is the next one to read; after applying records, the last one applied
            return appliedSinceLoad.Contains(changelogPartition) ? metadata.Offset.Value + 1 : metadata.Offset.Value;
        }

        private readonly HashSet<TopicPartition> appliedSinceLoad = new();

        /// <summary>
        /// Apply changelog records to the store of the changelog partition, through its (batch) restore callback
        /// </summary>
        internal void Apply(TopicPartition changelogPartition, IReadOnlyList<ConsumeResult<byte[], byte[]>> records)
        {
            if (IsClosed || records.Count == 0)
                return;

            var metadata = stateMgr.GetStoreMetadata(changelogPartition);
            if (metadata == null)
                return;

            // as when restoring, records with a null key are skipped
            var withKey = records.Where(r => r.Message?.Key != null).ToList();
            stateMgr.Restore(metadata, withKey);
            metadata.Offset = records[records.Count - 1].Offset.Value;
            appliedSinceLoad.Add(changelogPartition);
            dirty = true;
        }

        /// <summary>
        /// Flush the stores and write the checkpoint, if records were applied since the last one
        /// </summary>
        internal void Checkpoint()
        {
            if (!dirty || IsClosed)
                return;

            WriteCheckpoint(true);
            dirty = false;
        }

        #region AbstractTask

        public override IDictionary<TopicPartition, long> PurgeOffsets => new Dictionary<TopicPartition, long>();

        public override PartitionGrouper Grouper => null;

        public override bool CanProcess(long now) => false;

        public override long WallClockTime => DateTime.Now.GetMilliseconds();

        /// <summary>
        /// Open the stores backed by a changelog topic and load their checkpoint
        /// </summary>
        public override bool InitializeStateStores()
        {
            log.LogDebug("{LogPrefix}Initializing the state stores of the standby task", logPrefix);
            foreach (var kv in Topology.StateStores)
            {
                if (Topology.StoresToTopics.ContainsKey(kv.Key))
                    kv.Value.Init(Context, kv.Value);
            }

            stateMgr.InitializeOffsetsFromCheckpoint();
            TransitTo(TaskState.RUNNING);
            return true;
        }

        public override void Close(bool dirtyClose)
        {
            if (IsClosed)
                return;

            log.LogInformation("{LogPrefix}Closing standby task", logPrefix);
            try
            {
                if (!dirtyClose && state == TaskState.RUNNING)
                    Checkpoint();
            }
            finally
            {
                try
                {
                    CloseStateManager();
                }
                finally
                {
                    if (state != TaskState.SUSPENDED)
                        TransitTo(TaskState.SUSPENDED);
                    TransitTo(TaskState.CLOSED);
                    IsClosed = true;
                    metricsRegistry.RemoveTaskSensors(threadId, Id.ToString());
                }
            }
        }

        public override IEnumerable<TopicPartitionOffset> PrepareCommit() => Enumerable.Empty<TopicPartitionOffset>();

        public override void PostCommit(bool enforceCheckpoint)
        {
        }

        public override IStateStore GetStore(string name) => stateMgr.GetStore(name);

        public override void InitializeTopology()
        {
        }

        public override void RestorationIfNeeded()
        {
        }

        public override void Suspend()
        {
        }

        public override void MayWriteCheckpoint(bool force = false) => Checkpoint();

        public override TaskScheduled RegisterScheduleTask(TimeSpan interval, PunctuationType punctuationType,
            Action<long> punctuator)
            => throw new NotSupportedException("A standby task doesn't run punctuators");

        #endregion
    }
}
