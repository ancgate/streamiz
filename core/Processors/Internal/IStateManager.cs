using System;
using System.Collections.Generic;
using Confluent.Kafka;

namespace Streamiz.Kafka.Net.Processors.Internal
{
    internal interface IStateManager
    {
        IEnumerable<string> StateStoreNames { get; }
        ICollection<TopicPartition> ChangelogPartitions { get; }
        IDictionary<TopicPartition, long> ChangelogOffsets { get; }
        void Flush();
        void Register(IStateStore store, Action<ConsumeResult<byte[], byte[]>> callback);
        /// <summary>
        /// Register a store with a per-record restore callback and an optional batch restore callback.
        /// When the batch callback is set, it is used instead of the per-record one during restoration.
        /// </summary>
        void Register(IStateStore store, Action<ConsumeResult<byte[], byte[]>> callback,
            Action<IReadOnlyList<ConsumeResult<byte[], byte[]>>> batchCallback);
        void UpdateChangelogOffsets(IDictionary<TopicPartition, long> writtenOffsets);
        void Close();
        IStateStore GetStore(string name);
        TopicPartition GetRegisteredChangelogPartitionFor(string name);
        void InitializeOffsetsFromCheckpoint();
        void Checkpoint();
        string ChangelogFor(string storeName);
    }
}
