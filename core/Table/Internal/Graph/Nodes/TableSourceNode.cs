using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.State.Internal;
using Streamiz.Kafka.Net.Stream.Internal;
using Streamiz.Kafka.Net.Stream.Internal.Graph.Nodes;
using System;

namespace Streamiz.Kafka.Net.Table.Internal.Graph.Nodes
{
    internal interface ITableSourceNode
    {
        string SourceName { get; }
        string NodeName { get; }

        /// <summary>
        /// History retention of the table's versioned store, null if the table is not versioned
        /// </summary>
        long? VersionedHistoryRetentionMs { get; }
    }

    internal class TableSourceNode<K, V, S> : StreamSourceNode<K, V>, ITableSourceNode
        where S : IStateStore
    {
        private readonly Materialized<K, V, S> materialized;
        private readonly ProcessorParameters<K, V> processorParameters;
        private readonly String sourceName;
        private readonly bool isGlobalKTable;
        private bool shouldReuseSourceTopicForChangelog = false;

        public TableSourceNode(string topicName, string streamGraphNode,
                string sourceName, ConsumedInternal<K, V> consumed,
                Materialized<K, V, S> materialized, ProcessorParameters<K, V> processorParameters, bool isGlobalKTable = false)
            : base(topicName, streamGraphNode, consumed)
        {
            this.materialized = materialized;
            this.processorParameters = processorParameters;
            this.sourceName = sourceName;
            this.isGlobalKTable = isGlobalKTable;
        }

        public string SourceName => sourceName;

        public string NodeName => this.streamGraphNode;

        public long? VersionedHistoryRetentionMs =>
            (materialized as Materialized<K, V, IKeyValueStore<Bytes, byte[]>>)?.StoreSupplier is State.Supplier.IVersionedBytesStoreSupplier supplier
                ? supplier.HistoryRetentionMs
                : null;

        public void ReuseSourceTopicForChangeLog(bool shouldReuseSourceTopicForChangelog)
        {
            this.shouldReuseSourceTopicForChangelog = shouldReuseSourceTopicForChangelog;
        }

        public override string ToString()
        {
            return "TableSourceNode{" +
                   "materializedInternal=" + materialized +
                   ", processorParameters=" + processorParameters +
                   ", sourceName='" + sourceName + '\'' +
                   ", isGlobalKTable=" + isGlobalKTable +
                   "} " + base.ToString();
        }

        private static void DisableLogging(IStoreBuilder storeBuilder)
        {
            switch (storeBuilder)
            {
                case IStoreBuilder<State.ITimestampedKeyValueStore<K, V>> timestamped:
                    timestamped.WithLoggingDisabled();
                    break;
                case IStoreBuilder<IVersionedKeyValueStore<K, V>> versioned:
                    versioned.WithLoggingDisabled();
                    break;
            }
        }

        public override void WriteToTopology(InternalTopologyBuilder builder)
        {
            // source KTables are timestamped key-value stores, or versioned key-value stores (KIP-889)
            var keyValueMaterialized = materialized as Materialized<K, V, IKeyValueStore<Bytes, byte[]>>;
            IStoreBuilder storeBuilder;
            if (VersionedKeyValueStoreMaterializer<K, V>.IsVersioned(keyValueMaterialized))
            {
                if (isGlobalKTable)
                    throw new TopologyException($"Versioned stores are not supported for global tables (store {keyValueMaterialized.StoreName})");
                storeBuilder = new VersionedKeyValueStoreMaterializer<K, V>(keyValueMaterialized).Materialize();
            }
            else
                storeBuilder = new TimestampedKeyValueStoreMaterializer<K, V>(keyValueMaterialized).Materialize();

            if (isGlobalKTable)
            {
                builder.AddGlobalStore(topicName, (IStoreBuilder<State.ITimestampedKeyValueStore<K, V>>) storeBuilder, sourceName, consumed, processorParameters, false);
            }
            else
            {
                builder.AddSourceOperator(topicName, sourceName, consumed);
                builder.AddProcessor(processorParameters.ProcessorName, processorParameters.Processor, sourceName);

                //// only add state store if the source KTable should be materialized
                KTableSource<K, V> ktableSource = (KTableSource<K, V>)processorParameters.Processor;
                if (ktableSource.QueryableName != null)
                {
                    builder.AddStateStore(storeBuilder, this.streamGraphNode);

                    if (shouldReuseSourceTopicForChangelog)
                    {
                        DisableLogging(storeBuilder);
                        builder.ConnectSourceStoreAndTopic(storeBuilder.Name, topicName);
                    }
                }
            }
        }
    }
}