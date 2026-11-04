using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.State;

namespace Streamiz.Kafka.Net.Stream.Internal.Graph.Nodes
{
    internal class StreamTableJoinNode<K, V> : StreamGraphNode
    {
        private readonly ProcessorParameters<K, V> processorParameters;
        private readonly string[] storeNames;
        private readonly string otherJoinSideNodeName;
        private readonly IStoreBuilder bufferStoreBuilder;

        public StreamTableJoinNode(
                string streamGraphNode,
                ProcessorParameters<K, V> processorParameters,
                string[] storeNames,
                string otherJoinSideNodeName,
                IStoreBuilder bufferStoreBuilder = null) 
            : base(streamGraphNode)
        {
            this.processorParameters = processorParameters;
            this.storeNames = storeNames;
            this.otherJoinSideNodeName = otherJoinSideNodeName;
            this.bufferStoreBuilder = bufferStoreBuilder;
        }

        public override void WriteToTopology(InternalTopologyBuilder builder)
        {
            // Stream - Table join (Global or KTable)
            builder.AddProcessor(processorParameters.ProcessorName, processorParameters.Processor, ParentNodeNames());

            // Stream - KTable join only
            if (!string.IsNullOrEmpty(otherJoinSideNodeName))
            {
                builder.ConnectProcessorAndStateStore(processorParameters.ProcessorName, storeNames);
            }

            // KIP-923: buffer of the stream records during the grace period
            if (bufferStoreBuilder != null)
                builder.AddStateStore(bufferStoreBuilder, processorParameters.ProcessorName);
        }
    }
}
