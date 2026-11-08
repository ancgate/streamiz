using System.Collections.Generic;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Stream.Internal;
using Streamiz.Kafka.Net.Stream.Internal.Graph.Nodes;
using Streamiz.Kafka.Net.Table.Internal.ForeignKeyJoin;
using Streamiz.Kafka.Net.Table.Internal.Graph;

namespace Streamiz.Kafka.Net.Table.Internal.Graph.Nodes
{
    /// <summary>
    /// Writes the whole foreign-key join sub-topology (KIP-213): its parents are the left and right tables, and its name
    /// is the name of the result processor, so the downstream processors of the result table attach to it.
    /// </summary>
    internal class ForeignKeyJoinNode<K, V, KO, VO, VR> : StreamGraphNode
    {
        internal class Names
        {
            public string SubscriptionSend { get; set; }
            public string RegistrationTopic { get; set; }
            public string RegistrationSink { get; set; }
            public string RegistrationSource { get; set; }
            public string SubscriptionStore { get; set; }
            public string SubscriptionReceive { get; set; }
            public string ForeignTableJoin { get; set; }
            public string ResponseTopic { get; set; }
            public string ResponseSink { get; set; }
            public string ResponseSource { get; set; }
            public string ResponseJoin { get; set; }
            public string Result { get; set; }
        }

        private readonly Names names;
        private readonly string leftNodeName;
        private readonly string rightNodeName;
        private readonly IEnumerable<string> leftSourceNodes;
        private readonly IEnumerable<string> rightSourceNodes;
        private readonly string[] leftStoreNames;
        private readonly string[] rightStoreNames;

        private readonly IProcessorSupplier<K, Change<V>> subscriptionSend;
        private readonly IProcessorSupplier<KO, SubscriptionWrapper<K>> subscriptionReceive;
        private readonly IProcessorSupplier<KO, Change<VO>> foreignTableJoin;
        private readonly IProcessorSupplier<K, SubscriptionResponseWrapper<VO>> responseJoin;
        private readonly KTableSource<K, VR> result;

        private readonly ISerDes<KO> foreignKeySerdes;
        private readonly ISerDes<SubscriptionWrapper<K>> subscriptionSerdes;
        private readonly ISerDes<K> primaryKeySerdes;
        private readonly ISerDes<SubscriptionResponseWrapper<VO>> responseSerdes;

        private readonly IStoreBuilder subscriptionStoreBuilder;
        private readonly IStoreBuilder resultStoreBuilder;

        public ForeignKeyJoinNode(
            Names names,
            string leftNodeName,
            string rightNodeName,
            IEnumerable<string> leftSourceNodes,
            IEnumerable<string> rightSourceNodes,
            string[] leftStoreNames,
            string[] rightStoreNames,
            IProcessorSupplier<K, Change<V>> subscriptionSend,
            IProcessorSupplier<KO, SubscriptionWrapper<K>> subscriptionReceive,
            IProcessorSupplier<KO, Change<VO>> foreignTableJoin,
            IProcessorSupplier<K, SubscriptionResponseWrapper<VO>> responseJoin,
            KTableSource<K, VR> result,
            ISerDes<KO> foreignKeySerdes,
            ISerDes<SubscriptionWrapper<K>> subscriptionSerdes,
            ISerDes<K> primaryKeySerdes,
            ISerDes<SubscriptionResponseWrapper<VO>> responseSerdes,
            IStoreBuilder subscriptionStoreBuilder,
            IStoreBuilder resultStoreBuilder)
            : base(names.Result)
        {
            this.names = names;
            this.leftNodeName = leftNodeName;
            this.rightNodeName = rightNodeName;
            this.leftSourceNodes = leftSourceNodes;
            this.rightSourceNodes = rightSourceNodes;
            this.leftStoreNames = leftStoreNames;
            this.rightStoreNames = rightStoreNames;
            this.subscriptionSend = subscriptionSend;
            this.subscriptionReceive = subscriptionReceive;
            this.foreignTableJoin = foreignTableJoin;
            this.responseJoin = responseJoin;
            this.result = result;
            this.foreignKeySerdes = foreignKeySerdes;
            this.subscriptionSerdes = subscriptionSerdes;
            this.primaryKeySerdes = primaryKeySerdes;
            this.responseSerdes = responseSerdes;
            this.subscriptionStoreBuilder = subscriptionStoreBuilder;
            this.resultStoreBuilder = resultStoreBuilder;
        }

        public override void WriteToTopology(InternalTopologyBuilder builder)
        {
            // left table -> subscription registration topic (keyed by foreign key), co-partitioned with the right table
            builder.AddProcessor(names.SubscriptionSend, subscriptionSend, leftNodeName);
            builder.AddInternalTopic(names.RegistrationTopic, null, rightSourceNodes);
            builder.AddSinkOperator(
                new StaticTopicNameExtractor<KO, SubscriptionWrapper<K>>(names.RegistrationTopic),
                new DefaultRecordTimestampExtractor<KO, SubscriptionWrapper<K>>(),
                names.RegistrationSink,
                Produced<KO, SubscriptionWrapper<K>>.Create(foreignKeySerdes, subscriptionSerdes),
                names.SubscriptionSend);
            builder.AddSourceOperator(
                names.RegistrationTopic,
                names.RegistrationSource,
                new ConsumedInternal<KO, SubscriptionWrapper<K>>(names.RegistrationSource, foreignKeySerdes,
                    subscriptionSerdes, new FailOnInvalidTimestamp()));

            // right side: subscriptions and right table changes, both use the subscription store
            builder.AddProcessor(names.SubscriptionReceive, subscriptionReceive, names.RegistrationSource);
            builder.AddProcessor(names.ForeignTableJoin, foreignTableJoin, rightNodeName);
            builder.AddStateStore(subscriptionStoreBuilder, names.SubscriptionReceive, names.ForeignTableJoin);
            builder.ConnectProcessorAndStateStore(names.SubscriptionReceive, rightStoreNames);

            // right side -> subscription response topic (keyed by primary key), co-partitioned with the left table;
            // each response goes back to the partition of the left record which sent the subscription
            builder.AddInternalTopic(names.ResponseTopic, null, leftSourceNodes);
            builder.AddSinkOperator(
                new StaticTopicNameExtractor<K, SubscriptionResponseWrapper<VO>>(names.ResponseTopic),
                new DefaultRecordTimestampExtractor<K, SubscriptionResponseWrapper<VO>>(),
                names.ResponseSink,
                Produced<K, SubscriptionResponseWrapper<VO>>.Create(primaryKeySerdes, responseSerdes)
                    .WithPartitioner(new SubscriptionResponsePartitioner<K, VO>()),
                names.SubscriptionReceive, names.ForeignTableJoin);
            builder.AddSourceOperator(
                names.ResponseTopic,
                names.ResponseSource,
                new ConsumedInternal<K, SubscriptionResponseWrapper<VO>>(names.ResponseSource, primaryKeySerdes,
                    responseSerdes, new FailOnInvalidTimestamp()));

            // left side: join the responses with the current left values, then the result table
            builder.AddProcessor(names.ResponseJoin, responseJoin, names.ResponseSource);
            builder.ConnectProcessorAndStateStore(names.ResponseJoin, leftStoreNames);
            builder.AddProcessor(names.Result, result, names.ResponseJoin);

            // like a source table: the result is materialized if it is queryable or if a downstream operation needs it
            if (result.QueryableName != null)
                builder.AddStateStore(resultStoreBuilder, names.Result);
        }
    }
}
