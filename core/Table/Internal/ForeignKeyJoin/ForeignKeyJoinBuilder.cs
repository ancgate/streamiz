using System;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.SerDes.Internal;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.State.Internal;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Stream.Internal;
using Streamiz.Kafka.Net.Table.Internal.Graph;
using Streamiz.Kafka.Net.Table.Internal.Graph.Nodes;

namespace Streamiz.Kafka.Net.Table.Internal.ForeignKeyJoin
{
    /// <summary>
    /// Builds a KTable-KTable foreign-key join (KIP-213), like Kafka Streams' KTableImpl.doJoinOnForeignKey.
    /// </summary>
    internal static class ForeignKeyJoinBuilder
    {
        internal static IKTable<K, VR> Join<K, V, KO, VO, VR>(
            InternalStreamBuilder builder,
            IKTable<K, V> leftTable,
            IKTable<KO, VO> foreignKeyTable,
            Func<V, KO> foreignKeyExtractor,
            IValueJoiner<V, VO, VR> joiner,
            bool leftJoin,
            Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized,
            string named)
        {
            if (foreignKeyTable == null)
                throw new ArgumentNullException(nameof(foreignKeyTable));
            if (foreignKeyExtractor == null)
                throw new ArgumentNullException(nameof(foreignKeyExtractor));
            if (joiner == null)
                throw new ArgumentNullException(nameof(joiner));

            var left = (AbstractStream<K, V>) leftTable;
            var right = (AbstractStream<KO, VO>) foreignKeyTable;
            var leftGetter = (IKTableGetter<K, V>) leftTable;
            var rightGetter = (IKTableGetter<KO, VO>) foreignKeyTable;

            var renamed = new Named(named);
            var names = new ForeignKeyJoinNode<K, V, KO, VO, VR>.Names();

            names.RegistrationTopic = renamed.SuffixWithOrElseGet("-subscription-registration", builder,
                KTable.SUBSCRIPTION_REGISTRATION) + KTable.TOPIC_SUFFIX;
            // pseudo topics for the serialization of the keys and values inside the subscriptions (like Kafka Streams)
            string primaryKeySerdesTopic = names.RegistrationTopic + "-pk";
            string foreignKeySerdesTopic = names.RegistrationTopic + "-fk";
            string valueHashSerdesTopic = names.RegistrationTopic + "-vh";

            // the old foreign key of a left record is needed to unsubscribe it
            leftGetter.EnableSendingOldValues();

            names.SubscriptionSend = renamed.SuffixWithOrElseGet("-subscription-registration-processor", builder,
                KTable.SUBSCRIPTION_REGISTRATION);
            names.RegistrationSink = renamed.SuffixWithOrElseGet("-subscription-registration-sink", builder,
                KStream.SINK_NAME);
            names.RegistrationSource = renamed.SuffixWithOrElseGet("-subscription-registration-source", builder,
                KStream.SOURCE_NAME);
            names.SubscriptionStore = renamed.SuffixWithOrElseGet("-subscription-store", builder,
                KTable.FK_JOIN_STATE_STORE_NAME);
            names.SubscriptionReceive = renamed.SuffixWithOrElseGet("-subscription-receive", builder,
                KTable.SUBSCRIPTION_PROCESSOR);
            names.ForeignTableJoin = renamed.SuffixWithOrElseGet("-foreign-join-subscription", builder,
                KTable.SUBSCRIPTION_PROCESSOR);
            names.ResponseTopic = renamed.SuffixWithOrElseGet("-subscription-response", builder,
                KTable.SUBSCRIPTION_RESPONSE) + KTable.TOPIC_SUFFIX;
            string foreignValueSerdesTopic = names.ResponseTopic + "-fv";
            names.ResponseSink = renamed.SuffixWithOrElseGet("-subscription-response-sink", builder,
                KStream.SINK_NAME);
            names.ResponseSource = renamed.SuffixWithOrElseGet("-subscription-response-source", builder,
                KStream.SOURCE_NAME);
            names.ResponseJoin = renamed.SuffixWithOrElseGet("-subscription-response-resolver", builder,
                KTable.SUBSCRIPTION_RESPONSE_RESOLVER_PROCESSOR);
            names.Result = renamed.SuffixWithOrElseGet("-result", builder, KTable.FK_JOIN_OUTPUT_NAME);

            var primaryKeySerdes = left.KeySerdes;
            var leftValueSerdes = left.ValueSerdes;
            var foreignKeySerdes = right.KeySerdes;
            var foreignValueSerdes = right.ValueSerdes;

            var subscriptionSerdes = new SubscriptionWrapperSerDes<K>(primaryKeySerdesTopic, primaryKeySerdes);
            var responseSerdes = new SubscriptionResponseWrapperSerDes<VO>(foreignValueSerdesTopic, foreignValueSerdes);

            CombinedKeySchema<KO, K> NewKeySchema()
                => new(foreignKeySerdesTopic, foreignKeySerdes, primaryKeySerdesTopic, primaryKeySerdes);

            // subscription store: key = foreign key | primary key, without cache (like Kafka Streams)
            var subscriptionStoreBuilder = new TimestampedKeyValueStoreMaterializer<Bytes, SubscriptionWrapper<K>>(
                    Materialized<Bytes, SubscriptionWrapper<K>, IKeyValueStore<Bytes, byte[]>>
                        .Create(names.SubscriptionStore)
                        .WithKeySerdes(new BytesSerDes())
                        .WithValueSerdes(subscriptionSerdes)
                        .WithCachingDisabled())
                .Materialize();

            // result store: queryable if materialized is set, otherwise only built if a downstream operation needs it
            if (materialized != null)
                materialized.UseProvider(builder, KTable.FK_JOIN_OUTPUT_NAME);
            else
                materialized = Materialized<K, VR, IKeyValueStore<Bytes, byte[]>>.Create(
                    builder.NewStoreName(KTable.FK_JOIN_OUTPUT_NAME));

            if (materialized.KeySerdes == null)
                materialized.WithKeySerdes(primaryKeySerdes);

            var result = new KTableSource<K, VR>(materialized.StoreName, materialized.QueryableStoreName);
            var resultStoreBuilder = new TimestampedKeyValueStoreMaterializer<K, VR>(materialized).Materialize();

            var leftGetterSupplier = leftGetter.ValueGetterSupplier;
            var rightGetterSupplier = rightGetter.ValueGetterSupplier;

            var node = new ForeignKeyJoinNode<K, V, KO, VO, VR>(
                names,
                left.NameNode,
                right.NameNode,
                left.SetSourceNodes,
                right.SetSourceNodes,
                leftGetterSupplier.StoreNames,
                rightGetterSupplier.StoreNames,
                new WrappedProcessorSupplier<K, Change<V>>(() =>
                    new SubscriptionSendProcessor<K, V, KO>(foreignKeyExtractor, leftJoin,
                        foreignKeySerdesTopic, foreignKeySerdes, valueHashSerdesTopic, leftValueSerdes)),
                new WrappedProcessorSupplier<KO, SubscriptionWrapper<K>>(() =>
                    new SubscriptionReceiveProcessor<KO, VO, K>(names.SubscriptionStore, NewKeySchema(),
                        rightGetterSupplier.Get())),
                new WrappedProcessorSupplier<KO, Change<VO>>(() =>
                    new ForeignTableJoinProcessor<KO, VO, K>(names.SubscriptionStore, NewKeySchema())),
                new WrappedProcessorSupplier<K, SubscriptionResponseWrapper<VO>>(() =>
                    new ResponseJoinProcessor<K, V, VO, VR>(leftGetterSupplier.Get(), joiner, leftJoin,
                        valueHashSerdesTopic, leftValueSerdes)),
                result,
                foreignKeySerdes,
                subscriptionSerdes,
                primaryKeySerdes,
                responseSerdes,
                subscriptionStoreBuilder,
                resultStoreBuilder);

            builder.AddGraphNode(left.Node, node);
            // the right table is a parent too: the node is written once both tables are
            right.Node.AppendChild(node);

            return new KTable<K, VR, VR>(
                names.Result,
                materialized.KeySerdes,
                materialized.ValueSerdes,
                left.SetSourceNodes,
                materialized.QueryableStoreName,
                result,
                node,
                builder);
        }
    }
}
