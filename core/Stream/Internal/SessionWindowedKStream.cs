using System;
using System.Collections.Generic;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.State.Internal;
using Streamiz.Kafka.Net.Stream.Internal.Graph;
using Streamiz.Kafka.Net.Stream.Internal.Graph.Nodes;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Kafka.Net.Stream.Internal
{
    internal class SessionWindowedKStream<K, V> : AbstractStream<K, V>, ISessionWindowedKStream<K, V>
    {
        private readonly SessionWindowOptions windows;
        private readonly GroupedStreamAggregateBuilder<K, V> aggBuilder;

        public SessionWindowedKStream(SessionWindowOptions windows, GroupedStreamAggregateBuilder<K, V> aggBuilder, string name, ISerDes<K> keySerde, ISerDes<V> valSerde, List<string> sourceNodes, StreamGraphNode streamsGraphNode, InternalStreamBuilder builder)
            : base(name, keySerde, valSerde, sourceNodes, streamsGraphNode, builder)
        {
            CheckIfParamNull(windows, "windows");
            this.windows = windows;
            this.aggBuilder = aggBuilder;
        }

        #region Count

        // the count is nullable: the sessions merged into a new one are forwarded with a null count (a tombstone)
        public IKTable<Windowed<K>, long?> Count()
            => Count((string)null);

        public IKTable<Windowed<K>, long?> Count(string named)
            => Count(Materialized<K, long?, ISessionStore<Bytes, byte[]>>.Create(), named);

        public IKTable<Windowed<K>, long?> Count(Materialized<K, long?, ISessionStore<Bytes, byte[]>> materialized, string named = null)
        {
            materialized ??= Materialized<K, long?, ISessionStore<Bytes, byte[]>>.Create();

            if (materialized.ValueSerdes == null)
                materialized.WithValueSerdes(new NullableInt64SerDes());

            return DoAggregate(
                KGroupedStream.AGGREGATE_NAME,
                new WrappedInitializer<long?>(() => 0L),
                new WrappedAggregator<K, V, long?>((aggKey, value, aggregate) => (aggregate ?? 0L) + 1),
                new WrappedMerger<K, long?>((aggKey, aggOne, aggTwo) => (aggOne ?? 0L) + (aggTwo ?? 0L)),
                materialized,
                named);
        }

        #endregion

        #region Aggregate

        public IKTable<Windowed<K>, VR> Aggregate<VR>(Func<VR> initializer, Func<K, V, VR, VR> aggregator, Func<K, VR, VR, VR> sessionMerger)
            => Aggregate(new WrappedInitializer<VR>(initializer), new WrappedAggregator<K, V, VR>(aggregator), new WrappedMerger<K, VR>(sessionMerger));

        public IKTable<Windowed<K>, VR> Aggregate<VR>(Initializer<VR> initializer, Aggregator<K, V, VR> aggregator, Merger<K, VR> sessionMerger)
            => Aggregate(initializer, aggregator, sessionMerger, null);

        public IKTable<Windowed<K>, VR> Aggregate<VR, VRS>(Func<VR> initializer, Func<K, V, VR, VR> aggregator, Func<K, VR, VR, VR> sessionMerger) where VRS : ISerDes<VR>, new()
            => Aggregate<VR, VRS>(new WrappedInitializer<VR>(initializer), new WrappedAggregator<K, V, VR>(aggregator), new WrappedMerger<K, VR>(sessionMerger));

        public IKTable<Windowed<K>, VR> Aggregate<VR, VRS>(Initializer<VR> initializer, Aggregator<K, V, VR> aggregator, Merger<K, VR> sessionMerger) where VRS : ISerDes<VR>, new()
        {
            var materialized = Materialized<K, VR, ISessionStore<Bytes, byte[]>>.Create().WithValueSerdes(new VRS()).WithKeySerdes(KeySerdes);
            return Aggregate(initializer, aggregator, sessionMerger, materialized);
        }

        public IKTable<Windowed<K>, VR> Aggregate<VR>(Func<VR> initializer, Func<K, V, VR, VR> aggregator, Func<K, VR, VR, VR> sessionMerger, Materialized<K, VR, ISessionStore<Bytes, byte[]>> materialized, string named = null)
            => Aggregate(new WrappedInitializer<VR>(initializer), new WrappedAggregator<K, V, VR>(aggregator), new WrappedMerger<K, VR>(sessionMerger), materialized, named);

        public IKTable<Windowed<K>, VR> Aggregate<VR>(Initializer<VR> initializer, Aggregator<K, V, VR> aggregator, Merger<K, VR> sessionMerger, Materialized<K, VR, ISessionStore<Bytes, byte[]>> materialized, string named = null)
        {
            CheckIfParamNull(initializer, "initializer");
            CheckIfParamNull(aggregator, "aggregator");
            CheckIfParamNull(sessionMerger, "sessionMerger");

            materialized ??= Materialized<K, VR, ISessionStore<Bytes, byte[]>>.Create();

            return DoAggregate(KGroupedStream.AGGREGATE_NAME, initializer, aggregator, sessionMerger, materialized, named);
        }

        #endregion

        #region Reduce

        public IKTable<Windowed<K>, V> Reduce(Reducer<V> reducer)
            => Reduce(reducer, null);

        public IKTable<Windowed<K>, V> Reduce(Func<V, V, V> reducer)
            => Reduce(new WrappedReducer<V>(reducer));

        public IKTable<Windowed<K>, V> Reduce(Reducer<V> reducer, Materialized<K, V, ISessionStore<Bytes, byte[]>> materialized, string named = null)
        {
            CheckIfParamNull(reducer, "reducer");

            materialized ??= Materialized<K, V, ISessionStore<Bytes, byte[]>>.Create();

            if (materialized.ValueSerdes == null)
                materialized.WithValueSerdes(ValueSerdes);

            var reduceAggregator = new WrappedAggregator<K, V, V>((aggKey, value, agg) => agg == null ? value : reducer.Apply(agg, value));

            return DoAggregate(
                KGroupedStream.REDUCE_NAME,
                new WrappedInitializer<V>(() => default),
                reduceAggregator,
                // merging two sessions reduces the second aggregate into the first one
                new WrappedMerger<K, V>((aggKey, aggOne, aggTwo) => reduceAggregator.Apply(aggKey, aggTwo, aggOne)),
                materialized,
                named);
        }

        public IKTable<Windowed<K>, V> Reduce(Func<V, V, V> reducer, Materialized<K, V, ISessionStore<Bytes, byte[]>> materialized, string named = null)
            => Reduce(new WrappedReducer<V>(reducer), materialized, named);

        #endregion

        #region Privates

        private IKTable<Windowed<K>, VR> DoAggregate<VR>(
            string prefix,
            Initializer<VR> initializer,
            Aggregator<K, V, VR> aggregator,
            Merger<K, VR> sessionMerger,
            Materialized<K, VR, ISessionStore<Bytes, byte[]>> materialized,
            string named)
        {
            if (materialized.KeySerdes == null)
                materialized.WithKeySerdes(KeySerdes);

            string name = new Named(named).OrElseGenerateWithPrefix(builder, prefix);
            materialized.UseProvider(builder, prefix);

            var aggSupplier = new KStreamSessionWindowAggregate<K, V, VR>(
                windows,
                materialized.StoreName,
                initializer,
                aggregator,
                sessionMerger);

            ISerDes<Windowed<K>> sessionSerdes = materialized.KeySerdes != null ? new SessionWindowedSerDes<K>(materialized.KeySerdes) : null;

            return aggBuilder.BuildWindow(name,
                new SessionStoreMaterializer<K, VR>(windows.InactivityGapMs, windows.GracePeriodMs, materialized).Materialize(),
                aggSupplier,
                materialized.QueryableStoreName,
                sessionSerdes,
                materialized.ValueSerdes);
        }

        #endregion
    }
}
