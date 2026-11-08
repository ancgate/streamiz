using System;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Kafka.Net.Stream
{
    /// <summary>
    /// <see cref="ISessionWindowedKStream{K, V}"/> is an abstraction of a windowed record stream of key-value pairs, grouped by key
    /// and aggregated into sessions (see <see cref="SessionWindowOptions"/>).
    /// It is an intermediate representation after a grouping and windowing of a <see cref="IKStream{K, V}"/>
    /// before an aggregation is applied to the new (partitioned) windows resulting in a windowed <see cref="IKTable{K, V}"/>.
    /// <para>
    /// When a record merges existing sessions, a tombstone (null value) is forwarded for each merged session, then the new session.
    /// Results are forwarded with the session end as timestamp. The output keys can be deserialized with <see cref="SessionWindowedSerDes{T}"/>.
    /// </para>
    /// </summary>
    /// <typeparam name="K">Type of keys</typeparam>
    /// <typeparam name="V">Type of values</typeparam>
    public interface ISessionWindowedKStream<K, V>
    {
        /// <summary>
        /// Count the number of records in each session of this stream by the grouped key.
        /// </summary>
        /// <returns>a windowed <see cref="IKTable{K, V}"/> that contains "update" records with the number of records of each session,
        /// and tombstones (null count) for the sessions merged into a new one</returns>
        IKTable<Windowed<K>, long?> Count();

        /// <summary>
        /// Count the number of records in each session of this stream by the grouped key.
        /// </summary>
        /// <param name="named">a name config used to name the processor in the topology</param>
        /// <returns>a windowed <see cref="IKTable{K, V}"/> that contains "update" records with the number of records of each session,
        /// and tombstones (null count) for the sessions merged into a new one</returns>
        IKTable<Windowed<K>, long?> Count(string named);

        /// <summary>
        /// Count the number of records in each session of this stream by the grouped key.
        /// </summary>
        /// <param name="materialized">an instance of <see cref="Materialized{K, V, S}"/> used to materialize a session store.
        /// Its default value serdes is <see cref="NullableInt64SerDes"/>.</param>
        /// <param name="named">a name config used to name the processor in the topology</param>
        /// <returns>a windowed <see cref="IKTable{K, V}"/> that contains "update" records with the number of records of each session,
        /// and tombstones (null count) for the sessions merged into a new one</returns>
        IKTable<Windowed<K>, long?> Count(Materialized<K, long?, ISessionStore<Bytes, byte[]>> materialized, string named = null);

        /// <summary>
        /// Aggregate the values of records in each session of this stream by the grouped key.
        /// The <paramref name="initializer"/> gives the initial aggregate of a new session, the <paramref name="aggregator"/> adds a record to an aggregate,
        /// and the <paramref name="sessionMerger"/> merges the aggregates of two sessions which are merged into one.
        /// </summary>
        /// <typeparam name="VR">the value type of the resulting <see cref="IKTable{K, V}"/></typeparam>
        /// <param name="initializer">an initializer function that computes an initial intermediate aggregation result</param>
        /// <param name="aggregator">an aggregator function that computes a new aggregate result</param>
        /// <param name="sessionMerger">a merger function that merges the aggregates of two sessions</param>
        /// <returns>a windowed <see cref="IKTable{K, V}"/> that contains "update" records with the latest aggregate of each session</returns>
        IKTable<Windowed<K>, VR> Aggregate<VR>(Func<VR> initializer, Func<K, V, VR, VR> aggregator, Func<K, VR, VR, VR> sessionMerger);

        /// <summary>
        /// Aggregate the values of records in each session of this stream by the grouped key.
        /// </summary>
        /// <typeparam name="VR">the value type of the resulting <see cref="IKTable{K, V}"/></typeparam>
        /// <param name="initializer">an initializer that computes an initial intermediate aggregation result</param>
        /// <param name="aggregator">an aggregator that computes a new aggregate result</param>
        /// <param name="sessionMerger">a merger that merges the aggregates of two sessions</param>
        /// <returns>a windowed <see cref="IKTable{K, V}"/> that contains "update" records with the latest aggregate of each session</returns>
        IKTable<Windowed<K>, VR> Aggregate<VR>(Initializer<VR> initializer, Aggregator<K, V, VR> aggregator, Merger<K, VR> sessionMerger);

        /// <summary>
        /// Aggregate the values of records in each session of this stream by the grouped key.
        /// </summary>
        /// <typeparam name="VR">the value type of the resulting <see cref="IKTable{K, V}"/></typeparam>
        /// <typeparam name="VRS">the serdes of the aggregate</typeparam>
        /// <param name="initializer">an initializer function that computes an initial intermediate aggregation result</param>
        /// <param name="aggregator">an aggregator function that computes a new aggregate result</param>
        /// <param name="sessionMerger">a merger function that merges the aggregates of two sessions</param>
        /// <returns>a windowed <see cref="IKTable{K, V}"/> that contains "update" records with the latest aggregate of each session</returns>
        IKTable<Windowed<K>, VR> Aggregate<VR, VRS>(Func<VR> initializer, Func<K, V, VR, VR> aggregator, Func<K, VR, VR, VR> sessionMerger) where VRS : ISerDes<VR>, new();

        /// <summary>
        /// Aggregate the values of records in each session of this stream by the grouped key.
        /// </summary>
        /// <typeparam name="VR">the value type of the resulting <see cref="IKTable{K, V}"/></typeparam>
        /// <typeparam name="VRS">the serdes of the aggregate</typeparam>
        /// <param name="initializer">an initializer that computes an initial intermediate aggregation result</param>
        /// <param name="aggregator">an aggregator that computes a new aggregate result</param>
        /// <param name="sessionMerger">a merger that merges the aggregates of two sessions</param>
        /// <returns>a windowed <see cref="IKTable{K, V}"/> that contains "update" records with the latest aggregate of each session</returns>
        IKTable<Windowed<K>, VR> Aggregate<VR, VRS>(Initializer<VR> initializer, Aggregator<K, V, VR> aggregator, Merger<K, VR> sessionMerger) where VRS : ISerDes<VR>, new();

        /// <summary>
        /// Aggregate the values of records in each session of this stream by the grouped key.
        /// </summary>
        /// <typeparam name="VR">the value type of the resulting <see cref="IKTable{K, V}"/></typeparam>
        /// <param name="initializer">an initializer function that computes an initial intermediate aggregation result</param>
        /// <param name="aggregator">an aggregator function that computes a new aggregate result</param>
        /// <param name="sessionMerger">a merger function that merges the aggregates of two sessions</param>
        /// <param name="materialized">an instance of <see cref="Materialized{K, V, S}"/> used to materialize a session store.</param>
        /// <param name="named">a name config used to name the processor in the topology</param>
        /// <returns>a windowed <see cref="IKTable{K, V}"/> that contains "update" records with the latest aggregate of each session</returns>
        IKTable<Windowed<K>, VR> Aggregate<VR>(Func<VR> initializer, Func<K, V, VR, VR> aggregator, Func<K, VR, VR, VR> sessionMerger, Materialized<K, VR, ISessionStore<Bytes, byte[]>> materialized, string named = null);

        /// <summary>
        /// Aggregate the values of records in each session of this stream by the grouped key.
        /// </summary>
        /// <typeparam name="VR">the value type of the resulting <see cref="IKTable{K, V}"/></typeparam>
        /// <param name="initializer">an initializer that computes an initial intermediate aggregation result</param>
        /// <param name="aggregator">an aggregator that computes a new aggregate result</param>
        /// <param name="sessionMerger">a merger that merges the aggregates of two sessions</param>
        /// <param name="materialized">an instance of <see cref="Materialized{K, V, S}"/> used to materialize a session store.</param>
        /// <param name="named">a name config used to name the processor in the topology</param>
        /// <returns>a windowed <see cref="IKTable{K, V}"/> that contains "update" records with the latest aggregate of each session</returns>
        IKTable<Windowed<K>, VR> Aggregate<VR>(Initializer<VR> initializer, Aggregator<K, V, VR> aggregator, Merger<K, VR> sessionMerger, Materialized<K, VR, ISessionStore<Bytes, byte[]>> materialized, string named = null);

        /// <summary>
        /// Combine the values of records in each session of this stream by the grouped key.
        /// The reducer is also used to merge the aggregates of two sessions.
        /// </summary>
        /// <param name="reducer">a reducer that computes a new aggregate result</param>
        /// <returns>a windowed <see cref="IKTable{K, V}"/> that contains "update" records with the latest aggregate of each session</returns>
        IKTable<Windowed<K>, V> Reduce(Reducer<V> reducer);

        /// <summary>
        /// Combine the values of records in each session of this stream by the grouped key.
        /// The reducer is also used to merge the aggregates of two sessions.
        /// </summary>
        /// <param name="reducer">a reducer function that computes a new aggregate result</param>
        /// <returns>a windowed <see cref="IKTable{K, V}"/> that contains "update" records with the latest aggregate of each session</returns>
        IKTable<Windowed<K>, V> Reduce(Func<V, V, V> reducer);

        /// <summary>
        /// Combine the values of records in each session of this stream by the grouped key.
        /// The reducer is also used to merge the aggregates of two sessions.
        /// </summary>
        /// <param name="reducer">a reducer that computes a new aggregate result</param>
        /// <param name="materialized">an instance of <see cref="Materialized{K, V, S}"/> used to materialize a session store.</param>
        /// <param name="named">a name config used to name the processor in the topology</param>
        /// <returns>a windowed <see cref="IKTable{K, V}"/> that contains "update" records with the latest aggregate of each session</returns>
        IKTable<Windowed<K>, V> Reduce(Reducer<V> reducer, Materialized<K, V, ISessionStore<Bytes, byte[]>> materialized, string named = null);

        /// <summary>
        /// Combine the values of records in each session of this stream by the grouped key.
        /// The reducer is also used to merge the aggregates of two sessions.
        /// </summary>
        /// <param name="reducer">a reducer function that computes a new aggregate result</param>
        /// <param name="materialized">an instance of <see cref="Materialized{K, V, S}"/> used to materialize a session store.</param>
        /// <param name="named">a name config used to name the processor in the topology</param>
        /// <returns>a windowed <see cref="IKTable{K, V}"/> that contains "update" records with the latest aggregate of each session</returns>
        IKTable<Windowed<K>, V> Reduce(Func<V, V, V> reducer, Materialized<K, V, ISessionStore<Bytes, byte[]>> materialized, string named = null);
    }
}
