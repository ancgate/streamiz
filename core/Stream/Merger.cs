using System;

namespace Streamiz.Kafka.Net.Stream
{
    /// <summary>
    /// The <see cref="Merger{K, V}"/> interface for merging the aggregates of two sessions (session windows) of the same key.
    /// It is called when a new record falls into existing sessions, or bridges two of them.
    /// </summary>
    /// <typeparam name="K">key type</typeparam>
    /// <typeparam name="V">aggregate value type</typeparam>
    public interface Merger<in K, V>
    {
        /// <summary>
        /// Compute a new aggregate from the key and the aggregates of two sessions.
        /// </summary>
        /// <param name="aggKey">the key of the record</param>
        /// <param name="aggOne">the first aggregate</param>
        /// <param name="aggTwo">the second aggregate</param>
        /// <returns>the merged aggregate</returns>
        V Apply(K aggKey, V aggOne, V aggTwo);
    }

    internal class WrappedMerger<K, V> : Merger<K, V>
    {
        private readonly Func<K, V, V, V> function;

        public WrappedMerger(Func<K, V, V, V> function)
        {
            this.function = function ?? throw new ArgumentNullException($"Merger function can't be null");
        }

        public V Apply(K aggKey, V aggOne, V aggTwo) => function.Invoke(aggKey, aggOne, aggTwo);
    }
}
