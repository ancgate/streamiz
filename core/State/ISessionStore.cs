using Streamiz.Kafka.Net.Processors;

namespace Streamiz.Kafka.Net.State
{
    /// <summary>
    /// Interface for storing the aggregated values of sessions.
    /// <para>
    /// The key is a <see cref="Windowed{K}"/> made of the record key and its <see cref="Stream.SessionWindow"/>
    /// (start and end of the session, both inclusive).
    /// </para>
    /// </summary>
    /// <typeparam name="K">type of the record keys</typeparam>
    /// <typeparam name="AGG">type of the aggregated values</typeparam>
    public interface ISessionStore<K,AGG> : IStateStore, IReadOnlySessionStore<K,AGG>
    {
        /// <summary>
        /// Remove the session aggregated with the provided <see cref="Windowed{K}"/> from the store.
        /// </summary>
        /// <param name="sessionKey">key of the session to remove</param>
        void Remove(Windowed<K> sessionKey);

        /// <summary>
        /// Write the aggregated value for the provided key to the store.
        /// A null <paramref name="aggregate"/> removes the session.
        /// </summary>
        /// <param name="sessionKey">key of the session to write</param>
        /// <param name="aggregate">the aggregated value for the session, it can be null</param>
        void Put(Windowed<K> sessionKey, AGG aggregate);
    }
}
