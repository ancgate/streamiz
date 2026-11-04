using Streamiz.Kafka.Net.State.Enumerator;

namespace Streamiz.Kafka.Net.State
{
    /// <summary>
    /// A session store that only supports read operations.
    /// <para>
    /// A session is identified by its key and its <see cref="Stream.SessionWindow"/>: start and end are both inclusive.
    /// </para>
    /// </summary>
    /// <typeparam name="K">the key type</typeparam>
    /// <typeparam name="AGG">the aggregated value type</typeparam>
    public interface IReadOnlySessionStore<K,AGG>
    {
        /// <summary>
        /// Fetch any sessions with the matching key whose end is &gt;= <paramref name="earliestSessionEndTime"/>
        /// and whose start is &lt;= <paramref name="latestSessionStartTime"/>.
        /// <para>
        /// For a new record at time t and an inactivity gap g, the sessions to merge with are found with
        /// FindSessions(key, t - g, t + g).
        /// </para>
        /// </summary>
        /// <param name="key">the key to return sessions for</param>
        /// <param name="earliestSessionEndTime">the end timestamp of the earliest session to search for, inclusive</param>
        /// <param name="latestSessionStartTime">the start timestamp of the latest session to search for, inclusive</param>
        /// <returns>an enumerator over the sessions, ordered by end time then start time. It must be disposed.</returns>
        IKeyValueEnumerator<Windowed<K>, AGG> FindSessions(K key, long earliestSessionEndTime, long latestSessionStartTime);

        /// <summary>
        /// Fetch any sessions in the given key range whose end is &gt;= <paramref name="earliestSessionEndTime"/>
        /// and whose start is &lt;= <paramref name="latestSessionStartTime"/>.
        /// </summary>
        /// <param name="keyFrom">the first key in the range (inclusive), null means no lower bound</param>
        /// <param name="keyTo">the last key in the range (inclusive), null means no upper bound</param>
        /// <param name="earliestSessionEndTime">the end timestamp of the earliest session to search for, inclusive</param>
        /// <param name="latestSessionStartTime">the start timestamp of the latest session to search for, inclusive</param>
        /// <returns>an enumerator over the sessions. It must be disposed.</returns>
        IKeyValueEnumerator<Windowed<K>, AGG> FindSessions(K keyFrom, K keyTo, long earliestSessionEndTime, long latestSessionStartTime);

        /// <summary>
        /// Same as <see cref="FindSessions(K, long, long)"/>, in reverse order.
        /// </summary>
        /// <param name="key">the key to return sessions for</param>
        /// <param name="earliestSessionEndTime">the end timestamp of the earliest session to search for, inclusive</param>
        /// <param name="latestSessionStartTime">the start timestamp of the latest session to search for, inclusive</param>
        /// <returns>an enumerator over the sessions, from the latest to the earliest. It must be disposed.</returns>
        IKeyValueEnumerator<Windowed<K>, AGG> BackwardFindSessions(K key, long earliestSessionEndTime, long latestSessionStartTime);

        /// <summary>
        /// Same as <see cref="FindSessions(K, K, long, long)"/>, in reverse order.
        /// </summary>
        /// <param name="keyFrom">the first key in the range (inclusive), null means no lower bound</param>
        /// <param name="keyTo">the last key in the range (inclusive), null means no upper bound</param>
        /// <param name="earliestSessionEndTime">the end timestamp of the earliest session to search for, inclusive</param>
        /// <param name="latestSessionStartTime">the start timestamp of the latest session to search for, inclusive</param>
        /// <returns>an enumerator over the sessions, in reverse order. It must be disposed.</returns>
        IKeyValueEnumerator<Windowed<K>, AGG> BackwardFindSessions(K keyFrom, K keyTo, long earliestSessionEndTime, long latestSessionStartTime);

        /// <summary>
        /// Get the value of the session with the given key, start and end.
        /// </summary>
        /// <param name="key">the key of the session</param>
        /// <param name="sessionStartTime">start timestamp of the session</param>
        /// <param name="sessionEndTime">end timestamp of the session</param>
        /// <returns>the value of the session, or default if it doesn't exist</returns>
        AGG FetchSession(K key, long sessionStartTime, long sessionEndTime);

        /// <summary>
        /// Retrieve all sessions for the provided key.
        /// </summary>
        /// <param name="key">the key to return sessions for</param>
        /// <returns>an enumerator over the sessions of this key. It must be disposed.</returns>
        IKeyValueEnumerator<Windowed<K>, AGG> Fetch(K key);

        /// <summary>
        /// Retrieve all sessions for the keys in the given range.
        /// </summary>
        /// <param name="keyFrom">the first key in the range (inclusive), null means no lower bound</param>
        /// <param name="keyTo">the last key in the range (inclusive), null means no upper bound</param>
        /// <returns>an enumerator over the sessions. It must be disposed.</returns>
        IKeyValueEnumerator<Windowed<K>, AGG> Fetch(K keyFrom, K keyTo);

        /// <summary>
        /// Retrieve all sessions for the provided key, in reverse order.
        /// </summary>
        /// <param name="key">the key to return sessions for</param>
        /// <returns>an enumerator over the sessions of this key, from the latest to the earliest. It must be disposed.</returns>
        IKeyValueEnumerator<Windowed<K>, AGG> BackwardFetch(K key);

        /// <summary>
        /// Retrieve all sessions for the keys in the given range, in reverse order.
        /// </summary>
        /// <param name="keyFrom">the first key in the range (inclusive), null means no lower bound</param>
        /// <param name="keyTo">the last key in the range (inclusive), null means no upper bound</param>
        /// <returns>an enumerator over the sessions, in reverse order. It must be disposed.</returns>
        IKeyValueEnumerator<Windowed<K>, AGG> BackwardFetch(K keyFrom, K keyTo);
    }
}
