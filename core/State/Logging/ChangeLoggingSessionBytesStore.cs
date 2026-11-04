using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.State.Enumerator;
using Streamiz.Kafka.Net.State.Internal;

namespace Streamiz.Kafka.Net.State.Logging
{
    /// <summary>
    /// Session store which writes each put and remove to the changelog topic, with the <see cref="SessionKeySchema"/> binary key.
    /// </summary>
    internal class ChangeLoggingSessionBytesStore :
        WrappedStateStore<ISessionStore<Bytes, byte[]>>,
        ISessionStore<Bytes, byte[]>
    {
        public ChangeLoggingSessionBytesStore(ISessionStore<Bytes, byte[]> wrapped)
            : base(wrapped)
        {
        }

        public override bool IsCachedStore => false;

        private void Publish(Windowed<Bytes> sessionKey, byte[] value)
            => context.Log(Name, SessionKeySchema.ToBinary(sessionKey), value, context.RecordContext.Timestamp);

        public void Put(Windowed<Bytes> sessionKey, byte[] aggregate)
        {
            wrapped.Put(sessionKey, aggregate);
            Publish(sessionKey, aggregate);
        }

        public void Remove(Windowed<Bytes> sessionKey)
        {
            wrapped.Remove(sessionKey);
            Publish(sessionKey, null);
        }

        public IKeyValueEnumerator<Windowed<Bytes>, byte[]> FindSessions(Bytes key, long earliestSessionEndTime, long latestSessionStartTime)
            => wrapped.FindSessions(key, earliestSessionEndTime, latestSessionStartTime);

        public IKeyValueEnumerator<Windowed<Bytes>, byte[]> FindSessions(Bytes keyFrom, Bytes keyTo, long earliestSessionEndTime, long latestSessionStartTime)
            => wrapped.FindSessions(keyFrom, keyTo, earliestSessionEndTime, latestSessionStartTime);

        public IKeyValueEnumerator<Windowed<Bytes>, byte[]> BackwardFindSessions(Bytes key, long earliestSessionEndTime, long latestSessionStartTime)
            => wrapped.BackwardFindSessions(key, earliestSessionEndTime, latestSessionStartTime);

        public IKeyValueEnumerator<Windowed<Bytes>, byte[]> BackwardFindSessions(Bytes keyFrom, Bytes keyTo, long earliestSessionEndTime, long latestSessionStartTime)
            => wrapped.BackwardFindSessions(keyFrom, keyTo, earliestSessionEndTime, latestSessionStartTime);

        public byte[] FetchSession(Bytes key, long sessionStartTime, long sessionEndTime)
            => wrapped.FetchSession(key, sessionStartTime, sessionEndTime);

        public IKeyValueEnumerator<Windowed<Bytes>, byte[]> Fetch(Bytes key)
            => wrapped.Fetch(key);

        public IKeyValueEnumerator<Windowed<Bytes>, byte[]> Fetch(Bytes keyFrom, Bytes keyTo)
            => wrapped.Fetch(keyFrom, keyTo);

        public IKeyValueEnumerator<Windowed<Bytes>, byte[]> BackwardFetch(Bytes key)
            => wrapped.BackwardFetch(key);

        public IKeyValueEnumerator<Windowed<Bytes>, byte[]> BackwardFetch(Bytes keyFrom, Bytes keyTo)
            => wrapped.BackwardFetch(keyFrom, keyTo);
    }
}
