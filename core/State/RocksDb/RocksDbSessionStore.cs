using System.Collections.Generic;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.State.Enumerator;
using Streamiz.Kafka.Net.State.Internal;

namespace Streamiz.Kafka.Net.State
{
    /// <summary>
    /// RocksDB session store: sessions are written in a segmented bytes store (segments by session end time),
    /// with the <see cref="SessionKeySchema"/> binary layout.
    /// </summary>
    internal class RocksDbSessionStore
        : WrappedStateStore<RocksDbSegmentedBytesStore>, ISessionStore<Bytes, byte[]>
    {
        public RocksDbSessionStore(RocksDbSegmentedBytesStore wrapped)
            : base(wrapped)
        {
        }

        public override bool IsCachedStore => false;

        private static IKeyValueEnumerator<Windowed<Bytes>, byte[]> ToSessionEnumerator(IKeyValueEnumerator<Bytes, byte[]> enumerator)
            => new TransformKeyValueEnumerator<Bytes, byte[], Windowed<Bytes>, byte[]>(
                enumerator,
                (key, value) => new KeyValuePair<Windowed<Bytes>, byte[]>(SessionKeySchema.From(key), value));

        public IKeyValueEnumerator<Windowed<Bytes>, byte[]> FindSessions(Bytes key, long earliestSessionEndTime, long latestSessionStartTime)
            => ToSessionEnumerator(wrapped.Fetch(key, earliestSessionEndTime, latestSessionStartTime));

        public IKeyValueEnumerator<Windowed<Bytes>, byte[]> FindSessions(Bytes keyFrom, Bytes keyTo, long earliestSessionEndTime, long latestSessionStartTime)
            => ToSessionEnumerator(wrapped.Fetch(keyFrom, keyTo, earliestSessionEndTime, latestSessionStartTime));

        public IKeyValueEnumerator<Windowed<Bytes>, byte[]> BackwardFindSessions(Bytes key, long earliestSessionEndTime, long latestSessionStartTime)
            => ToSessionEnumerator(wrapped.ReverseFetch(key, earliestSessionEndTime, latestSessionStartTime));

        public IKeyValueEnumerator<Windowed<Bytes>, byte[]> BackwardFindSessions(Bytes keyFrom, Bytes keyTo, long earliestSessionEndTime, long latestSessionStartTime)
            => ToSessionEnumerator(wrapped.ReverseFetch(keyFrom, keyTo, earliestSessionEndTime, latestSessionStartTime));

        public byte[] FetchSession(Bytes key, long sessionStartTime, long sessionEndTime)
            => wrapped.Get(SessionKeySchema.ToBinary(key, sessionStartTime, sessionEndTime));

        public IKeyValueEnumerator<Windowed<Bytes>, byte[]> Fetch(Bytes key)
            => FindSessions(key, 0, long.MaxValue);

        public IKeyValueEnumerator<Windowed<Bytes>, byte[]> Fetch(Bytes keyFrom, Bytes keyTo)
            => FindSessions(keyFrom, keyTo, 0, long.MaxValue);

        public IKeyValueEnumerator<Windowed<Bytes>, byte[]> BackwardFetch(Bytes key)
            => BackwardFindSessions(key, 0, long.MaxValue);

        public IKeyValueEnumerator<Windowed<Bytes>, byte[]> BackwardFetch(Bytes keyFrom, Bytes keyTo)
            => BackwardFindSessions(keyFrom, keyTo, 0, long.MaxValue);

        public void Remove(Windowed<Bytes> sessionKey)
            => wrapped.Remove(SessionKeySchema.ToBinary(sessionKey));

        public void Put(Windowed<Bytes> sessionKey, byte[] aggregate)
            => wrapped.Put(SessionKeySchema.ToBinary(sessionKey), aggregate);
    }
}
