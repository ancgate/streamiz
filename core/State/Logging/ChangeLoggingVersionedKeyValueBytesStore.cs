using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.State.Internal;

namespace Streamiz.Kafka.Net.State.Logging
{
    /// <summary>
    /// Logs the changes of a versioned store: each record is logged with its own timestamp (tombstones included),
    /// so the versions can be restored by putting the changelog records again.
    /// </summary>
    internal class ChangeLoggingVersionedKeyValueBytesStore :
        WrappedStateStore<IVersionedKeyValueStore<Bytes, byte[]>>,
        IVersionedKeyValueStore<Bytes, byte[]>
    {
        public ChangeLoggingVersionedKeyValueBytesStore(IVersionedKeyValueStore<Bytes, byte[]> wrapped)
            : base(wrapped)
        {
        }

        public override bool IsCachedStore => false;

        public long Put(Bytes key, byte[] value, long timestamp)
        {
            long validTo = wrapped.Put(key, value, timestamp);
            context.Log(Name, key, value, timestamp);
            return validTo;
        }

        public VersionedRecord<byte[]> Delete(Bytes key, long timestamp)
        {
            var existing = wrapped.Delete(key, timestamp);
            context.Log(Name, key, null, timestamp);
            return existing;
        }

        public VersionedRecord<byte[]> Get(Bytes key)
            => wrapped.Get(key);

        public VersionedRecord<byte[]> Get(Bytes key, long asOfTimestamp)
            => wrapped.Get(key, asOfTimestamp);
    }
}
