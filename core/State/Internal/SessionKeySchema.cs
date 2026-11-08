using System;
using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State.Enumerator;
using Streamiz.Kafka.Net.Stream;

namespace Streamiz.Kafka.Net.State.Internal
{
    /// <summary>
    /// Binary layout of a session key, the same as Kafka Streams: <c>key | endTime (8 bytes) | startTime (8 bytes)</c>, big-endian.
    /// Sessions are segmented by end time.
    /// </summary>
    internal class SessionKeySchema : IKeySchema
    {
        internal const int TIMESTAMP_SIZE = sizeof(long);
        internal const int SUFFIX_SIZE = 2 * TIMESTAMP_SIZE;
        internal static readonly byte[] MIN_SUFFIX = new byte[SUFFIX_SIZE];

        private static readonly IComparer<Bytes> bytesComparer = new BytesComparer();

        #region Static helpers

        internal static Bytes ToBinary(Windowed<Bytes> sessionKey)
            => ToBinary(sessionKey.Key, sessionKey.Window.StartMs, sessionKey.Window.EndMs);

        internal static Bytes ToBinary(Bytes key, long startTime, long endTime)
            => Bytes.Wrap(ToBinary(key?.Get, startTime, endTime));

        internal static byte[] ToBinary(byte[] key, long startTime, long endTime)
        {
            key ??= Array.Empty<byte>();
            using var buffer = ByteBuffer.Build(key.Length + SUFFIX_SIZE, true);
            buffer.Put(key);
            buffer.PutLong(endTime);
            buffer.PutLong(startTime);
            return buffer.ToArray();
        }

        internal static byte[] ToBinary<K>(Windowed<K> sessionKey, ISerDes<K> serializer, string topic)
        {
            var bytes = serializer.Serialize(sessionKey.Key,
                new SerializationContext(MessageComponentType.Key, topic));
            return ToBinary(bytes, sessionKey.Window.StartMs, sessionKey.Window.EndMs);
        }

        internal static byte[] ExtractKeyBytes(byte[] binaryKey)
            => binaryKey.AsSpan(0, binaryKey.Length - SUFFIX_SIZE).ToArray();

        internal static long ExtractEndTimestamp(byte[] binaryKey)
        {
            using var buffer = ByteBuffer.Build(binaryKey, true);
            return buffer.GetLong(binaryKey.Length - SUFFIX_SIZE);
        }

        internal static long ExtractStartTimestamp(byte[] binaryKey)
        {
            using var buffer = ByteBuffer.Build(binaryKey, true);
            return buffer.GetLong(binaryKey.Length - TIMESTAMP_SIZE);
        }

        internal static SessionWindow ExtractWindow(byte[] binaryKey)
            => new(ExtractStartTimestamp(binaryKey), ExtractEndTimestamp(binaryKey));

        internal static Windowed<Bytes> From(Bytes binaryKey)
            => new(Bytes.Wrap(ExtractKeyBytes(binaryKey.Get)), ExtractWindow(binaryKey.Get));

        internal static Windowed<K> From<K>(byte[] binaryKey, ISerDes<K> deserializer, string topic)
        {
            var key = deserializer.Deserialize(ExtractKeyBytes(binaryKey),
                new SerializationContext(MessageComponentType.Key, topic));
            return new Windowed<K>(key, ExtractWindow(binaryKey));
        }

        #endregion

        #region IKeySchema

        public Bytes UpperRange(Bytes key, long to)
        {
            if (key == null)
                return null;

            using var buffer = ByteBuffer.Build(SUFFIX_SIZE, true);
            // the end can be as large as possible as long as it's larger than the start
            byte[] maxSuffix = buffer.PutLong(long.MaxValue)
                .PutLong(to)
                .ToArray();
            return OrderedBytes.UpperRange(key, maxSuffix);
        }

        public Bytes LowerRange(Bytes key, long from)
            => key == null ? null : OrderedBytes.LowerRange(key, MIN_SUFFIX);

        public Bytes UpperRangeFixedSize(Bytes key, long to)
            => ToBinary(key, to, long.MaxValue);

        public Bytes LowerRangeFixedSize(Bytes key, long from)
            => ToBinary(key, 0, Math.Max(0, from));

        public long SegmentTimestamp(Bytes key)
            => ExtractEndTimestamp(key.Get);

        // from: earliest session end time, to: latest session start time
        public Func<IKeyValueEnumerator<Bytes, byte[]>, bool> HasNextCondition(Bytes binaryKeyFrom, Bytes binaryKeyTo, long from, long to)
        {
            return enumerator =>
            {
                while (enumerator.MoveNext())
                {
                    var bytes = enumerator.PeekNextKey();
                    var keyBytes = Bytes.Wrap(ExtractKeyBytes(bytes.Get));
                    if ((binaryKeyFrom == null || bytesComparer.Compare(keyBytes, binaryKeyFrom) >= 0)
                        && (binaryKeyTo == null || bytesComparer.Compare(keyBytes, binaryKeyTo) <= 0)
                        && ExtractEndTimestamp(bytes.Get) >= from
                        && ExtractStartTimestamp(bytes.Get) <= to)
                        return true;
                }

                return false;
            };
        }

        // sessions are segmented by end time: any segment from the earliest end time can contain a matching session
        public IList<S> SegmentsToSearch<S>(ISegments<S> segments, long from, long to, bool forward) where S : ISegment
            => segments.Segments(from, long.MaxValue, forward).ToList();

        #endregion
    }
}
