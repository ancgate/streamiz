using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Confluent.Kafka;
using NUnit.Framework;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.State.Enumerator;
using Streamiz.Kafka.Net.State.Internal;
using Streamiz.Kafka.Net.Stream;

namespace Streamiz.Kafka.Net.Tests.Private
{
    public class SessionKeySchemaTests
    {
        private readonly SessionKeySchema schema = new();
        private static Bytes Key(string key) => Bytes.Wrap(Encoding.UTF8.GetBytes(key));

        private static byte[] BigEndian(long value)
        {
            var bytes = BitConverter.GetBytes(value);
            if (BitConverter.IsLittleEndian)
                Array.Reverse(bytes);
            return bytes;
        }

        [Test]
        public void BinaryLayoutIsKeyEndStart()
        {
            var binary = SessionKeySchema.ToBinary(Key("ab"), 10, 20).Get;

            var expected = Encoding.UTF8.GetBytes("ab").Concat(BigEndian(20)).Concat(BigEndian(10)).ToArray();
            CollectionAssert.AreEqual(expected, binary);
            Assert.AreEqual(10, SessionKeySchema.ExtractStartTimestamp(binary));
            Assert.AreEqual(20, SessionKeySchema.ExtractEndTimestamp(binary));
            CollectionAssert.AreEqual(Encoding.UTF8.GetBytes("ab"), SessionKeySchema.ExtractKeyBytes(binary));
            Assert.AreEqual(20, schema.SegmentTimestamp(Bytes.Wrap(binary)));

            var windowed = SessionKeySchema.From(Bytes.Wrap(binary));
            Assert.AreEqual(new SessionWindow(10, 20), windowed.Window);
            Assert.AreEqual("ab", Encoding.UTF8.GetString(windowed.Key.Get));
        }

        [Test]
        public void FixedSizeRangeContainsMatchingSessionsOfTheKey()
        {
            var comparer = new BytesComparer();
            var lower = schema.LowerRangeFixedSize(Key("a"), 100);
            var upper = schema.UpperRangeFixedSize(Key("a"), 200);

            // end >= 100 and start <= 200
            Assert.LessOrEqual(comparer.Compare(lower, SessionKeySchema.ToBinary(Key("a"), 0, 100)), 0);
            Assert.GreaterOrEqual(comparer.Compare(upper, SessionKeySchema.ToBinary(Key("a"), 200, long.MaxValue)), 0);
            Assert.Greater(comparer.Compare(lower, SessionKeySchema.ToBinary(Key("a"), 0, 99)), 0);
            Assert.Greater(comparer.Compare(SessionKeySchema.ToBinary(Key("b"), 0, 0), upper), 0);
        }

        [Test]
        public void UnboundedRangesAreNull()
        {
            Assert.IsNull(schema.LowerRange(null, 0));
            Assert.IsNull(schema.UpperRange(null, 0));
        }

        [Test]
        public void HasNextConditionFiltersOnKeyAndTimes()
        {
            var records = new List<KeyValuePair<Bytes, byte[]>>
            {
                new(SessionKeySchema.ToBinary(Key("a"), 0, 50), null), // end < 100
                new(SessionKeySchema.ToBinary(Key("a"), 150, 300), null), // match
                new(SessionKeySchema.ToBinary(Key("a"), 250, 300), null), // start > 200
                new(SessionKeySchema.ToBinary(Key("b"), 150, 300), null), // other key
            };

            var condition = schema.HasNextCondition(Key("a"), Key("a"), 100, 200);
            using var enumerator = new WrapEnumerableKeyValueEnumerator<Bytes, byte[]>(records);

            var matches = new List<Bytes>();
            while (condition(enumerator))
                matches.Add(enumerator.PeekNextKey());

            Assert.AreEqual(1, matches.Count);
            Assert.AreEqual(new SessionWindow(150, 300), SessionKeySchema.From(matches[0]).Window);
        }

        [Test]
        public void SessionWindowsOverlapInclusively()
        {
            Assert.IsTrue(new SessionWindow(0, 10).Overlap(new SessionWindow(10, 20)));
            Assert.IsTrue(new SessionWindow(5, 5).Overlap(new SessionWindow(0, 10)));
            Assert.IsFalse(new SessionWindow(0, 10).Overlap(new SessionWindow(11, 20)));
            Assert.IsFalse(new SessionWindow(0, 10).Overlap(new TimeWindow(0, 10)));
            Assert.AreNotEqual(new SessionWindow(0, 10), new TimeWindow(0, 10));
        }

        [Test]
        public void SessionWindowedSerDesRoundTrip()
        {
            var serdes = new SessionWindowedSerDes<string>(new StringSerDes());
            var key = new Windowed<string>("key", new SessionWindow(100, 200));

            var bytes = serdes.Serialize(key, new SerializationContext());
            CollectionAssert.AreEqual(SessionKeySchema.ToBinary(Encoding.UTF8.GetBytes("key"), 100, 200), bytes);

            var result = serdes.Deserialize(bytes, new SerializationContext());
            Assert.AreEqual(key, result);
            Assert.IsInstanceOf<SessionWindow>(result.Window);

            Assert.IsNull(serdes.Serialize(null, new SerializationContext()));
            Assert.IsNull(serdes.Deserialize(null, new SerializationContext()));
        }
    }
}
