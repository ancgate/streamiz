using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using NUnit.Framework;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Mock.Kafka;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Table.Internal.ForeignKeyJoin;

namespace Streamiz.Kafka.Net.Tests.Private
{
    public class ForeignKeyJoinInternalsTests
    {
        private static readonly SerializationContext context = new(MessageComponentType.Value, "topic");

        // reference values computed with the mmh3 Python library (MurmurHash3 x64 128, seed 104729 as Kafka Streams):
        // h1 = low 64 bits, h2 = high 64 bits, as signed longs
        private static IEnumerable<TestCaseData> Murmur3Vectors()
        {
            yield return new TestCaseData(Array.Empty<byte>(), -7122613646888064702L, -8341524471658347240L).SetName("empty");
            yield return new TestCaseData(Encoding.UTF8.GetBytes("a"), 3702349557448790408L, -1601067453332442517L).SetName("a");
            yield return new TestCaseData(Encoding.UTF8.GetBytes("21"), -1045175892060202657L, -6196245913608550939L).SetName("21");
            yield return new TestCaseData(Encoding.UTF8.GetBytes("foobar"), -1926091222759195742L, 2201308245896195406L).SetName("foobar");
            yield return new TestCaseData(Encoding.UTF8.GetBytes("hello"), 3512725192710940516L, 6227047293690958278L).SetName("hello");
            yield return new TestCaseData(Encoding.UTF8.GetBytes("The quick brown fox jumps over the lazy dog"),
                -1779949751512360504L, -5190294356955732186L).SetName("fox (2 blocks + tail)");
            yield return new TestCaseData(Enumerable.Range(0, 33).Select(i => (byte) i).ToArray(),
                8634550732263550921L, -8560910543912848340L).SetName("33 bytes");
            yield return new TestCaseData(
                Enumerable.Repeat(new byte[] {0xff, 0x80, 0x7f, 0x00, 0xfe}, 7).SelectMany(b => b).ToArray(),
                -2533389864541559941L, 7380254795117832774L).SetName("35 bytes with high bits");
        }

        [TestCaseSource(nameof(Murmur3Vectors))]
        public void Murmur3Hash128MatchesReference(byte[] data, long h1, long h2)
        {
            var hash = Murmur3.Hash128(data);
            Assert.AreEqual(h1, hash[0]);
            Assert.AreEqual(h2, hash[1]);
        }

        [Test]
        public void Murmur3SeedZeroMatchesTheWellKnownHelloValue()
        {
            // MurmurHash3_x64_128("hello", 0) = cbd8a7b341bd9b02 5b1e906a48ae1d19
            var hash = Murmur3.Hash128(Encoding.UTF8.GetBytes("hello"), 0, 5, 0);
            Assert.AreEqual(unchecked((long) 0xcbd8a7b341bd9b02UL), hash[0]);
            Assert.AreEqual(0x5b1e906a48ae1d19L, hash[1]);
        }

        [Test]
        public void SubscriptionWrapperRoundTrip()
        {
            var serdes = new SubscriptionWrapperSerDes<string>("topic-pk", new StringSerDes());
            var wrapper = new SubscriptionWrapper<string>(new[] {1L, -2L},
                SubscriptionInstruction.PROPAGATE_ONLY_IF_FK_VAL_AVAILABLE, "pk", 3);

            var bytes = serdes.Serialize(wrapper, context);
            // version | instruction | hash (16) | partition (4) | "pk"
            Assert.AreEqual(2 + 16 + 4 + 2, bytes.Length);
            Assert.AreEqual(SubscriptionWrapper<string>.CURRENT_VERSION, bytes[0]);

            var result = serdes.Deserialize(bytes, context);
            CollectionAssert.AreEqual(wrapper.Hash, result.Hash);
            Assert.AreEqual(wrapper.Instruction, result.Instruction);
            Assert.AreEqual("pk", result.PrimaryKey);
            Assert.AreEqual(3, result.PrimaryPartition);
        }

        [Test]
        public void SubscriptionWrapperWithNullHashRoundTrip()
        {
            var serdes = new SubscriptionWrapperSerDes<string>("topic-pk", new StringSerDes());
            var wrapper = new SubscriptionWrapper<string>(null, SubscriptionInstruction.DELETE_KEY_AND_PROPAGATE, "pk", 0);

            var bytes = serdes.Serialize(wrapper, context);
            Assert.AreEqual(2 + 4 + 2, bytes.Length);
            Assert.AreEqual(SubscriptionWrapper<string>.CURRENT_VERSION | 0x80, bytes[0]);

            var result = serdes.Deserialize(bytes, context);
            Assert.IsNull(result.Hash);
            Assert.AreEqual(SubscriptionInstruction.DELETE_KEY_AND_PROPAGATE, result.Instruction);
            Assert.AreEqual("pk", result.PrimaryKey);
        }

        [Test]
        public void SubscriptionWrapperWithUnknownVersionFails()
        {
            var serdes = new SubscriptionWrapperSerDes<string>("topic-pk", new StringSerDes());
            var bytes = serdes.Serialize(
                new SubscriptionWrapper<string>(null, SubscriptionInstruction.DELETE_KEY_AND_PROPAGATE, "pk", 0), context);
            bytes[0] = 0x80 | 42;
            Assert.Throws<Errors.StreamsException>(() => serdes.Deserialize(bytes, context));
        }

        [Test]
        public void SubscriptionResponseWrapperRoundTrip()
        {
            var serdes = new SubscriptionResponseWrapperSerDes<string>("topic-fv", new StringSerDes());

            var withValue = serdes.Deserialize(
                serdes.Serialize(new SubscriptionResponseWrapper<string>(new[] {5L, 6L}, "value", 2), context), context);
            CollectionAssert.AreEqual(new[] {5L, 6L}, withValue.OriginalValueHash);
            Assert.AreEqual("value", withValue.ForeignValue);
            Assert.AreEqual(2, withValue.PrimaryPartition);

            // a null foreign value is different from an empty one
            var withNull = serdes.Deserialize(
                serdes.Serialize(new SubscriptionResponseWrapper<string>(null, null, 1), context), context);
            Assert.IsNull(withNull.OriginalValueHash);
            Assert.IsNull(withNull.ForeignValue);
            Assert.AreEqual(1, withNull.PrimaryPartition);

            var withEmpty = serdes.Deserialize(
                serdes.Serialize(new SubscriptionResponseWrapper<string>(new[] {1L, 1L}, "", 0), context), context);
            Assert.AreEqual("", withEmpty.ForeignValue);
        }

        [Test]
        public void CombinedKeyRoundTripAndPrefix()
        {
            var schema = new CombinedKeySchema<string, string>("fk", new StringSerDes(), "pk", new StringSerDes());

            var key = schema.ToBytes("product-1", "order-1");
            // length of the foreign key (4 bytes, big-endian) | foreign key | primary key
            CollectionAssert.AreEqual(new byte[] {0, 0, 0, 9}, key.Get.Take(4).ToArray());
            Assert.AreEqual("product-1order-1", Encoding.UTF8.GetString(key.Get.Skip(4).ToArray()));

            var (foreignKey, primaryKey) = schema.FromBytes(key);
            Assert.AreEqual("product-1", foreignKey);
            Assert.AreEqual("order-1", primaryKey);

            var prefix = schema.PrefixBytes("product-1");
            Assert.IsTrue(CombinedKeySchema<string, string>.StartsWith(key.Get, prefix.Get));
            // "product-10" has a different length, so a different prefix
            Assert.IsFalse(CombinedKeySchema<string, string>.StartsWith(schema.ToBytes("product-10", "o").Get, prefix.Get));
        }

        [Test]
        public void PrefixUpperBoundIsTheSmallestGreaterArray()
        {
            CollectionAssert.AreEqual(new byte[] {1, 3},
                CombinedKeySchema<string, string>.PrefixUpperBound(Bytes.Wrap(new byte[] {1, 2})).Get);
            // trailing 0xFF are dropped and the previous byte incremented
            CollectionAssert.AreEqual(new byte[] {2},
                CombinedKeySchema<string, string>.PrefixUpperBound(Bytes.Wrap(new byte[] {1, 0xFF, 0xFF})).Get);
            Assert.IsNull(CombinedKeySchema<string, string>.PrefixUpperBound(Bytes.Wrap(new byte[] {0xFF, 0xFF})));

            var comparer = new BytesComparer();
            var prefix = Bytes.Wrap(new byte[] {0, 0, 0, 1, 7});
            var upper = CombinedKeySchema<string, string>.PrefixUpperBound(prefix);
            Assert.Less(comparer.Compare(Bytes.Wrap(new byte[] {0, 0, 0, 1, 7, 0xFF, 0xFF, 0xFF}), upper), 0);
            Assert.Greater(comparer.Compare(Bytes.Wrap(new byte[] {0, 0, 0, 1, 8}), prefix), 0);
        }

        [Test]
        public void SubscriptionTopicsAreCopartitionedWithTheirTables()
        {
            // left table (orders) has 3 partitions and right table (products) 2: the registration topic (keyed by product)
            // gets 2 partitions and the response topic (keyed by order) 3
            var config = new StreamConfig<StringSerDes, StringSerDes> {ApplicationId = "fk-partitions"};
            var supplier = new MockKafkaSupplier(1, 0);
            var admin = supplier.GetAdmin(config.ToAdminConfig("admin"));
            admin.CreateTopicsAsync(new[]
            {
                new TopicSpecification {Name = "orders", NumPartitions = 3},
                new TopicSpecification {Name = "products", NumPartitions = 2}
            }).GetAwaiter().GetResult();

            var builder = new StreamBuilder();
            var products = builder.Table<string, string>("products");
            builder.Table<string, string>("orders")
                .Join(products, v => v.Split(':')[0], (o, p) => o + p, null, "fk")
                .ToStream()
                .To("output");
            var topology = builder.Build();
            topology.Builder.RewriteTopology(config);
            topology.Builder.BuildTopology(config);

            InternalTopicManagerUtils.New(config)
                .CreateInternalTopicsAsync(new DefaultTopicManager(config, admin), topology.Builder, false)
                .GetAwaiter().GetResult();

            int Partitions(string topic)
                => admin.GetMetadata(topic, TimeSpan.FromSeconds(1)).Topics.Single().Partitions.Count;

            Assert.AreEqual(2, Partitions("fk-partitions-fk-subscription-registration-topic"));
            Assert.AreEqual(3, Partitions("fk-partitions-fk-subscription-response-topic"));
            supplier.Destroy();
        }
    }
}
