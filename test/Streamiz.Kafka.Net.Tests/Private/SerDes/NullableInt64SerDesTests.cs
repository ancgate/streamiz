using System;
using Confluent.Kafka;
using NUnit.Framework;
using Streamiz.Kafka.Net.SerDes;

namespace Streamiz.Kafka.Net.Tests.Private.SerDes
{
    public class NullableInt64SerDesTests
    {
        [Test]
        public void NullIsSerializedAsNullBytes()
        {
            var serdes = new NullableInt64SerDes();
            Assert.IsNull(serdes.Serialize(null, new SerializationContext()));
            Assert.IsNull(serdes.Deserialize(null, new SerializationContext()));
        }

        [TestCase(0L)]
        [TestCase(42L)]
        [TestCase(long.MinValue)]
        [TestCase(long.MaxValue)]
        public void ValuesRoundTripLikeInt64SerDes(long value)
        {
            var serdes = new NullableInt64SerDes();
            var bytes = serdes.Serialize(value, new SerializationContext());

            CollectionAssert.AreEqual(new Int64SerDes().Serialize(value, new SerializationContext()), bytes);
            Assert.AreEqual(value, serdes.Deserialize(bytes, new SerializationContext()));
        }
    }
}
