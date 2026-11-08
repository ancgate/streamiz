using System.Text;
using Confluent.Kafka;
using NUnit.Framework;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Processors.Internal;

namespace Streamiz.Kafka.Net.Tests.Private
{
    public class RecordContextTests
    {
        [Test]
        public void NullHeadersHaveNoSizeAndSerialize()
        {
            var context = new RecordContext(null, 10, 20, 1, "topic");

            Assert.AreEqual(0, ((Headers) null).GetEstimatedSize());
            Assert.AreEqual(sizeof(int) + sizeof(long) + sizeof(long) + "topic".Length, context.MemorySizeEstimate);

            var restored = RecordContext.Deserialize(ByteBuffer.Build(context.Serialize(), true));
            Assert.AreEqual(10, restored.Offset);
            Assert.AreEqual(20, restored.Timestamp);
            Assert.AreEqual(1, restored.Partition);
            Assert.AreEqual("topic", restored.Topic);
            Assert.AreEqual(0, restored.Headers.Count);
        }

        [Test]
        public void HeadersWithNonAsciiKeyAndNullValueRoundTrip()
        {
            var headers = new Headers();
            headers.Add("clé-é", Encoding.UTF8.GetBytes("value"));
            headers.Add("empty", null);
            var context = new RecordContext(headers, 10, 20, 1, "topic");

            Assert.AreEqual("clé-é".Length + 5 + "empty".Length, headers.GetEstimatedSize());

            var restored = RecordContext.Deserialize(ByteBuffer.Build(context.Serialize(), true));
            Assert.AreEqual(2, restored.Headers.Count);
            Assert.AreEqual("clé-é", restored.Headers[0].Key);
            Assert.AreEqual("value", Encoding.UTF8.GetString(restored.Headers[0].GetValueBytes()));
            Assert.AreEqual("empty", restored.Headers[1].Key);
            Assert.IsNull(restored.Headers[1].GetValueBytes());
        }
    }
}
