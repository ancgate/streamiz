using System;
using System.Linq;
using NUnit.Framework;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;

namespace Streamiz.Kafka.Net.Tests.TestDriver
{
    /// <summary>
    /// Topics in the default (synchronous) TopologyTestDriver: partition counts of internal topics,
    /// and the size of an output topic.
    /// </summary>
    public class SyncDriverTopicsTests
    {
        private static StreamConfig<StringSerDes, StringSerDes> Config(string id)
            => new() {ApplicationId = id};

        [Test]
        [Timeout(30000)]
        public void RepartitionWithSeveralPartitions()
        {
            var builder = new StreamBuilder();
            builder.Stream<string, string>("input")
                .SelectKey((k, v, _) => v)
                .Repartition(Repartitioned<string, string>.As("by-value").WithNumberOfPartitions(4))
                .To("output");

            // the repartition topic is created with 4 partitions: it used to be created with 1, so its
            // validation failed after about 48 seconds of retries
            var start = DateTime.Now;
            using var driver = new TopologyTestDriver(builder.Build(), Config("test-sync-repartition-partitions"));
            var input = driver.CreateInputTopic<string, string>("input");
            var output = driver.CreateOutputTopic<string, string>("output");

            input.PipeInput("k1", "a");
            input.PipeInput("k2", "b");

            var records = output.ReadKeyValueList().Select(r => (r.Message.Key, r.Message.Value)).ToList();
            CollectionAssert.AreEqual(new[] {("a", "a"), ("b", "b")}, records);
            Assert.Less((DateTime.Now - start).TotalSeconds, 10);
        }

        [Test]
        public void OutputTopicSizeInSynchronousMode()
        {
            var builder = new StreamBuilder();
            builder.Stream<string, string>("input").To("output");

            using var driver = new TopologyTestDriver(builder.Build(), Config("test-sync-output-size"));
            var input = driver.CreateInputTopic<string, string>("input");
            var output = driver.CreateOutputTopic<string, string>("output");

            Assert.IsTrue(output.IsEmpty);
            Assert.AreEqual(0, output.QueueSize);

            input.PipeInput("k1", "a");
            input.PipeInput("k2", "b");
            Assert.IsFalse(output.IsEmpty);
            Assert.AreEqual(2, output.QueueSize);

            output.ReadKeyValue();
            Assert.AreEqual(1, output.QueueSize);

            output.ReadKeyValue();
            Assert.IsTrue(output.IsEmpty);
            Assert.AreEqual(0, output.QueueSize);
        }
    }
}
