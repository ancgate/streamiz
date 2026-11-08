using NUnit.Framework;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Streamiz.Kafka.Net.Tests.Processors
{
    public class KStreamAlterRecordTimestampTests
    {
        [Test]
        public void ShouldNotAllowNullTimestampExtractorAction()
        {
            var builder = new StreamBuilder();
            var stream = builder.Stream<string, string>("topic");

            Assert.Throws<ArgumentNullException>(() => stream.AlterRecordTimestamp(null));
        }

        [Test]
        public void ShouldIgnoreNegativeTimestamp()
        {
            var builder = new StreamBuilder();
            var data = new List<KeyValuePair<string, string>>();
            var negativeTimestamp = -12345;

            data.Add(KeyValuePair.Create("key1", "123456"));

            builder.Stream<string, string>("topic")
                .AlterRecordTimestamp((k, v, _) => negativeTimestamp)
                .To("output-topic");

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-flatmap";

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("topic");
                var outputTopic = driver.CreateOutputTopic<string, string, StringSerDes, StringSerDes>("output-topic");

                inputTopic.PipeInputs(data);
                var result = outputTopic.ReadKeyValueList().ToList();

                Assert.IsNotNull(result);
                Assert.IsTrue(result.Count == 1);
                var record = result[0];
                Assert.IsTrue(record.Message.Timestamp.UnixTimestampMs > 0);
            }
        }

        [TestCase(0)]
        [TestCase(12345)]
        [TestCase(int.MaxValue)]
        public void ShouldReturnMessageWithExtractedTimestamp(long customTimestamp)
        {
            var builder = new StreamBuilder();
            var data = new List<KeyValuePair<string, string>>();

            data.Add(KeyValuePair.Create("key1", "123456"));

            builder.Stream<string, string>("topic")
                .AlterRecordTimestamp((k, v, _) => customTimestamp)
                .To("output-topic");

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-flatmap";

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("topic");
                var outputTopic = driver.CreateOutputTopic<string, string, StringSerDes, StringSerDes>("output-topic");

                inputTopic.PipeInputs(data);
                var result = outputTopic.ReadKeyValueList().ToList();

                Assert.IsNotNull(result);
                Assert.IsTrue(result.Count == 1);
                var record = result[0];
                Assert.AreEqual(customTimestamp, record.Message.Timestamp.UnixTimestampMs);
            }
        }

        [Test]
        public void ObsoleteWithRecordTimestampStillAltersTimestamp()
        {
            var builder = new StreamBuilder();

#pragma warning disable CS0618 // kept for backward compatibility
            builder.Stream<string, string>("topic")
                .WithRecordTimestamp((k, v, _) => 12345)
                .To("output-topic");
#pragma warning restore CS0618

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-obsolete-with-record-timestamp";

            using var driver = new TopologyTestDriver(builder.Build(), config);
            var inputTopic = driver.CreateInputTopic<string, string>("topic");
            var outputTopic = driver.CreateOutputTopic<string, string, StringSerDes, StringSerDes>("output-topic");

            inputTopic.PipeInput("key1", "123456");
            var result = outputTopic.ReadKeyValueList().ToList();

            Assert.AreEqual(1, result.Count);
            Assert.AreEqual(12345, result[0].Message.Timestamp.UnixTimestampMs);
        }
    }
}
