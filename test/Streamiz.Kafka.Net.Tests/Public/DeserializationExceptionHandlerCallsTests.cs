using System;
using System.Linq;
using Confluent.Kafka;
using NUnit.Framework;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.SerDes;

namespace Streamiz.Kafka.Net.Tests.Public
{
    /// <summary>
    /// The DeserializationExceptionHandler is called once per bad record, even though the record is also
    /// deserialized for the timestamp extractor.
    /// </summary>
    public class DeserializationExceptionHandlerCallsTests
    {
        private class ValueReadingTimestampExtractor : ITimestampExtractor
        {
            public long Extract(ConsumeResult<object, object> record, long partitionTime)
                => record.Message.Value is int value ? value : record.Message.Timestamp.UnixTimestampMs;
        }

        private static StreamBuilder Topology()
        {
            var builder = new StreamBuilder();
            builder.Stream<string, int>("input", new StringSerDes(), new Int32SerDes()).To("output");
            return builder;
        }

        [TestCase(false)]
        [TestCase(true)]
        public void ContinueCallsTheHandlerOnceAndSendsTheRecordOnceToTheDeadLetterQueue(bool customExtractor)
        {
            int handlerCalls = 0;
            var config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = $"test-deserialization-handler-calls-{Guid.NewGuid()}",
                DeadLetterQueueTopicName = "dlq",
                DeserializationExceptionHandler = (_, _, _) =>
                {
                    ++handlerCalls;
                    return ExceptionHandlerResponse.CONTINUE;
                }
            };
            if (customExtractor)
                config.DefaultTimestampExtractor = new ValueReadingTimestampExtractor();

            using var driver = new TopologyTestDriver(Topology().Build(), config);
            var input = driver.CreateInputTopic<string, string>("input");
            var dlq = driver.CreateOutputTopic<string, string>("dlq");

            input.PipeInput("k1", "ab"); // 2 bytes: not a valid int

            Assert.AreEqual(1, handlerCalls);
            Assert.AreEqual(1, dlq.ReadKeyValueList().Count());
        }

        [TestCase(false)]
        [TestCase(true)]
        public void FailCallsTheHandlerOnceAndRaisesTheError(bool customExtractor)
        {
            int handlerCalls = 0;
            var config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = $"test-deserialization-handler-fail-{Guid.NewGuid()}",
                DeserializationExceptionHandler = (_, _, _) =>
                {
                    ++handlerCalls;
                    return ExceptionHandlerResponse.FAIL;
                }
            };
            if (customExtractor)
                config.DefaultTimestampExtractor = new ValueReadingTimestampExtractor();

            using var driver = new TopologyTestDriver(Topology().Build(), config);
            var input = driver.CreateInputTopic<string, string>("input");

            Assert.Throws<DeserializationException>(() => input.PipeInput("k1", "ab"));
            Assert.AreEqual(1, handlerCalls);
        }
    }
}
