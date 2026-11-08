using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using NUnit.Framework;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Kafka.Internal;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;

namespace Streamiz.Kafka.Net.Tests.Public
{
    public class ProcessingExceptionHandlerTests
    {
        private static StreamConfig<StringSerDes, StringSerDes> Config(Action<StreamConfig> configure = null)
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = $"test-processing-exception-handler-{Guid.NewGuid()}"
            };
            configure?.Invoke(config);
            return config;
        }

        private static StreamBuilder PoisonTopology()
        {
            var builder = new StreamBuilder();
            builder.Stream<string, string>("input")
                .Peek((_, v, _) =>
                {
                    if (v == "poison")
                        throw new InvalidOperationException("poison record");
                }, "poison-peek")
                .To("output");
            return builder;
        }

        private static string Header(Headers headers, string key)
            => Encoding.UTF8.GetString(headers.GetLastBytes(key));

        [Test]
        public void DefaultHandlerFails()
        {
            using var driver = new TopologyTestDriver(PoisonTopology().Build(), Config());
            var input = driver.CreateInputTopic<string, string>("input");

            Assert.Throws<InvalidOperationException>(() => input.PipeInput("k2", "poison"));
        }

        [Test]
        public void ContinueSkipsTheRecordAndGivesItsContext()
        {
            ErrorHandlerContext errorContext = null;
            Exception handledException = null;
            var config = Config(c => c.ProcessingExceptionHandler = (context, exception) =>
            {
                errorContext = context;
                handledException = exception;
                return ExceptionHandlerResponse.CONTINUE;
            });

            using var driver = new TopologyTestDriver(PoisonTopology().Build(), config);
            var input = driver.CreateInputTopic<string, string>("input");
            var output = driver.CreateOutputTopic<string, string>("output");

            input.PipeInput("k1", "a");
            input.PipeInput("k2", "poison");
            input.PipeInput("k3", "b");

            CollectionAssert.AreEqual(new[] {"a", "b"}, output.ReadValueList().ToList());
            Assert.IsInstanceOf<InvalidOperationException>(handledException);
            Assert.AreEqual("input", errorContext.Topic);
            Assert.AreEqual(0, errorContext.Partition);
            Assert.AreEqual(1, errorContext.Offset);
            Assert.AreEqual("poison-peek", errorContext.ProcessorNodeId);
            Assert.AreEqual("k2", Encoding.UTF8.GetString(errorContext.SourceRawKey));
            Assert.AreEqual("poison", Encoding.UTF8.GetString(errorContext.SourceRawValue));
            Assert.AreEqual(0, errorContext.TaskId.Id);
        }

        [Test]
        public void FailingHandlerRaisesTheProcessingException()
        {
            var config = Config(c => c.ProcessingExceptionHandler = (_, _) => throw new ArgumentException("handler bug"));

            using var driver = new TopologyTestDriver(PoisonTopology().Build(), config);
            var input = driver.CreateInputTopic<string, string>("input");

            Assert.Throws<InvalidOperationException>(() => input.PipeInput("k2", "poison"));
        }

        [Test]
        public void ContinueSendsTheRecordToTheDeadLetterQueue()
        {
            var config = Config(c =>
            {
                c.ProcessingExceptionHandler = (_, _) => ExceptionHandlerResponse.CONTINUE;
                c.DeadLetterQueueTopicName = "dlq";
            });

            using var driver = new TopologyTestDriver(PoisonTopology().Build(), config);
            var input = driver.CreateInputTopic<string, string>("input");
            var dlq = driver.CreateOutputTopic<string, string>("dlq");

            var headers = new Headers {{"origin", Encoding.UTF8.GetBytes("test")}};
            input.PipeInput("k1", "a");
            input.PipeInput("k2", "poison", DateTime.UtcNow, headers);

            var records = dlq.ReadKeyValueList().ToList();
            Assert.AreEqual(1, records.Count);
            var record = records[0];
            Assert.AreEqual("k2", record.Message.Key);
            Assert.AreEqual("poison", record.Message.Value);
            Assert.AreEqual("test", Header(record.Message.Headers, "origin"));
            Assert.AreEqual(typeof(InvalidOperationException).FullName,
                Header(record.Message.Headers, DeadLetterQueue.EXCEPTION_HEADER));
            Assert.AreEqual("poison record", Header(record.Message.Headers, DeadLetterQueue.EXCEPTION_MESSAGE_HEADER));
            StringAssert.Contains("poison record", Header(record.Message.Headers, DeadLetterQueue.EXCEPTION_STACKTRACE_HEADER));
            Assert.AreEqual("input", Header(record.Message.Headers, DeadLetterQueue.TOPIC_HEADER));
            Assert.AreEqual("0", Header(record.Message.Headers, DeadLetterQueue.PARTITION_HEADER));
            Assert.AreEqual("1", Header(record.Message.Headers, DeadLetterQueue.OFFSET_HEADER));
        }

        [Test]
        public void NoDeadLetterQueueWhenTheHandlerFails()
        {
            var config = Config(c => c.DeadLetterQueueTopicName = "dlq");

            using var driver = new TopologyTestDriver(PoisonTopology().Build(), config);
            var input = driver.CreateInputTopic<string, string>("input");
            var dlq = driver.CreateOutputTopic<string, string>("dlq");

            Assert.Throws<InvalidOperationException>(() => input.PipeInput("k2", "poison"));
            Assert.AreEqual(0, dlq.ReadKeyValueList().Count());
        }

        [Test]
        public void DeserializationContinueSendsTheRecordToTheDeadLetterQueue()
        {
            var config = Config(c =>
            {
                c.DeserializationExceptionHandler = (_, _, _) => ExceptionHandlerResponse.CONTINUE;
                c.DeadLetterQueueTopicName = "dlq";
            });

            var builder = new StreamBuilder();
            builder.Stream<string, int>("input", new StringSerDes(), new Int32SerDes()).To("output");

            using var driver = new TopologyTestDriver(builder.Build(), config);
            var input = driver.CreateInputTopic<string, string>("input"); // 2 bytes are not a valid int
            var dlq = driver.CreateOutputTopic<string, string>("dlq");

            input.PipeInput("k1", "ab");

            var records = dlq.ReadKeyValueList().ToList();
            Assert.AreEqual(1, records.Count);
            Assert.AreEqual("ab", records[0].Message.Value);
            Assert.AreEqual("0", Header(records[0].Message.Headers, DeadLetterQueue.OFFSET_HEADER));
        }

        [Test]
        public void AsyncSkipSendsTheRecordToTheDeadLetterQueue()
        {
            var config = Config(c =>
            {
                c.DeadLetterQueueTopicName = "dlq";
                c.PollMs = 10;
                c.CommitIntervalMs = 100;
            });

            var builder = new StreamBuilder();
            builder.Stream<string, string>("input")
                .MapValuesAsync<string>(async (record, _) =>
                    {
                        if (record.Value == "poison")
                            throw new StreamsException("external service error");
                        return await Task.FromResult(record.Value.ToUpper());
                    },
                    RetryPolicy.NewBuilder()
                        .RetriableException<StreamsException>()
                        .NumberOfRetry(1)
                        .RetryBackOffMs(1)
                        .RetryBehavior(EndRetryBehavior.SKIP)
                        .Build())
                .To("output");

            using var driver = new TopologyTestDriver(builder.Build(), config, TopologyTestDriver.Mode.ASYNC_CLUSTER_IN_MEMORY);
            var input = driver.CreateInputTopic<string, string>("input");
            var output = driver.CreateOutputTopic<string, string>("output");
            var dlq = driver.CreateOutputTopic<string, string>("dlq");

            input.PipeInput("k1", "poison");
            input.PipeInput("k2", "b");

            var results = IntegrationTestUtils.WaitUntilMinKeyValueRecordsReceived(output, 1);
            Assert.AreEqual("B", results[0].Message.Value);

            var dead = IntegrationTestUtils.WaitUntilMinKeyValueRecordsReceived(dlq, 1);
            Assert.AreEqual(1, dead.Count);
            Assert.AreEqual("k1", dead[0].Message.Key);
            Assert.AreEqual("poison", dead[0].Message.Value);
            Assert.AreEqual(typeof(NoneRetryableException).FullName,
                Header(dead[0].Message.Headers, DeadLetterQueue.EXCEPTION_HEADER));
        }
    }
}
