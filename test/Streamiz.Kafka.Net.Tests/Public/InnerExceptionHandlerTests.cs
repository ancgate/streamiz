using Confluent.Kafka;
using NUnit.Framework;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.Mock.Sync;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.SerDes;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
namespace Streamiz.Kafka.Net.Tests.Public
{
    public class InnerExceptionHandlerTests
    {
        private class PoisonTimestampExtractor : ITimestampExtractor
        {
            public int PoisonCalls;

            public long Extract(ConsumeResult<object, object> record, long partitionTime)
            {
                if ("poison".Equals(record.Message.Value))
                {
                    Interlocked.Increment(ref PoisonCalls);
                    throw new InvalidOperationException("poison record");
                }
                return record.Message.Timestamp.UnixTimestampMs;
            }
        }

        private class FailTimestampExtractor : ITimestampExtractor
        {
            public long Extract(ConsumeResult<object, object> record, long partitionTime)
            {
                throw new NotImplementedException();
            }
        }

        [Test]
        public async Task KafkaStreamInnerExceptionHandlerContinueTest()
        {
            var _return = new List<KeyValuePair<string, string>>();

            var timeout = TimeSpan.FromSeconds(10);

            bool isRunningState = false;
            DateTime dt = DateTime.Now;

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test";
            config.BootstrapServers = "127.0.0.1";
            config.PollMs = 10;
            config.InnerExceptionHandler += (e) => ExceptionHandlerResponse.CONTINUE;
            config.DefaultTimestampExtractor = new FailTimestampExtractor();

            var supplier = new SyncKafkaSupplier();
            var producer = supplier.GetProducer(config.ToProducerConfig());

            var builder = new StreamBuilder();
            builder
                .Stream<string, string>("test")
                .Peek((k, v, _) => _return.Add(KeyValuePair.Create(k, v)));

            var t = builder.Build();
            var stream = new KafkaStream(t, config, supplier);

            stream.StateChanged += (old, @new) =>
            {
                if (@new.Equals(KafkaStream.State.RUNNING))
                {
                    isRunningState = true;
                }
            };
            await stream.StartAsync();
            while (!isRunningState)
            {
                Thread.Sleep(250);
                if (DateTime.Now > dt + timeout)
                {
                    break;
                }
            }
            Assert.IsTrue(isRunningState);

            if (isRunningState)
            {
                var serdes = new StringSerDes();
                dt = DateTime.Now;
                producer.Produce("test",
                    new Confluent.Kafka.Message<byte[], byte[]>
                    {
                        Key = serdes.Serialize("k", new SerializationContext()),
                        Value = serdes.Serialize("test4", new SerializationContext()),
                        Timestamp = new Confluent.Kafka.Timestamp(dt)
                    });
                Thread.Sleep(1000);
                var expected = new List<KeyValuePair<string, string>>();
                Assert.AreEqual(expected, _return);
            }

            stream.Dispose();
        }


        private static List<string> RunPoisonRecordScenario(Action<StreamConfig> configure, Action<StreamBuilder> build)
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = $"test-poison-{Guid.NewGuid()}";
            config.BootstrapServers = "127.0.0.1";
            config.PollMs = 10;
            config.CommitIntervalMs = 100;
            configure(config);

            var builder = new StreamBuilder();
            build(builder);

            using var driver = new TopologyTestDriver(builder.Build(), config, TopologyTestDriver.Mode.ASYNC_CLUSTER_IN_MEMORY);
            var input = driver.CreateInputTopic<string, string>("test");
            var output = driver.CreateOutputTopic<string, string>("output");
            input.PipeInput("k1", "a");
            input.PipeInput("k2", "poison");
            input.PipeInput("k3", "b");

            var values = new List<string>();
            var end = DateTime.Now.AddSeconds(10);
            while (!values.Contains("b") && DateTime.Now < end)
            {
                foreach (var r in output.ReadKeyValueList())
                    values.Add(r.Message.Value);
                Thread.Sleep(50);
            }

            // give the thread time to show whether it keeps replaying the poison record
            Thread.Sleep(1000);
            foreach (var r in output.ReadKeyValueList())
                values.Add(r.Message.Value);
            return values;
        }

        [Test]
        public void InnerExceptionHandlerContinueSkipsRecordThatFailsInProcessor()
        {
            int poisonCalls = 0, handlerCalls = 0;

            var values = RunPoisonRecordScenario(
                config => config.InnerExceptionHandler = _ =>
                {
                    Interlocked.Increment(ref handlerCalls);
                    return ExceptionHandlerResponse.CONTINUE;
                },
                builder => builder
                    .Stream<string, string>("test")
                    .Peek((_, v, _) =>
                    {
                        if (v == "poison")
                        {
                            Interlocked.Increment(ref poisonCalls);
                            throw new InvalidOperationException("poison record");
                        }
                    })
                    .To("output"));

            CollectionAssert.Contains(values, "a");
            CollectionAssert.Contains(values, "b");
            CollectionAssert.DoesNotContain(values, "poison");
            Assert.AreEqual(1, poisonCalls);
            Assert.AreEqual(1, handlerCalls);
        }

        [Test]
        public void InnerExceptionHandlerContinueSkipsRecordThatFailsInTimestampExtractor()
        {
            int handlerCalls = 0;
            var extractor = new PoisonTimestampExtractor();

            var values = RunPoisonRecordScenario(
                config =>
                {
                    config.DefaultTimestampExtractor = extractor;
                    config.InnerExceptionHandler = _ =>
                    {
                        Interlocked.Increment(ref handlerCalls);
                        return ExceptionHandlerResponse.CONTINUE;
                    };
                },
                builder => builder
                    .Stream<string, string>("test")
                    .To("output"));

            CollectionAssert.Contains(values, "a");
            CollectionAssert.Contains(values, "b");
            CollectionAssert.DoesNotContain(values, "poison");
            Assert.AreEqual(1, extractor.PoisonCalls);
            Assert.AreEqual(1, handlerCalls);
        }


        [Test]
        public void KafkaStreamInnerExceptionHandlerFailTest()
        {
            bool errorState = false;
            var _return = new List<KeyValuePair<string, string>>();
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            var dt = DateTime.Now;
            var timeout = TimeSpan.FromSeconds(10);

            config.ApplicationId = "test";
            config.BootstrapServers = "127.0.0.1";
            config.PollMs = 10;
            config.InnerExceptionHandler += (e) => ExceptionHandlerResponse.FAIL;
            config.DefaultTimestampExtractor = new FailTimestampExtractor();

            var builder = new StreamBuilder();
            builder
                .Stream<string, string>("test")
                .Peek((k, v, _) => _return.Add(KeyValuePair.Create(k, v)));


            var t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config, TopologyTestDriver.Mode.ASYNC_CLUSTER_IN_MEMORY))
            {
                var inputtopic = driver.CreateInputTopic<string, string>("test");
                inputtopic.PipeInput("coucou");
                while (!errorState)
                {
                    errorState = driver.IsError;
                    Thread.Sleep(10);
                    if (DateTime.Now > dt + timeout)
                    {
                        break;
                    }
                }
                Assert.IsTrue(driver.IsError);
            }

            Assert.AreEqual(new List<KeyValuePair<string, string>>(), _return);
        }
    }
}