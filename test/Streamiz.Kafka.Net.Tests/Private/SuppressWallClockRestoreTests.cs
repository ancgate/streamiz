using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using Confluent.Kafka;
using NUnit.Framework;
using Streamiz.Kafka.Net.Metrics;
using Streamiz.Kafka.Net.Mock.Kafka;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;
using Streamiz.Kafka.Net.Tests.Helpers;
using ThreadState = Streamiz.Kafka.Net.Processors.ThreadState;

namespace Streamiz.Kafka.Net.Tests.Private
{
    /// <summary>
    /// A record buffered by UntilWallClockTimeLimit must still be emitted after a restart,
    /// when it is only restored from the suppress changelog and no new record arrives.
    /// </summary>
    public class SuppressWallClockRestoreTests
    {
        private static readonly TimeSpan WallClockLimit = TimeSpan.FromSeconds(3);

        private readonly StreamConfig<StringSerDes, StringSerDes> config = new();
        private MockKafkaSupplier supplier;
        private Topology topology;

        [SetUp]
        public void Init()
        {
            config.ApplicationId = "test-suppress-wall-clock-restore";
            config.StateDir = Path.Combine(".", Guid.NewGuid().ToString());
            config.Guarantee = ProcessingGuarantee.AT_LEAST_ONCE;
            config.PollMs = 10;
            config.CommitIntervalMs = 100;

            supplier = new MockKafkaSupplier(1, 0);

            var builder = new StreamBuilder();
            builder.Table("input", InMemory.As<string, string>("table-store"))
                .Suppress(SuppressedBuilder
                    .UntilWallClockTimeLimit<string, string>(WallClockLimit, StrictBufferConfig.Unbounded())
                    .WithKeySerdes(new StringSerDes())
                    .WithValueSerdes(new StringSerDes()))
                .ToStream()
                .To("output");

            topology = builder.Build();
            topology.Builder.RewriteTopology(config);
            topology.Builder.BuildTopology(config);

            var internalTopicManager = new DefaultTopicManager(config, supplier.GetAdmin(config.ToAdminConfig("admin")));
            InternalTopicManagerUtils
                .New(config)
                .CreateInternalTopicsAsync(internalTopicManager, topology.Builder, false).GetAwaiter().GetResult();
        }

        [TearDown]
        public void Dispose()
        {
            supplier.Destroy();
            if (Directory.Exists(config.StateDir))
                Directory.Delete(config.StateDir, true);
        }

        private StreamThread CreateThread(string name)
            => StreamThread.Create(
                name, Guid.NewGuid(), name,
                topology.Builder, new StreamMetricsRegistry(), config,
                supplier, supplier.GetAdmin(config.ToAdminConfig("admin")),
                new StatestoreRestoreManager(null),
                0) as StreamThread;

        private List<string> ReadOutputValues(TimeSpan timeout, int expected)
        {
            var serdes = new StringSerDes();
            var consumerConfig = config.ToConsumerConfig("output-reader");
            consumerConfig.GroupId = $"output-reader-{Guid.NewGuid()}";
            using var consumer = supplier.GetConsumer(consumerConfig, null);
            consumer.Subscribe("output");

            var values = new List<string>();
            var end = DateTime.Now + timeout;
            while (values.Count < expected && DateTime.Now < end)
            {
                var record = consumer.Consume(TimeSpan.FromMilliseconds(100));
                if (record != null)
                    values.Add(serdes.Deserialize(record.Message.Value, new SerializationContext()));
            }

            consumer.Unsubscribe();
            return values;
        }

        [Test]
        public void RestoredRecordIsEmittedWithoutNewInput()
        {
            var serdes = new StringSerDes();
            var producer = supplier.GetProducer(config.ToProducerConfig("producer"));

            // first run: the record is buffered (and logged to the suppress changelog on commit),
            // then the thread stops before the wall-clock limit is reached
            var token1 = new CancellationTokenSource();
            var thread1 = CreateThread("thread-0");
            thread1.Start(token1.Token);
            AssertExtensions.WaitUntil(() => thread1.State == ThreadState.RUNNING,
                TimeSpan.FromSeconds(5), TimeSpan.FromMilliseconds(20));

            producer.Produce(new TopicPartition("input", 0), new Message<byte[], byte[]>
            {
                Key = serdes.Serialize("key1", new SerializationContext()),
                Value = serdes.Serialize("value1", new SerializationContext())
            });

            Thread.Sleep(500); // processed and committed (commit interval 100ms)
            token1.Cancel();
            thread1.Dispose();

            Assert.AreEqual(0, ReadOutputValues(TimeSpan.FromMilliseconds(300), 1).Count,
                "nothing must be emitted before the wall-clock limit");

            // second run: the source offset is committed, so key1 is only known from the restored buffer
            var token2 = new CancellationTokenSource();
            var thread2 = CreateThread("thread-1");
            thread2.Start(token2.Token);
            try
            {
                var values = ReadOutputValues(WallClockLimit + TimeSpan.FromSeconds(5), 1);
                CollectionAssert.AreEqual(new[] {"value1"}, values);
            }
            finally
            {
                token2.Cancel();
                thread2.Dispose();
            }
        }
    }
}
