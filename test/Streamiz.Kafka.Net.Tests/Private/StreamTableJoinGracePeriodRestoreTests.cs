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
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;
using Streamiz.Kafka.Net.Tests.Helpers;
using ThreadState = Streamiz.Kafka.Net.Processors.ThreadState;

namespace Streamiz.Kafka.Net.Tests.Private
{
    /// <summary>
    /// A stream record buffered by a stream-table join with a grace period (KIP-923) must survive a restart:
    /// the buffer is restored from its changelog and the record is joined when the stream time passes its deadline.
    /// </summary>
    public class StreamTableJoinGracePeriodRestoreTests
    {
        private readonly StreamConfig<StringSerDes, StringSerDes> config = new();
        private MockKafkaSupplier supplier;
        private Topology topology;

        [SetUp]
        public void Init()
        {
            config.ApplicationId = "test-stream-table-join-grace-restore";
            config.StateDir = Path.Combine(".", Guid.NewGuid().ToString());
            config.Guarantee = ProcessingGuarantee.AT_LEAST_ONCE;
            config.PollMs = 10;
            config.CommitIntervalMs = 100;

            supplier = new MockKafkaSupplier(1, 0);

            var builder = new StreamBuilder();
            var table = builder.Table("table", InMemoryVersioned.As<string, string>("table-store", TimeSpan.FromSeconds(10)));
            builder.Stream<string, string>("stream")
                .Join(table, (s, t) => $"{s}-{t}",
                    new StreamTableJoinProps<string, string, string>(new StringSerDes(), new StringSerDes(), new StringSerDes())
                        .WithGracePeriod(TimeSpan.FromSeconds(2)))
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

        private void Produce(IProducer<byte[], byte[]> producer, string topic, string key, string value, long timestamp)
        {
            var serdes = new StringSerDes();
            producer.Produce(new TopicPartition(topic, 0), new Message<byte[], byte[]>
            {
                Key = serdes.Serialize(key, new SerializationContext()),
                Value = serdes.Serialize(value, new SerializationContext()),
                Timestamp = new Timestamp(timestamp, TimestampType.CreateTime)
            });
        }

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
        public void BufferedRecordIsJoinedAfterARestart()
        {
            var producer = supplier.GetProducer(config.ToProducerConfig("producer"));

            // first run: s1 (2 000) is buffered until the stream time reaches 4 000
            var token1 = new CancellationTokenSource();
            var thread1 = CreateThread("thread-0");
            thread1.Start(token1.Token);
            AssertExtensions.WaitUntil(() => thread1.State == ThreadState.RUNNING,
                TimeSpan.FromSeconds(5), TimeSpan.FromMilliseconds(20));

            Produce(producer, "table", "k", "t1", 1_000);
            Thread.Sleep(300);
            Produce(producer, "stream", "k", "s1", 2_000);

            Thread.Sleep(500); // processed and committed (commit interval 100ms)
            token1.Cancel();
            thread1.Dispose();

            Assert.AreEqual(0, ReadOutputValues(TimeSpan.FromMilliseconds(300), 1).Count,
                "s1 must still be in the buffer");

            // second run: the source offsets are committed, so s1 is only known from the restored buffer
            var token2 = new CancellationTokenSource();
            var thread2 = CreateThread("thread-1");
            thread2.Start(token2.Token);
            try
            {
                AssertExtensions.WaitUntil(() => thread2.State == ThreadState.RUNNING,
                    TimeSpan.FromSeconds(5), TimeSpan.FromMilliseconds(20));
                Produce(producer, "stream", "k", "s2", 5_000); // deadline 3 000: s1 is due

                var values = ReadOutputValues(TimeSpan.FromSeconds(5), 1);
                CollectionAssert.AreEqual(new[] {"s1-t1"}, values);
            }
            finally
            {
                token2.Cancel();
                thread2.Dispose();
            }
        }
    }
}
