using System;
using System.Collections.Concurrent;
using System.IO;
using System.Linq;
using System.Threading;
using Confluent.Kafka;
using NUnit.Framework;
using Streamiz.Kafka.Net.Metrics;
using Streamiz.Kafka.Net.Mock.Kafka;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Tests.Helpers;
using ThreadState = Streamiz.Kafka.Net.Processors.ThreadState;

namespace Streamiz.Kafka.Net.Tests.Private
{
    /// <summary>
    /// A stream thread dies while processing a record (InnerExceptionHandler FAIL), then a new thread
    /// starts on the same cluster. With at-most-once the whole poll was committed before processing,
    /// so nothing is processed again; with at-least-once the failing record is processed again.
    /// </summary>
    public class AtMostOnceProcessingTests
    {
        private StreamConfig<StringSerDes, StringSerDes> config;
        private MockKafkaSupplier supplier;
        private Topology topology;
        private ConcurrentQueue<string> processed;

        private void Setup(ProcessingGuarantee guarantee)
        {
            processed = new ConcurrentQueue<string>();
            config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = $"test-at-most-once-{Guid.NewGuid()}",
                StateDir = Path.Combine(".", Guid.NewGuid().ToString()),
                Guarantee = guarantee,
                PollMs = 100,
                InnerExceptionHandler = _ => ExceptionHandlerResponse.FAIL
            };

            supplier = new MockKafkaSupplier(1, 0);

            var builder = new StreamBuilder();
            builder.Stream<string, string>("input")
                .Peek((_, v, _) =>
                {
                    processed.Enqueue(v);
                    if (v == "poison")
                        throw new InvalidOperationException("poison record");
                })
                .To("output");

            topology = builder.Build();
            topology.Builder.RewriteTopology(config);
            topology.Builder.BuildTopology(config);
        }

        [TearDown]
        public void Dispose()
        {
            supplier?.Destroy();
            if (config != null && Directory.Exists(config.StateDir))
                Directory.Delete(config.StateDir, true);
        }

        private void RunThreadUntilDeadOrIdle(string name)
        {
            var token = new CancellationTokenSource();
            var thread = StreamThread.Create(
                name, Guid.NewGuid(), name,
                topology.Builder, new StreamMetricsRegistry(), config,
                supplier, supplier.GetAdmin(config.ToAdminConfig("admin")),
                new StatestoreRestoreManager(null),
                0) as StreamThread;

            thread.Start(token.Token);
            // the thread dies on the poison record, or stays alive when there is nothing (more) to process
            AssertExtensions.WaitUntil(() => thread.State == ThreadState.DEAD,
                TimeSpan.FromSeconds(5), TimeSpan.FromMilliseconds(20));
            token.Cancel();
            thread.Dispose();
        }

        private void ProduceInput()
        {
            var serdes = new StringSerDes();
            var producer = supplier.GetProducer(config.ToProducerConfig("producer"));
            foreach (var value in new[] {"v1", "poison", "v2", "v3"})
                producer.Produce(new TopicPartition("input", 0), new Message<byte[], byte[]>
                {
                    Key = serdes.Serialize("key", new SerializationContext()),
                    Value = serdes.Serialize(value, new SerializationContext())
                });
        }

        [Test]
        public void AtMostOnceDoesNotProcessRecordsTwiceAfterACrash()
        {
            Setup(ProcessingGuarantee.AT_MOST_ONCE);
            ProduceInput();

            RunThreadUntilDeadOrIdle("thread-0");
            CollectionAssert.AreEqual(new[] {"v1", "poison"}, processed.ToArray());

            // the 4 polled records were committed before being processed: v2 and v3 are lost, poison is not retried
            RunThreadUntilDeadOrIdle("thread-1");
            CollectionAssert.AreEqual(new[] {"v1", "poison"}, processed.ToArray());
        }

        [Test]
        public void AtLeastOnceProcessesTheFailingRecordAgainAfterACrash()
        {
            Setup(ProcessingGuarantee.AT_LEAST_ONCE);
            ProduceInput();

            RunThreadUntilDeadOrIdle("thread-0");
            CollectionAssert.AreEqual(new[] {"v1", "poison"}, processed.ToArray());

            RunThreadUntilDeadOrIdle("thread-1");
            Assert.AreEqual(2, processed.Count(v => v == "poison"));
        }
    }
}
