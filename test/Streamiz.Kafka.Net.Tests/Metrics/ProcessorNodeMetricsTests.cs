using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Confluent.Kafka;
using NUnit.Framework;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Metrics;
using Streamiz.Kafka.Net.Metrics.Internal;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.SerDes;

namespace Streamiz.Kafka.Net.Tests.Metrics
{
    public class ProcessorNodeMetricsTests
    {
        private StreamMetricsRegistry streamMetricsRegistry = null;

        private readonly StreamConfig<StringSerDes, StringSerDes> config =
            new StreamConfig<StringSerDes, StringSerDes>();

        private string threadId = StreamMetricsRegistry.UNKNOWN_THREAD;
        private string processorNodeName = "source-processor";
        private TopicPartition topicPartition;
        private TaskId id;
        private SourceProcessor<string, string> sourceProcessor;
        private ProcessorContext context;

        [SetUp]
        public void Initialize()
        {
            streamMetricsRegistry
                = new StreamMetricsRegistry(Guid.NewGuid().ToString(),
                    MetricsRecordingLevel.DEBUG);
            
            config.ApplicationId = "test-stream-thread";
            config.StateDir = Guid.NewGuid().ToString();
            config.Guarantee = ProcessingGuarantee.AT_LEAST_ONCE;
            config.PollMs = 10;
            config.CommitIntervalMs = 1;
            
            
            id = new TaskId {Id = 0, Partition = 0};
            topicPartition = new TopicPartition("topic", 0);

            sourceProcessor = new SourceProcessor<string, string>(
                processorNodeName,
                "topic",
                new StringSerDes(),
                new StringSerDes(),
                new FailOnInvalidTimestamp());
            sourceProcessor.AddNextProcessor(new KStreamPeekProcessor<string, string>((k, v, _) => { }, false));

            context = new ProcessorContext(
                UnassignedStreamTask.Create(id),
                config,
                new ProcessorStateManager(
                    id,
                    new List<TopicPartition> {topicPartition},
                    null,
                    new StoreChangelogReader(config, null, threadId, new StatestoreRestoreManager(null), streamMetricsRegistry),
                    new MockOffsetCheckpointManager()
                ), streamMetricsRegistry);
            
            sourceProcessor.Init(context);
        }
        
        [TearDown]
        public void Dispose()
        {
        }
        
        [Test]
        public void ProcessorNodeMetricsTest()
        {
            var serdes = new StringSerDes();
            var rd = new Random();
            int nbMessage = rd.Next(0, 4000);
            
            // simulate processing message
            for (int i = 0; i < nbMessage; ++i)
            {
                string key = "key" + i, value = "value" + i;
                var r = new ConsumeResult<byte[], byte[]>()
                {
                    Message = new Message<byte[], byte[]>()
                    {
                        Key = serdes.Serialize(key, new SerializationContext()),
                        Value = serdes.Serialize(value, new SerializationContext()),
                        Timestamp = new Timestamp(DateTime.Now)
                    },
                    Offset = i,
                    IsPartitionEOF = false,
                    Topic = topicPartition.Topic,
                    Partition = topicPartition.Partition
                };
                
                context.SetRecordMetaData(r);
                sourceProcessor.Process(key, value);
            }
            
            long now = DateTime.Now.GetMilliseconds();
            var sensors = streamMetricsRegistry.GetThreadScopeSensor(threadId);
           
            var processorSensor = sourceProcessor.ProcessSensor;
            Assert.AreEqual(2, processorSensor.Metrics.Count());
            Assert.AreEqual(nbMessage,  
                processorSensor.Metrics[MetricName.NameAndGroup(
                    ProcessorNodeMetrics.PROCESS + StreamMetricsRegistry.TOTAL_SUFFIX, 
                    StreamMetricsRegistry.PROCESSOR_NODE_LEVEL_GROUP)].Value);
            Assert.IsTrue(
                (double)processorSensor.Metrics[MetricName.NameAndGroup(
                    ProcessorNodeMetrics.PROCESS + StreamMetricsRegistry.RATE_SUFFIX, 
                    StreamMetricsRegistry.PROCESSOR_NODE_LEVEL_GROUP)].Value > 0d);
        }

        [Test]
        public void RecordEndToEndLatencyOnSourceAndTerminalNodesTest()
        {
            var registry = new StreamMetricsRegistry(Guid.NewGuid().ToString(), MetricsRecordingLevel.INFO);
            var source = new SourceProcessor<string, string>("e2e-source", "topic",
                new StringSerDes(), new StringSerDes(), new FailOnInvalidTimestamp());
            var filter = new KStreamFilterProcessor<string, string>((_, _, _) => true, false) {Name = "e2e-filter"};
            var peek = new KStreamPeekProcessor<string, string>((_, _, _) => { }, false) {Name = "e2e-peek"};
            source.AddNextProcessor(filter);
            filter.AddNextProcessor(peek);

            var ctx = new ProcessorContext(
                UnassignedStreamTask.Create(id),
                config,
                new ProcessorStateManager(
                    id,
                    new List<TopicPartition> {topicPartition},
                    null,
                    new StoreChangelogReader(config, null, threadId, new StatestoreRestoreManager(null), registry),
                    new MockOffsetCheckpointManager()
                ), registry);
            source.Init(ctx);

            var serdes = new StringSerDes();
            long now = DateTime.Now.GetMilliseconds();
            // records are 1s, 3s and 5s old
            foreach (var ageMs in new[] {1000L, 3000L, 5000L})
            {
                var r = new ConsumeResult<byte[], byte[]>
                {
                    Message = new Message<byte[], byte[]>
                    {
                        Key = serdes.Serialize("key", new SerializationContext()),
                        Value = serdes.Serialize("value", new SerializationContext()),
                        Timestamp = new Timestamp(now - ageMs, TimestampType.CreateTime)
                    },
                    Offset = ageMs,
                    Topic = topicPartition.Topic,
                    Partition = topicPartition.Partition
                };
                ctx.SetRecordMetaData(r);
                source.Process("key", "value");
            }

            // sensors are registered with the name of the thread which initialized the processors
            Sensor E2ESensor(string node) => registry
                .GetThreadScopeSensor(Thread.CurrentThread.Name ?? StreamMetricsRegistry.UNKNOWN_THREAD)
                .SingleOrDefault(s => s.Name.Contains(node) && s.Name.EndsWith(ProcessorNodeMetrics.RECORD_E2E_LATENCY));

            double Metric(Sensor sensor, string suffix) => (double) sensor.Metrics[MetricName.NameAndGroup(
                ProcessorNodeMetrics.RECORD_E2E_LATENCY + suffix,
                StreamMetricsRegistry.PROCESSOR_NODE_LEVEL_GROUP)].Value;

            // a node with children which is not a source does not record the end-to-end latency
            Assert.IsNull(E2ESensor("e2e-filter"));

            // allow some slack for the time spent in the test itself
            const double slack = 2000;
            foreach (var node in new[] {"e2e-source", "e2e-peek"})
            {
                var sensor = E2ESensor(node);
                Assert.IsNotNull(sensor, node);
                Assert.AreEqual(3, sensor.Metrics.Count());
                Assert.That(Metric(sensor, StreamMetricsRegistry.MIN_SUFFIX), Is.InRange(1000d, 1000d + slack));
                Assert.That(Metric(sensor, StreamMetricsRegistry.AVG_SUFFIX), Is.InRange(3000d, 3000d + slack));
                Assert.That(Metric(sensor, StreamMetricsRegistry.MAX_SUFFIX), Is.InRange(5000d, 5000d + slack));
            }
        }
    }
}