using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Kafka;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Processors.Internal;

namespace Streamiz.Kafka.Net.Processors.Internal
{
    /// <summary>
    /// Shares the metadata of the instances of an application (interactive queries) through the compacted topic
    /// <c>&lt;application.id&gt;-streamiz-metadata</c> : a background thread publishes the tasks of this instance (when they
    /// change, and every <see cref="IStreamConfig.MetadataHeartbeatIntervalMs"/>) and reads the metadata of the other instances.
    /// It only runs when <see cref="IStreamConfig.ApplicationServer"/> is set; otherwise only the local instance is known.
    /// </summary>
    internal class StreamsMetadataManager : IDisposable
    {
        internal const string METADATA_TOPIC_SUFFIX = "-streamiz-metadata";

        private readonly ILogger log = Logger.GetLogger(typeof(StreamsMetadataManager));
        private readonly string logPrefix;
        private readonly string instanceId;
        private readonly HostInfo host;
        private readonly IStreamConfig config;
        private readonly IKafkaSupplier kafkaSupplier;
        private readonly string clientId;
        private readonly IEnumerable<IThread> threads;
        private readonly ConcurrentDictionary<string, int> partitionCounts = new();
        private readonly TimeSpan heartbeatInterval;

        private Thread thread;
        private volatile bool running;
        private IAdminClient adminClient;
        private InstanceMetadata lastPublished;
        private DateTime lastPublication = DateTime.MinValue;

        internal StreamsMetadataManager(
            string instanceId,
            IStreamConfig config,
            IKafkaSupplier kafkaSupplier,
            string clientId,
            InternalTopologyBuilder builder,
            IEnumerable<IThread> threads)
        {
            this.instanceId = instanceId;
            this.config = config;
            this.kafkaSupplier = kafkaSupplier;
            this.clientId = clientId;
            this.threads = threads;
            host = HostInfo.Parse(config.ApplicationServer);
            heartbeatInterval = TimeSpan.FromMilliseconds(Math.Max(1, config.MetadataHeartbeatIntervalMs));
            logPrefix = $"streams-metadata[{config.ApplicationId}] ";
            MetadataTopic = MetadataTopicName(config.ApplicationId);

            State = new StreamsMetadataState(
                builder,
                instanceId,
                host,
                LocalSnapshot,
                TimeSpan.FromMilliseconds(3 * heartbeatInterval.TotalMilliseconds),
                PartitionCount);
        }

        internal static string MetadataTopicName(string applicationId) => applicationId + METADATA_TOPIC_SUFFIX;

        internal string MetadataTopic { get; }

        internal StreamsMetadataState State { get; }

        internal HostInfo Host => host;

        /// <summary>
        /// Partitioner of the producers (murmur2 random by default)
        /// </summary>
        internal Confluent.Kafka.Partitioner? ProducerPartitioner => config.ToProducerConfig(clientId).Partitioner;

        /// <summary>
        /// The metadata is shared with the other instances only when the application server is set
        /// </summary>
        internal bool IsShared => !Equals(host, HostInfo.Unavailable);

        internal InstanceMetadata LocalSnapshot()
        {
            var metadata = new InstanceMetadata
            {
                InstanceId = instanceId,
                ApplicationServer = IsShared ? host.ToString() : null,
                Timestamp = DateTime.Now.GetMilliseconds()
            };

            foreach (var t in threads)
            {
                var threadInfo = new ThreadInfo {ThreadName = t.Name, State = t.State.ToString()};
                try
                {
                    foreach (var task in t.ActiveTasks.OrderBy(task => task.Id.Id).ThenBy(task => task.Id.Partition))
                    {
                        threadInfo.ActiveTasks.Add(new TaskInfo
                        {
                            SubTopology = task.Id.Id,
                            Partition = task.Id.Partition,
                            TopicPartitions = task.Partition
                                .OrderBy(p => p.Topic).ThenBy(p => p.Partition.Value)
                                .Select(p => new TaskTopicPartition {Topic = p.Topic, Partition = p.Partition.Value})
                                .ToList()
                        });
                    }
                }
                catch (Exception e) when (e is NotSupportedException or InvalidOperationException)
                {
                    // not a stream thread, or its tasks are changing
                }

                metadata.Threads.Add(threadInfo);
            }

            return metadata;
        }

        private int? PartitionCount(string topic)
        {
            if (partitionCounts.TryGetValue(topic, out var count))
                return count;

            try
            {
                adminClient ??= kafkaSupplier.GetAdmin(config.ToAdminConfig($"{clientId}-metadata-admin"));
                var metadata = adminClient.GetMetadata(topic, TimeSpan.FromSeconds(10));
                var partitions = metadata?.Topics?.FirstOrDefault(t => t.Topic == topic)?.Partitions?.Count ?? 0;
                if (partitions <= 0)
                    return null;
                partitionCounts[topic] = partitions;
                return partitions;
            }
            catch (Exception e)
            {
                log.LogWarning(e, "{LogPrefix}Unable to get the number of partitions of the topic {Topic}", logPrefix, topic);
                return null;
            }
        }

        internal void Start()
        {
            if (!IsShared || running)
                return;

            running = true;
            thread = new Thread(Run) {Name = $"{clientId}-metadata-thread", IsBackground = true};
            thread.Start();
        }

        private void Run()
        {
            IProducer<byte[], byte[]> producer = null;
            IConsumer<byte[], byte[]> consumer = null;
            try
            {
                producer = kafkaSupplier.GetProducer(config.ToProducerConfig($"{clientId}-metadata-producer"));
                var consumerConfig = config.ToRestoreConsumerConfig($"{clientId}-metadata-reader");
                // the partition is assigned manually and offsets are never committed : the group id is unique to this instance
                consumerConfig.GroupId = $"{config.ApplicationId}-metadata-reader-{instanceId}";
                consumerConfig.EnableAutoCommit = false;
                consumer = kafkaSupplier.GetRestoreConsumer(consumerConfig);
                consumer.Assign(new TopicPartitionOffset(MetadataTopic, 0, Offset.Beginning));

                while (running)
                {
                    PublishIfNeeded(producer);

                    var record = consumer.Consume(TimeSpan.FromMilliseconds(100));
                    while (record != null && running)
                    {
                        HandleRecord(record);
                        record = consumer.Consume(TimeSpan.Zero);
                    }
                }

                // stopped cleanly : the other instances stop routing queries to this instance
                producer.Produce(MetadataTopic,
                    new Message<byte[], byte[]> {Key = Encoding.UTF8.GetBytes(instanceId), Value = null});
                producer.Flush(TimeSpan.FromSeconds(5));
            }
            catch (Exception e)
            {
                log.LogError(e, "{LogPrefix}The metadata thread failed, the metadata of the other instances is not updated anymore", logPrefix);
            }
            finally
            {
                try
                {
                    consumer?.Close();
                    consumer?.Dispose();
                    producer?.Dispose();
                }
                catch (Exception e)
                {
                    log.LogWarning(e, "{LogPrefix}Error while closing the metadata clients", logPrefix);
                }
            }
        }

        private void HandleRecord(ConsumeResult<byte[], byte[]> record)
        {
            if (record.Message?.Key == null)
                return;

            try
            {
                State.OnRecord(Encoding.UTF8.GetString(record.Message.Key), InstanceMetadata.Deserialize(record.Message.Value));
            }
            catch (Exception e)
            {
                log.LogWarning(e, "{LogPrefix}Skipping an invalid metadata record at offset {Offset}", logPrefix, record.Offset);
            }
        }

        private void PublishIfNeeded(IProducer<byte[], byte[]> producer)
        {
            var snapshot = LocalSnapshot();
            if (snapshot.SameContent(lastPublished) && DateTime.Now - lastPublication < heartbeatInterval)
                return;

            producer.Produce(MetadataTopic,
                new Message<byte[], byte[]> {Key = Encoding.UTF8.GetBytes(instanceId), Value = snapshot.Serialize()},
                report =>
                {
                    if (report.Error.IsError)
                        log.LogWarning("{LogPrefix}Unable to publish the metadata of this instance : {Error}", logPrefix, report.Error.Reason);
                });
            lastPublished = snapshot;
            lastPublication = DateTime.Now;
        }

        public void Dispose()
        {
            if (running)
            {
                running = false;
                thread?.Join(TimeSpan.FromSeconds(10));
            }

            adminClient?.Dispose();
        }
    }
}
