using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Confluent.Kafka;
using Moq;
using NUnit.Framework;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Mock.Kafka;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.State.Internal;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;
using Streamiz.Kafka.Net.Tests.Helpers;
using ThreadState = Streamiz.Kafka.Net.Processors.ThreadState;

namespace Streamiz.Kafka.Net.Tests.Public
{
    public class InteractiveQueriesMetadataTests
    {
        private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(20);
        private static readonly TimeSpan Step = TimeSpan.FromMilliseconds(50);
        private readonly StringSerDes serdes = new();
        private readonly List<string> stateDirs = new();

        [TearDown]
        public void Cleanup()
        {
            foreach (var dir in stateDirs.Where(Directory.Exists))
                Directory.Delete(dir, true);
            stateDirs.Clear();
        }

        private StreamConfig<StringSerDes, StringSerDes> Config(string applicationId, string applicationServer)
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = applicationId,
                BootstrapServers = "127.0.0.1",
                ApplicationServer = applicationServer,
                MetadataHeartbeatIntervalMs = 200,
                PollMs = 10,
                CommitIntervalMs = 100,
                StateDir = Path.Combine(".", Guid.NewGuid().ToString())
            };
            stateDirs.Add(config.StateDir);
            return config;
        }

        private static Topology TableTopology()
        {
            var builder = new StreamBuilder();
            builder.Table("input", InMemory.As<string, string>("store"));
            return builder.Build();
        }

        private static void WaitRunning(KafkaStream stream)
        {
            AssertExtensions.WaitUntil(() => stream.StreamState == KafkaStream.State.RUNNING, Timeout, Step);
            Assert.AreEqual(KafkaStream.State.RUNNING, stream.StreamState);
        }

        // a key which the default producer partitioner (murmur2) writes in the partition
        private string KeyOfPartition(int partition, int numPartitions, int skip = 0)
            => Enumerable.Range(0, 1000).Select(i => $"key{i}")
                .Where(k => KeyPartitioner.Partition(null, serdes.Serialize(k, new SerializationContext()), numPartitions) == partition)
                .Skip(skip)
                .First();

        private void Produce(MockKafkaSupplier supplier, string key, string value, int numPartitions)
        {
            var producer = supplier.GetProducer(new ProducerConfig());
            var partition = KeyPartitioner.Partition(null, serdes.Serialize(key, new SerializationContext()), numPartitions).Value;
            producer.Produce(new TopicPartition("input", partition), new Message<byte[], byte[]>
            {
                Key = serdes.Serialize(key, new SerializationContext()),
                Value = serdes.Serialize(value, new SerializationContext())
            });
        }

        private static string Get(KafkaStream stream, string key, int? partition = null)
        {
            var parameters = StoreQueryParameters.FromNameAndType("store", QueryableStoreTypes.KeyValueStore<string, string>());
            if (partition.HasValue)
                parameters = parameters.WithPartition(partition.Value);
            return stream.Store(parameters).Get(key);
        }

        [Test]
        public void InvalidApplicationServerIsRejected()
        {
            var config = Config("test-iq-invalid", "localhost");
            Assert.Throws<StreamConfigException>(() => new KafkaStream(TableTopology(), config, new MockKafkaSupplier()));
        }

        [Test]
        public async Task LocalMetadataWithoutApplicationServer()
        {
            var supplier = new MockKafkaSupplier(2);
            using var stream = new KafkaStream(TableTopology(), Config("test-iq-local", null), supplier);
            await stream.StartAsync();
            WaitRunning(stream);
            AssertExtensions.WaitUntil(() => stream.AllMetadata().Single().TopicPartitions.Count == 2, Timeout, Step);

            var metadata = stream.AllMetadata().Single();
            Assert.AreEqual(HostInfo.Unavailable, metadata.HostInfo);
            CollectionAssert.AreEquivalent(new[] {new TopicPartition("input", 0), new TopicPartition("input", 1)}, metadata.TopicPartitions);
            CollectionAssert.AreEquivalent(new[] {"store"}, metadata.StateStoreNames);

            var thread = stream.MetadataForLocalThreads().Single();
            Assert.AreEqual(ThreadState.RUNNING.ToString(), thread.ThreadState);
            CollectionAssert.AreEquivalent(new[] {"0-0", "0-1"}, thread.ActiveTasks.Select(t => t.TaskId));
            CollectionAssert.IsEmpty(thread.StandbyTasks);
        }

        [Test]
        public async Task QueryOnePartition()
        {
            var supplier = new MockKafkaSupplier(2);
            using var stream = new KafkaStream(TableTopology(), Config("test-iq-partition", "localhost:9001"), supplier);
            await stream.StartAsync();
            WaitRunning(stream);

            string key0 = KeyOfPartition(0, 2), key1 = KeyOfPartition(1, 2);
            Produce(supplier, key0, "v0", 2);
            Produce(supplier, key1, "v1", 2);
            AssertExtensions.WaitUntil(() => Get(stream, key0) != null && Get(stream, key1) != null, Timeout, Step);

            Assert.AreEqual("v0", Get(stream, key0, 0));
            Assert.IsNull(Get(stream, key0, 1));
            Assert.AreEqual("v1", Get(stream, key1, 1));
            Assert.IsNull(Get(stream, key1, 0));
            Assert.Throws<InvalidStateStoreException>(() => Get(stream, key0, 5));

            var keyMetadata = stream.QueryMetadataForKey("store", key1, serdes);
            Assert.AreEqual(new HostInfo("localhost", 9001), keyMetadata.ActiveHost);
            Assert.AreEqual(1, keyMetadata.Partition);
        }

        [Test]
        public void StaleStoresCanBeQueriedWhileTheThreadIsNotRunning()
        {
            var store = new Mock<ITimestampedKeyValueStore<string, string>>();
            store.Setup(s => s.Name).Returns("store");
            store.Setup(s => s.IsOpen).Returns(true);
            var task = new Mock<ITask>();
            task.Setup(t => t.Id).Returns(new TaskId {Id = 0, Partition = 1});
            task.Setup(t => t.GetStore("store")).Returns(store.Object);
            var thread = new Mock<IThread>();
            thread.Setup(t => t.State).Returns(ThreadState.PARTITIONS_ASSIGNED);
            thread.Setup(t => t.ActiveTasks).Returns(new[] {task.Object});

            var provider = new StreamThreadStateStoreProvider(thread.Object, null);
            var parameters = StoreQueryParameters.FromNameAndType("store", QueryableStoreTypes.KeyValueStore<string, string>());

            Assert.Throws<InvalidStateStoreException>(() => provider.Stores(parameters));
            Assert.AreEqual(1, provider.Stores(parameters.EnableStaleStores()).Count());
            Assert.AreEqual(1, provider.Stores(parameters.EnableStaleStores().WithPartition(1)).Count());
            Assert.AreEqual(0, provider.Stores(parameters.EnableStaleStores().WithPartition(0)).Count());

            // a stopping thread can't be queried, even with stale stores
            thread.Setup(t => t.State).Returns(ThreadState.PENDING_SHUTDOWN);
            Assert.Throws<InvalidStateStoreException>(() => provider.Stores(parameters.EnableStaleStores()));
        }

        [Test]
        public async Task MetadataIsSharedBetweenInstances()
        {
            const int partitions = 2;
            var supplier = new MockKafkaSupplier(partitions);
            var host1 = new HostInfo("host1", 1001);
            var host2 = new HostInfo("host2", 1002);

            var stream1 = new KafkaStream(TableTopology(), Config("test-iq-shared", host1.ToString()), supplier);
            var stream2 = new KafkaStream(TableTopology(), Config("test-iq-shared", host2.ToString()), supplier);
            bool stream2Disposed = false;
            try
            {
                await stream1.StartAsync();
                await stream2.StartAsync();
                WaitRunning(stream1);
                WaitRunning(stream2);

                bool BothInstancesWithOnePartitionEach(KafkaStream stream)
                {
                    var all = stream.AllMetadata().ToList();
                    return all.Count == 2 && all.All(m => m.TopicPartitions.Count == 1);
                }

                AssertExtensions.WaitUntil(() => BothInstancesWithOnePartitionEach(stream1) && BothInstancesWithOnePartitionEach(stream2), Timeout, Step);
                Assert.IsTrue(BothInstancesWithOnePartitionEach(stream1), string.Join(" ; ", stream1.AllMetadata()));
                Assert.IsTrue(BothInstancesWithOnePartitionEach(stream2), string.Join(" ; ", stream2.AllMetadata()));

                var fromStream1 = stream1.AllMetadata().ToDictionary(m => m.HostInfo);
                CollectionAssert.AreEquivalent(new[] {host1, host2}, fromStream1.Keys);
                CollectionAssert.IsEmpty(fromStream1[host1].TopicPartitions.Intersect(fromStream1[host2].TopicPartitions));
                Assert.AreEqual(2, stream2.AllMetadataForStore("store").Count());

                // the key is in the store of the instance returned by QueryMetadataForKey
                var streams = new Dictionary<HostInfo, KafkaStream> {{host1, stream1}, {host2, stream2}};
                var keys = new[] {KeyOfPartition(0, partitions), KeyOfPartition(1, partitions)};
                foreach (var key in keys)
                    Produce(supplier, key, $"value-{key}", partitions);

                foreach (var key in keys)
                {
                    var keyMetadata = stream1.QueryMetadataForKey("store", key, serdes);
                    Assert.AreEqual(keyMetadata.ActiveHost, stream2.QueryMetadataForKey("store", key, serdes).ActiveHost);
                    Assert.IsTrue(streams.ContainsKey(keyMetadata.ActiveHost), keyMetadata.ToString());

                    var owner = streams[keyMetadata.ActiveHost];
                    var other = streams.Single(s => !s.Key.Equals(keyMetadata.ActiveHost)).Value;
                    AssertExtensions.WaitUntil(() => Get(owner, key) != null, Timeout, Step);
                    Assert.AreEqual($"value-{key}", Get(owner, key));
                    Assert.IsNull(Get(other, key));
                }

                // the second instance stops : its tombstone removes it, and its partition moves to the first one
                stream2.Dispose();
                stream2Disposed = true;

                AssertExtensions.WaitUntil(() =>
                {
                    var all = stream1.AllMetadata().ToList();
                    return all.Count == 1 && all[0].TopicPartitions.Count == partitions;
                }, Timeout, Step);
                var remaining = stream1.AllMetadata().ToList();
                Assert.AreEqual(1, remaining.Count, string.Join(" ; ", remaining));
                Assert.AreEqual(host1, remaining[0].HostInfo);
                Assert.AreEqual(partitions, remaining[0].TopicPartitions.Count);
                foreach (var key in keys)
                    Assert.AreEqual(host1, stream1.QueryMetadataForKey("store", key, serdes).ActiveHost);
            }
            finally
            {
                if (!stream2Disposed)
                    stream2.Dispose();
                stream1.Dispose();
            }
        }
    }
}
