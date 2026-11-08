using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Confluent.Kafka;
using NUnit.Framework;
using Streamiz.Kafka.Net.Mock.Kafka;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;
using Streamiz.Kafka.Net.Tests.Helpers;

namespace Streamiz.Kafka.Net.Tests.Public
{
    /// <summary>
    /// Two instances of an application on one mock cluster, with a persistent table store and one standby replica per task
    /// </summary>
    public class StandbyReplicasTests
    {
        private const int Partitions = 2;
        private const int KeysPerPartition = 20;
        private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(30);
        private static readonly TimeSpan Step = TimeSpan.FromMilliseconds(50);
        private readonly StringSerDes serdes = new();
        private readonly List<string> stateDirs = new();

        [TearDown]
        public void Cleanup()
        {
            foreach (var dir in stateDirs.Where(Directory.Exists))
            {
                try
                {
                    Directory.Delete(dir, true);
                }
                catch (IOException)
                {
                    // rocksdb files may still be released
                }
            }
            stateDirs.Clear();
        }

        private StreamConfig<StringSerDes, StringSerDes> Config(string applicationId, string applicationServer, int standbys)
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = applicationId,
                BootstrapServers = "127.0.0.1",
                ApplicationServer = applicationServer,
                NumStandbyReplicas = standbys,
                MetadataHeartbeatIntervalMs = 200,
                PollMs = 10,
                CommitIntervalMs = 100,
                // each instance has its own state directory, as on different machines
                StateDir = Path.Combine(".", Guid.NewGuid().ToString())
            };
            stateDirs.Add(config.StateDir);
            return config;
        }

        private static Topology TableTopology()
        {
            var builder = new StreamBuilder();
            builder.Table("input", RocksDb.As<string, string>("store"));
            return builder.Build();
        }

        private static void WaitRunning(KafkaStream stream)
        {
            AssertExtensions.WaitUntil(() => stream.StreamState == KafkaStream.State.RUNNING, Timeout, Step);
            Assert.AreEqual(KafkaStream.State.RUNNING, stream.StreamState);
        }

        private int PartitionOf(string key)
            => KeyPartitioner.Partition(null, serdes.Serialize(key, new SerializationContext()), Partitions).Value;

        private List<string> KeysOf(int partition)
            => Enumerable.Range(0, 1000).Select(i => $"key{i}")
                .Where(k => PartitionOf(k) == partition)
                .Take(KeysPerPartition)
                .ToList();

        private void Produce(MockKafkaSupplier supplier, IEnumerable<string> keys)
        {
            var producer = supplier.GetProducer(new ProducerConfig());
            foreach (var key in keys)
            {
                producer.Produce(new TopicPartition("input", PartitionOf(key)), new Message<byte[], byte[]>
                {
                    Key = serdes.Serialize(key, new SerializationContext()),
                    Value = serdes.Serialize($"value-{key}", new SerializationContext())
                });
            }
        }

        private static string Get(KafkaStream stream, string key, int partition, bool stale)
        {
            var parameters = StoreQueryParameters
                .FromNameAndType("store", QueryableStoreTypes.KeyValueStore<string, string>())
                .WithPartition(partition);
            if (stale)
                parameters = parameters.EnableStaleStores();
            try
            {
                return stream.Store(parameters).Get(key);
            }
            catch (Errors.InvalidStateStoreException)
            {
                return null;
            }
        }

        private static int ActivePartition(KafkaStream stream)
            => stream.MetadataForLocalThreads().SelectMany(t => t.ActiveTasks).Single().TopicPartitions.Single().Partition.Value;

        /// <summary>
        /// Run two instances, produce to both partitions, stop the second instance, and return the number of changelog
        /// records restored by the first instance for the partition it took over
        /// </summary>
        private async Task<long> RunAndStopTheSecondInstance(string applicationId, int standbys, Action<KafkaStream, KafkaStream, HostInfo, HostInfo> whileBothRun)
        {
            var supplier = new MockKafkaSupplier(Partitions);
            var host1 = new HostInfo("host1", 1001);
            var host2 = new HostInfo("host2", 1002);
            var stream1 = new KafkaStream(TableTopology(), Config(applicationId, host1.ToString(), standbys), supplier);
            var stream2 = new KafkaStream(TableTopology(), Config(applicationId, host2.ToString(), standbys), supplier);

            var restored = new ConcurrentQueue<(int Partition, long Total)>();
            bool recordRestorations = false;
            stream1.OnRestoreEndEvent += (topicPartition, _, total) =>
            {
                if (recordRestorations)
                    restored.Enqueue((topicPartition.Partition.Value, total));
            };

            bool stream2Disposed = false;
            try
            {
                await stream1.StartAsync();
                await stream2.StartAsync();
                WaitRunning(stream1);
                WaitRunning(stream2);
                AssertExtensions.WaitUntil(() =>
                    stream1.MetadataForLocalThreads().SelectMany(t => t.ActiveTasks).Count() == 1 &&
                    stream2.MetadataForLocalThreads().SelectMany(t => t.ActiveTasks).Count() == 1, Timeout, Step);

                Produce(supplier, KeysOf(0).Concat(KeysOf(1)));
                whileBothRun(stream1, stream2, host1, host2);

                // all the records are in the active store of the second instance
                int movedPartition = ActivePartition(stream2);
                AssertExtensions.WaitUntil(() => KeysOf(movedPartition).All(k => Get(stream2, k, movedPartition, false) != null), Timeout, Step);

                recordRestorations = true;
                stream2.Dispose();
                stream2Disposed = true;

                // the first instance takes over the partition, with all its records
                AssertExtensions.WaitUntil(() =>
                    stream1.StreamState == KafkaStream.State.RUNNING &&
                    KeysOf(movedPartition).All(k => Get(stream1, k, movedPartition, false) == $"value-{k}"), Timeout, Step);
                foreach (var key in KeysOf(movedPartition))
                    Assert.AreEqual($"value-{key}", Get(stream1, key, movedPartition, false));

                AssertExtensions.WaitUntil(() => restored.Any(r => r.Partition == movedPartition), Timeout, Step);
                return restored.Where(r => r.Partition == movedPartition).Sum(r => r.Total);
            }
            finally
            {
                if (!stream2Disposed)
                    stream2.Dispose();
                stream1.Dispose();
            }
        }

        [Test]
        public async Task StandbyReplicasAreUpdatedAndReusedWhenTheTaskMoves()
        {
            long restored = await RunAndStopTheSecondInstance("test-standby", 1, (stream1, stream2, host1, host2) =>
            {
                var streams = new Dictionary<HostInfo, KafkaStream> {{host1, stream1}, {host2, stream2}};

                // each instance hosts the standby of the other instance's task
                AssertExtensions.WaitUntil(() => stream1.AllMetadata().Count() == 2 &&
                    stream1.AllMetadata().All(m => m.StandbyTopicPartitions.Count == 1 && m.TopicPartitions.Count == 1), Timeout, Step);
                foreach (var metadata in stream1.AllMetadata())
                    CollectionAssert.AreNotEquivalent(metadata.TopicPartitions, metadata.StandbyTopicPartitions);

                foreach (var (host, stream) in streams)
                {
                    int standbyPartition = 1 - ActivePartition(stream);
                    var keys = KeysOf(standbyPartition);

                    // the standby store has the records of the other instance (stale stores only)
                    AssertExtensions.WaitUntil(() => keys.All(k => Get(stream, k, standbyPartition, true) == $"value-{k}"), Timeout, Step);
                    foreach (var key in keys)
                    {
                        Assert.AreEqual($"value-{key}", Get(stream, key, standbyPartition, true));
                        Assert.IsNull(Get(stream, key, standbyPartition, false), "the standby store is only queryable as a stale store");
                    }

                    // the key metadata lists the standby host
                    var keyMetadata = stream1.QueryMetadataForKey("store", keys[0], serdes);
                    Assert.AreNotEqual(host, keyMetadata.ActiveHost);
                    CollectionAssert.AreEquivalent(new[] {host}, keyMetadata.StandbyHosts);
                }
            });

            // the standby was up to date : the active task restored nothing
            Assert.AreEqual(0, restored);
        }

        [Test]
        public async Task WithoutStandbyReplicasTheWholeChangelogIsRestored()
        {
            long restored = await RunAndStopTheSecondInstance("test-no-standby", 0, (_, _, _, _) => { });
            Assert.AreEqual(KeysPerPartition, restored);
        }
    }
}
