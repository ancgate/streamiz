using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Confluent.Kafka;
using NUnit.Framework;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Kafka.Net.Tests.Private
{
    public class StreamsMetadataStateTests
    {
        [Test]
        public void Murmur2IsTheJavaProducerHash()
        {
            // expected values from Kafka's UtilsTest.testMurmur2
            Assert.AreEqual(-973932308, KeyPartitioner.Murmur2(Encoding.UTF8.GetBytes("21")));
            Assert.AreEqual(-790332482, KeyPartitioner.Murmur2(Encoding.UTF8.GetBytes("foobar")));
            Assert.AreEqual(-985981536, KeyPartitioner.Murmur2(Encoding.UTF8.GetBytes("a-little-bit-long-string")));
            Assert.AreEqual(-1486304829, KeyPartitioner.Murmur2(Encoding.UTF8.GetBytes("a-little-bit-longer-string")));
            Assert.AreEqual(-58897971,
                KeyPartitioner.Murmur2(Encoding.UTF8.GetBytes("lkjh234lh9fiuh90y23oiuhsafujhadof229phr9h19h89h8")));
            Assert.AreEqual(479470107, KeyPartitioner.Murmur2(new byte[] {(byte) 'a', (byte) 'b', (byte) 'c'}));
        }

        [Test]
        public void PartitionForEachPartitioner()
        {
            var key = Encoding.UTF8.GetBytes("foobar");
            // murmur2 : toPositive(hash) % partitions
            Assert.AreEqual((-790332482 & 0x7fffffff) % 6, KeyPartitioner.Partition(null, key, 6));
            Assert.AreEqual((-790332482 & 0x7fffffff) % 6, KeyPartitioner.Partition(Partitioner.Murmur2Random, key, 6));
            Assert.AreEqual((-790332482 & 0x7fffffff) % 6, KeyPartitioner.Partition(Partitioner.Murmur2, key, 6));
            // CRC32 (check value of "123456789")
            Assert.AreEqual(0xCBF43926u, KeyPartitioner.Crc32(Encoding.ASCII.GetBytes("123456789")));
            Assert.AreEqual((int) (0xCBF43926u % 7), KeyPartitioner.Partition(Partitioner.Consistent, Encoding.ASCII.GetBytes("123456789"), 7));
            // can't be computed
            Assert.IsNull(KeyPartitioner.Partition(Partitioner.Random, key, 6));
            Assert.IsNull(KeyPartitioner.Partition(null, null, 6));
            Assert.IsNull(KeyPartitioner.Partition(null, key, 0));
        }

        [Test]
        public void HostInfoParse()
        {
            Assert.AreEqual(new HostInfo("localhost", 8080), HostInfo.Parse("localhost:8080"));
            Assert.AreEqual(new HostInfo("::1", 80), HostInfo.Parse("::1:80"));
            Assert.AreSame(HostInfo.Unavailable, HostInfo.Parse(null));
            Assert.AreSame(HostInfo.Unavailable, HostInfo.Parse(" "));
            Assert.Throws<StreamConfigException>(() => HostInfo.Parse("localhost"));
            Assert.Throws<StreamConfigException>(() => HostInfo.Parse("localhost:"));
            Assert.Throws<StreamConfigException>(() => HostInfo.Parse(":80"));
            Assert.Throws<StreamConfigException>(() => HostInfo.Parse("localhost:abc"));
            Assert.Throws<StreamConfigException>(() => HostInfo.Parse("localhost:70000"));
        }

        [Test]
        public void InstanceMetadataRoundTrip()
        {
            var metadata = Instance("instance-1", "host1:1", 123, (0, 1, "input"));
            metadata.Threads[0].StandbyTasks.Add(new TaskInfo
            {
                SubTopology = 1, Partition = 0,
                TopicPartitions = new List<TaskTopicPartition> {new() {Topic = "input2", Partition = 0}}
            });

            var restored = InstanceMetadata.Deserialize(metadata.Serialize());

            Assert.AreEqual(InstanceMetadata.CURRENT_VERSION, restored.Version);
            Assert.AreEqual("instance-1", restored.InstanceId);
            Assert.AreEqual("host1:1", restored.ApplicationServer);
            Assert.AreEqual(123, restored.Timestamp);
            Assert.AreEqual("0-1", restored.ActiveTasks.Single().TaskId);
            Assert.AreEqual(new TopicPartition("input", 1), restored.ActiveTasks.Single().TopicPartitions.Single().ToTopicPartition());
            Assert.AreEqual("1-0", restored.StandbyTasks.Single().TaskId);
            Assert.IsNull(InstanceMetadata.Deserialize(null));

            // the publication timestamp is not part of the content
            var later = InstanceMetadata.Deserialize(metadata.Serialize());
            later.Timestamp = 456;
            Assert.IsTrue(metadata.SameContent(later));
            later.Threads[0].ActiveTasks.Clear();
            Assert.IsFalse(metadata.SameContent(later));
        }

        private static InstanceMetadata Instance(string id, string server, long timestamp, params (int subTopology, int partition, string topic)[] tasks)
        {
            var thread = new ThreadInfo {ThreadName = $"{id}-stream-thread-0", State = "RUNNING"};
            foreach (var t in tasks)
                thread.ActiveTasks.Add(new TaskInfo
                {
                    SubTopology = t.subTopology, Partition = t.partition,
                    TopicPartitions = new List<TaskTopicPartition> {new() {Topic = t.topic, Partition = t.partition}}
                });
            return new InstanceMetadata
            {
                InstanceId = id, ApplicationServer = server, Timestamp = timestamp,
                Threads = new List<ThreadInfo> {thread}
            };
        }

        // sub-topology 0 : table "input" (store "table-store"); global table "global" (store "global-store")
        private static InternalTopologyBuilder Builder()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes> {ApplicationId = "test-metadata"};
            var builder = new StreamBuilder();
            builder.Table("input", InMemory.As<string, string>("table-store"));
            builder.GlobalTable("global", InMemory.As<string, string>("global-store"));
            var topology = builder.Build();
            topology.Builder.RewriteTopology(config);
            topology.Builder.BuildTopology(config);
            return topology.Builder;
        }

        [Test]
        public void MetadataOfTheLocalAndRemoteInstances()
        {
            long now = 10_000;
            var local = Instance("local", "local:1", now, (0, 0, "input"));
            var state = new StreamsMetadataState(Builder(), "local", new HostInfo("local", 1), () => local,
                TimeSpan.FromSeconds(3), _ => 2, () => now);

            state.OnRecord("remote", Instance("remote", "remote:2", now, (0, 1, "input")));
            state.OnRecord("dead", Instance("dead", "dead:3", now - 5000, (0, 1, "input")));
            // records of the local instance are ignored : its metadata comes from its threads
            state.OnRecord("local", Instance("local", "old:9", now, (0, 1, "input")));

            var all = state.AllMetadata().OrderBy(m => m.ProcessId).ToList();
            Assert.AreEqual(new[] {"local", "remote"}, all.Select(m => m.ProcessId).ToArray());
            Assert.AreEqual(new HostInfo("local", 1), all[0].HostInfo);
            Assert.AreEqual(new HostInfo("remote", 2), all[1].HostInfo);
            CollectionAssert.AreEquivalent(new[] {new TopicPartition("input", 0)}, all[0].TopicPartitions);
            CollectionAssert.AreEquivalent(new[] {new TopicPartition("input", 1)}, all[1].TopicPartitions);
            CollectionAssert.AreEquivalent(new[] {"table-store"}, all[1].StateStoreNames);
            CollectionAssert.IsEmpty(all[1].StandbyTopicPartitions);

            Assert.AreEqual(2, state.AllMetadataForStore("table-store").Count());
            Assert.AreEqual(2, state.AllMetadataForStore("global-store").Count());
            Assert.AreEqual(0, state.AllMetadataForStore("unknown").Count());

            // tombstone
            state.OnRecord("remote", null);
            Assert.AreEqual(new[] {"local"}, state.AllMetadata().Select(m => m.ProcessId).ToArray());
        }

        [Test]
        public void QueryMetadataForKey()
        {
            long now = 10_000;
            var local = Instance("local", "local:1", now, (0, 0, "input"));
            var state = new StreamsMetadataState(Builder(), "local", new HostInfo("local", 1), () => local,
                TimeSpan.FromSeconds(3), topic => topic == "input" ? 2 : null, () => now);
            state.OnRecord("remote", Instance("remote", "remote:2", now, (0, 1, "input")));

            var serdes = new StringSerDes();
            string KeyOfPartition(int partition)
                => Enumerable.Range(0, 100).Select(i => $"key{i}")
                    .First(k => KeyPartitioner.Partition(null, serdes.Serialize(k, new SerializationContext()), 2) == partition);

            var metadata0 = state.QueryMetadataForKey("table-store", KeyOfPartition(0), serdes, null);
            Assert.AreEqual(new HostInfo("local", 1), metadata0.ActiveHost);
            Assert.AreEqual(0, metadata0.Partition);
            CollectionAssert.IsEmpty(metadata0.StandbyHosts);

            var metadata1 = state.QueryMetadataForKey("table-store", KeyOfPartition(1), serdes, null);
            Assert.AreEqual(new HostInfo("remote", 2), metadata1.ActiveHost);
            Assert.AreEqual(1, metadata1.Partition);

            // custom partitioner
            var custom = state.QueryMetadataForKey("table-store", "any", (topic, key, partitions) => 1);
            Assert.AreEqual(new HostInfo("remote", 2), custom.ActiveHost);

            // global store : on every instance
            var global = state.QueryMetadataForKey("global-store", "any", serdes, null);
            Assert.AreEqual(new HostInfo("local", 1), global.ActiveHost);
            Assert.AreEqual(-1, global.Partition);

            Assert.AreSame(KeyQueryMetadata.NotAvailable, state.QueryMetadataForKey("unknown", "any", serdes, null));
            Assert.AreSame(KeyQueryMetadata.NotAvailable,
                state.QueryMetadataForKey("table-store", "any", serdes, Partitioner.Random));

            // the remote instance stopped : nobody hosts the partition 1
            state.OnRecord("remote", null);
            var unowned = state.QueryMetadataForKey("table-store", KeyOfPartition(1), serdes, null);
            Assert.AreEqual(HostInfo.Unavailable, unowned.ActiveHost);
            Assert.AreEqual(1, unowned.Partition);
        }

        [Test]
        public void UnknownPartitionCountIsNotAvailable()
        {
            var state = new StreamsMetadataState(Builder(), "local", new HostInfo("local", 1),
                () => Instance("local", "local:1", 0, (0, 0, "input")), TimeSpan.FromSeconds(3), _ => null, () => 0);
            Assert.AreSame(KeyQueryMetadata.NotAvailable,
                state.QueryMetadataForKey("table-store", "key", new StringSerDes(), null));
        }
    }
}
