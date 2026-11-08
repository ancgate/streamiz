using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka;
using Moq;
using NUnit.Framework;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Metrics;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.State.Helper;
using Streamiz.Kafka.Net.State.Internal;
using Streamiz.Kafka.Net.Tests.Helpers;

namespace Streamiz.Kafka.Net.Tests.Stores
{
    public class RocksDbSegmentedBytesStoreRestoreTests
    {
        private const long Retention = 100;
        private const long SegmentInterval = 10;

        private StreamConfig config;
        private ProcessorContext context;
        private ProcessorStateManager stateManager;
        private RocksDbSegmentedBytesStore perRecordStore;
        private RocksDbSegmentedBytesStore batchStore;

        [SetUp]
        public void Begin()
        {
            config = new StreamConfig();
            config.ApplicationId = "unit-test-rocksdb-segmented-restore";
            config.UseRandomRocksDbConfigForTest();

            var id = new TaskId {Id = 0, Partition = 0};
            stateManager = new ProcessorStateManager(
                id,
                new List<TopicPartition> {new("source", 0)},
                null,
                new MockChangelogRegister(),
                new MockOffsetCheckpointManager());

            var task = new Mock<AbstractTask>();
            task.Setup(k => k.Id).Returns(id);
            context = new ProcessorContext(task.Object, config, stateManager, new StreamMetricsRegistry());

            perRecordStore = new RocksDbSegmentedBytesStore("per-record", "rocksdb-window", Retention, SegmentInterval, new WindowKeySchema());
            batchStore = new RocksDbSegmentedBytesStore("batch", "rocksdb-window", Retention, SegmentInterval, new WindowKeySchema());
            perRecordStore.Init(context, perRecordStore);
            batchStore.Init(context, batchStore);
        }

        [TearDown]
        public void End()
        {
            stateManager.Close();
            config.RemoveRocksDbFolderForTest();
        }

        private static Bytes Key(string key, long timestamp)
            => WindowKeyHelper.ToStoreKeyBinary(Bytes.Wrap(new StringSerDes().Serialize(key, new SerializationContext())), timestamp, 0);

        private static Dictionary<string, string> Content(RocksDbSegmentedBytesStore store)
            => store.All().ToList().ToDictionary(
                kv => System.Convert.ToBase64String(kv.Key.Get),
                kv => kv.Value == null ? null : System.Text.Encoding.UTF8.GetString(kv.Value));

        [Test]
        public void RestoreBatchGivesSameContentAsRecordByRecord()
        {
            byte[] V(string v) => System.Text.Encoding.UTF8.GetBytes(v);

            // several segments, out-of-order records, a delete, and a record whose segment has expired
            var records = new List<(Bytes Key, byte[] Value, long Timestamp)>
            {
                (Key("a", 5), V("a5"), 5),
                (Key("b", 7), V("b7"), 7),
                (Key("a", 15), V("a15"), 15),
                (Key("b", 7), null, 7),
                (Key("c", 12), V("c12"), 12),
                (Key("a", 250), V("a250"), 250),  // stream time jumps: segments before 150 expire
                (Key("d", 20), V("d20"), 20),     // expired, dropped
                (Key("e", 245), V("e245"), 245),
                (Key("a", 250), V("a250b"), 250),
            };

            // during a real restoration the context always holds the current changelog record metadata
            context.SetRecordMetaData(new ConsumeResult<byte[], byte[]>
            {
                TopicPartitionOffset = new TopicPartitionOffset("changelog", 0, 0),
                Message = new Message<byte[], byte[]> {Timestamp = new Timestamp(0, TimestampType.CreateTime), Headers = new Headers()}
            });

            foreach (var r in records)
                perRecordStore.Put(r.Key, r.Value);
            batchStore.RestoreBatch(records);

            var expected = Content(perRecordStore);
            CollectionAssert.AreEquivalent(expected, Content(batchStore));
            Assert.AreEqual(2, expected.Count);
            CollectionAssert.AreEquivalent(new[] {"a250b", "e245"}, expected.Values);
        }
    }
}
