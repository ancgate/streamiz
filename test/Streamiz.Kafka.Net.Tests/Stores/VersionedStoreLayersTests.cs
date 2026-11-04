using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Confluent.Kafka;
using Moq;
using NUnit.Framework;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Kafka;
using Streamiz.Kafka.Net.Kafka.Internal;
using Streamiz.Kafka.Net.Metrics;
using Streamiz.Kafka.Net.Metrics.Internal;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.Mock.Sync;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.State.InMemory;
using Streamiz.Kafka.Net.State.Internal;
using Streamiz.Kafka.Net.State.Logging;
using Streamiz.Kafka.Net.State.Metered;
using Streamiz.Kafka.Net.Table;
using StoresFactory = Streamiz.Kafka.Net.State.Stores;

namespace Streamiz.Kafka.Net.Tests.Stores
{
    /// <summary>
    /// Change-logging and metered (typed) versioned store layers, builder and changelog topic config.
    /// </summary>
    public class VersionedStoreLayersTests
    {
        private const string STORE_NAME = "versioned-store";
        private const string CHANGELOG = STORE_NAME + "-changelog";

        private StreamConfig config;
        private ProcessorStateManager stateManager;
        private ProcessorContext context;
        private SyncKafkaSupplier kafkaSupplier;
        private IRecordCollector recordCollector;

        [SetUp]
        public void Begin()
        {
            config = new StreamConfig {ApplicationId = "unit-test-versioned-layers"};
            var id = new TaskId {Id = 0, Partition = 0};
            kafkaSupplier = new SyncKafkaSupplier();
            var producer = new StreamsProducer(config, "thread", Guid.NewGuid(), kafkaSupplier, "log");
            recordCollector = new RecordCollector("p-1", config, id, producer,
                new NoRunnableSensor("s", "s", MetricsRecordingLevel.DEBUG));

            stateManager = new ProcessorStateManager(
                id,
                new List<TopicPartition> {new("source", 0)},
                new Dictionary<string, string> {{STORE_NAME, CHANGELOG}},
                new MockChangelogRegister(),
                new MockOffsetCheckpointManager());

            var task = new Mock<AbstractTask>();
            task.Setup(k => k.Id).Returns(id);
            context = new ProcessorContext(task.Object, config, stateManager, new StreamMetricsRegistry());
            context.UseRecordCollector(recordCollector);
            context.SetRecordMetaData(new RecordContext(new Headers(), 0, 100, 0, "source"));
        }

        [TearDown]
        public void End()
        {
            stateManager.Close();
            recordCollector.Close(false);
        }

        private static Bytes Key(string key) => Bytes.Wrap(Encoding.UTF8.GetBytes(key));

        [Test]
        public void ChangeLoggingStoreLogsPutsAndDeletesWithTheirTimestamp()
        {
            var consumerConfig = new ConsumerConfig {GroupId = "test-versioned-changelog"};
            using var consumer = kafkaSupplier.GetConsumer(consumerConfig, null);
            consumer.Subscribe(CHANGELOG);

            var store = new ChangeLoggingVersionedKeyValueBytesStore(new InMemoryVersionedKeyValueStore(STORE_NAME, 10_000));
            store.Init(context, store);

            Assert.AreEqual(VersionedKeyValueStoreReturnCodes.PUT_RETURN_CODE_VALID_TO_UNDEFINED,
                store.Put(Key("a"), Encoding.UTF8.GetBytes("1"), 50));
            Assert.AreEqual(50, store.Put(Key("a"), Encoding.UTF8.GetBytes("0"), 20));
            store.Delete(Key("a"), 60);

            var records = new[] {consumer.Consume(), consumer.Consume(), consumer.Consume()};
            CollectionAssert.AreEqual(new long[] {50, 20, 60}, records.Select(r => r.Message.Timestamp.UnixTimestampMs));
            Assert.AreEqual("1", Encoding.UTF8.GetString(records[0].Message.Value));
            Assert.AreEqual("0", Encoding.UTF8.GetString(records[1].Message.Value));
            Assert.IsNull(records[2].Message.Value);
            Assert.IsTrue(records.All(r => Encoding.UTF8.GetString(r.Message.Key) == "a"));
        }

        [Test]
        public void MeteredStoreSerializesKeysAndValues()
        {
            var builder = StoresFactory.VersionedKeyValueStoreBuilder(
                StoresFactory.InMemoryVersionedKeyValueStore(STORE_NAME, TimeSpan.FromSeconds(10)),
                new StringSerDes(),
                new StringSerDes());
            var store = builder.Build(config);
            store.Init(context, store);

            Assert.IsInstanceOf<MeteredVersionedKeyValueStore<string, string>>(store);
            Assert.IsNotNull(((IRestoreSensorStore) store).RestoreSensor);

            store.Put("key", "v1", 10);
            store.Put("key", "v2", 20);

            Assert.AreEqual("v2", store.Get("key").Value);
            var asOf = store.Get("key", 15);
            Assert.AreEqual("v1", asOf.Value);
            Assert.AreEqual(10, asOf.Timestamp);
            Assert.AreEqual(20, asOf.ValidTo);

            var deleted = store.Delete("key", 30);
            Assert.AreEqual("v2", deleted.Value);
            Assert.IsNull(store.Get("key"));
            Assert.IsNull(store.Get("unknown"));
        }

        [Test]
        public void BuilderLogsByDefaultAndIsNotAWindowStore()
        {
            var builder = new VersionedKeyValueStoreBuilder<string, string>(
                StoresFactory.PersistentVersionedKeyValueStore(STORE_NAME, TimeSpan.FromMinutes(1)),
                new StringSerDes(),
                new StringSerDes());

            Assert.IsTrue(builder.LoggingEnabled);
            Assert.IsFalse(builder.IsWindowStore);
            Assert.AreEqual(60_000, builder.RetentionMs);
        }

        [Test]
        public void SupplierValidation()
        {
            Assert.Throws<ArgumentException>(() => StoresFactory.InMemoryVersionedKeyValueStore("s", TimeSpan.FromMilliseconds(-1)));
            Assert.Throws<ArgumentException>(() => StoresFactory.PersistentVersionedKeyValueStore("s", TimeSpan.FromMilliseconds(-1)));

            var rocksDb = (RocksDbVersionedKeyValueBytesStoreSupplier) StoresFactory.PersistentVersionedKeyValueStore("s", TimeSpan.FromMinutes(1));
            Assert.AreEqual(20_000, rocksDb.SegmentIntervalMs); // max(60 000 / 3, 2 000)
            Assert.AreEqual(2_000, ((RocksDbVersionedKeyValueBytesStoreSupplier) StoresFactory.PersistentVersionedKeyValueStore("s", TimeSpan.FromSeconds(1))).SegmentIntervalMs);

            // a versioned supplier is not a plain key-value store supplier
            Assert.Throws<NotSupportedException>(() => rocksDb.Get());
        }

        [Test]
        public void VersionedChangelogTopicConfig()
        {
            var topicConfig = new VersionedChangelogTopicConfig
            {
                Name = CHANGELOG,
                HistoryRetentionMs = 10_000,
                Configs = new Dictionary<string, string>()
            };
            var properties = topicConfig.GetProperties(new Dictionary<string, string>(), 86_400_000);
            Assert.AreEqual("compact", properties["cleanup.policy"]);
            Assert.AreEqual("86410000", properties["min.compaction.lag.ms"]);

            // a configured value is kept
            topicConfig.Configs = new Dictionary<string, string> {{"min.compaction.lag.ms", "5"}};
            Assert.AreEqual("5", topicConfig.GetProperties(new Dictionary<string, string>(), 86_400_000)["min.compaction.lag.ms"]);
        }

        [Test]
        public void SourceTableChangelogUsesTheVersionedConfig()
        {
            var builder = new StreamBuilder();
            builder.Table("input", InMemoryVersioned.As<string, string>("table-store", TimeSpan.FromSeconds(30)));
            var topology = builder.Build();
            topology.Builder.BuildTopology(new StreamConfig<StringSerDes, StringSerDes> {ApplicationId = "app"});

            var changelog = topology.Builder.MakeInternalTopicGroups()
                .SelectMany(g => g.Value.ChangelogTopics.Values)
                .Single();
            Assert.IsInstanceOf<VersionedChangelogTopicConfig>(changelog);
            Assert.AreEqual(30_000, ((VersionedChangelogTopicConfig) changelog).HistoryRetentionMs);
        }

        [Test]
        public void VersionedStoresAreOnlySupportedForSourceTables()
        {
            var builder = new StreamBuilder();
            Assert.Throws<TopologyException>(() => builder.Stream<string, string>("input")
                .GroupByKey()
                .Count(InMemoryVersioned.As<string, long>("count-store", TimeSpan.FromSeconds(10))));

            var globalBuilder = new StreamBuilder();
            globalBuilder.GlobalTable("global", InMemoryVersioned.As<string, string>("global-store", TimeSpan.FromSeconds(10)));
            Assert.Throws<TopologyException>(() => globalBuilder.Build());
        }
    }
}
