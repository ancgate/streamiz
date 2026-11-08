using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Confluent.Kafka;
using Moq;
using NUnit.Framework;
using Streamiz.Kafka.Net.Crosscutting;
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
using Streamiz.Kafka.Net.State.Cache;
using Streamiz.Kafka.Net.State.Enumerator;
using Streamiz.Kafka.Net.State.InMemory;
using Streamiz.Kafka.Net.State.Internal;
using Streamiz.Kafka.Net.State.Logging;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;
using Streamiz.Kafka.Net.Table.Internal;
using StoresFactory = Streamiz.Kafka.Net.State.Stores;

namespace Streamiz.Kafka.Net.Tests.Stores
{
    /// <summary>
    /// Caching, change-logging and metered (typed) session store layers.
    /// </summary>
    public class SessionStoreLayersTests
    {
        private const string STORE_NAME = "session-store";
        private const string CHANGELOG = STORE_NAME + "-changelog";

        private StreamConfig config;
        private ProcessorStateManager stateManager;
        private ProcessorContext context;
        private SyncKafkaSupplier kafkaSupplier;
        private IRecordCollector recordCollector;

        [SetUp]
        public void Begin()
        {
            config = new StreamConfig {ApplicationId = "unit-test-session-layers"};
            config.DefaultStateStoreCacheMaxBytes = 1024 * 1024;

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
        private static byte[] Value(string value) => Encoding.UTF8.GetBytes(value);
        private static Windowed<Bytes> Session(string key, long start, long end) => new(Key(key), new SessionWindow(start, end));

        private static List<(string key, long start, long end, string value)> ToList(IKeyValueEnumerator<Windowed<Bytes>, byte[]> enumerator)
        {
            var list = new List<(string, long, long, string)>();
            using (enumerator)
            {
                while (enumerator.MoveNext())
                {
                    var kv = enumerator.Current.Value;
                    list.Add((Encoding.UTF8.GetString(kv.Key.Key.Get), kv.Key.Window.StartMs, kv.Key.Window.EndMs,
                        Encoding.UTF8.GetString(kv.Value)));
                }
            }

            return list;
        }

        #region Caching

        private (CachingSessionStore cache, InMemorySessionStore inner, List<KeyValuePair<byte[], Change<byte[]>>> flushed) CachingStore(bool sendOldValues = true)
        {
            var inner = new InMemorySessionStore(STORE_NAME, 10_000);
            var caching = new CachingSessionStore(inner, 1, null);
            caching.Init(context, caching);
            var flushed = new List<KeyValuePair<byte[], Change<byte[]>>>();
            caching.SetFlushListener(flushed.Add, sendOldValues);
            return (caching, inner, flushed);
        }

        [Test]
        public void CachingStoreKeepsWritesInCacheUntilFlush()
        {
            var (caching, inner, flushed) = CachingStore();

            caching.Put(Session("a", 0, 10), Value("1"));

            Assert.IsTrue(caching.IsCachedStore);
            Assert.AreEqual(0, ToList(inner.Fetch(Key("a"))).Count);
            CollectionAssert.AreEqual(new[] {("a", 0L, 10L, "1")}, ToList(caching.FindSessions(Key("a"), 0, 10)));
            Assert.AreEqual("1", Encoding.UTF8.GetString(caching.FetchSession(Key("a"), 0, 10)));

            caching.Flush();

            CollectionAssert.AreEqual(new[] {("a", 0L, 10L, "1")}, ToList(inner.Fetch(Key("a"))));
            Assert.AreEqual(1, flushed.Count);
            var flushedKey = SessionKeySchema.From(Bytes.Wrap(flushed[0].Key));
            Assert.AreEqual(new SessionWindow(0, 10), flushedKey.Window);
            Assert.AreEqual("1", Encoding.UTF8.GetString(flushed[0].Value.NewValue));
            Assert.IsNull(flushed[0].Value.OldValue);
        }

        [Test]
        public void CachingStoreForwardsOldValueOnUpdate()
        {
            var (caching, _, flushed) = CachingStore();

            caching.Put(Session("a", 0, 10), Value("1"));
            caching.Flush();
            caching.Put(Session("a", 0, 10), Value("2"));
            caching.Flush();

            Assert.AreEqual(2, flushed.Count);
            Assert.AreEqual("2", Encoding.UTF8.GetString(flushed[1].Value.NewValue));
            Assert.AreEqual("1", Encoding.UTF8.GetString(flushed[1].Value.OldValue));
        }

        [Test]
        public void CachingStoreRemoveHidesTheWrappedSession()
        {
            var (caching, inner, flushed) = CachingStore();

            caching.Put(Session("a", 0, 10), Value("1"));
            caching.Put(Session("a", 20, 30), Value("2"));
            caching.Flush();
            flushed.Clear();

            caching.Remove(Session("a", 0, 10));

            // still in the wrapped store, but hidden by the removed cache entry
            Assert.AreEqual(2, ToList(inner.Fetch(Key("a"))).Count);
            CollectionAssert.AreEqual(new[] {("a", 20L, 30L, "2")}, ToList(caching.Fetch(Key("a"))));
            Assert.IsNull(caching.FetchSession(Key("a"), 0, 10));

            caching.Flush();

            CollectionAssert.AreEqual(new[] {("a", 20L, 30L, "2")}, ToList(inner.Fetch(Key("a"))));
            Assert.AreEqual(1, flushed.Count);
            Assert.IsNull(flushed[0].Value.NewValue);
            Assert.AreEqual("1", Encoding.UTF8.GetString(flushed[0].Value.OldValue));
        }

        [Test]
        public void CachingStoreDoesNotForwardSessionAddedAndRemovedBeforeFlush()
        {
            var (caching, inner, flushed) = CachingStore();

            caching.Put(Session("a", 0, 10), Value("1"));
            caching.Remove(Session("a", 0, 10));
            caching.Flush();

            Assert.AreEqual(0, flushed.Count);
            Assert.AreEqual(0, ToList(inner.Fetch(Key("a"))).Count);
        }

        [Test]
        public void CachingStoreMergesCacheAndStoreInOrder()
        {
            var (caching, _, _) = CachingStore();

            caching.Put(Session("a", 0, 10), Value("1"));
            caching.Put(Session("a", 40, 50), Value("3"));
            caching.Flush();
            caching.Put(Session("a", 20, 30), Value("2"));
            caching.Put(Session("a", 40, 50), Value("3bis"));

            CollectionAssert.AreEqual(
                new[] {("a", 0L, 10L, "1"), ("a", 20L, 30L, "2"), ("a", 40L, 50L, "3bis")},
                ToList(caching.Fetch(Key("a"))));
            CollectionAssert.AreEqual(
                new[] {("a", 40L, 50L, "3bis"), ("a", 20L, 30L, "2"), ("a", 0L, 10L, "1")},
                ToList(caching.BackwardFetch(Key("a"))));
        }

        [Test]
        public void CachingDisabledWritesDirectly()
        {
            config.DefaultStateStoreCacheMaxBytes = 0;
            var inner = new InMemorySessionStore(STORE_NAME, 10_000);
            var caching = new CachingSessionStore(inner, 1, null);
            caching.Init(context, caching);

            caching.Put(Session("a", 0, 10), Value("1"));

            Assert.IsFalse(caching.IsCachedStore);
            Assert.AreEqual(1, ToList(inner.Fetch(Key("a"))).Count);
        }

        #endregion

        #region Change logging

        [Test]
        public void ChangeLoggingStoreLogsPutsAndRemoves()
        {
            var consumerConfig = new ConsumerConfig {GroupId = "test-session-changelog"};
            using var consumer = kafkaSupplier.GetConsumer(consumerConfig, null);
            consumer.Subscribe(CHANGELOG);

            var inner = new InMemorySessionStore(STORE_NAME, 10_000);
            var store = new ChangeLoggingSessionBytesStore(inner);
            store.Init(context, store);

            store.Put(Session("a", 0, 10), Value("1"));
            store.Remove(Session("a", 0, 10));

            var put = consumer.Consume();
            Assert.AreEqual(SessionKeySchema.ToBinary(Session("a", 0, 10)).Get, put.Message.Key);
            Assert.AreEqual("1", Encoding.UTF8.GetString(put.Message.Value));

            var tombstone = consumer.Consume();
            Assert.AreEqual(SessionKeySchema.ToBinary(Session("a", 0, 10)).Get, tombstone.Message.Key);
            Assert.IsNull(tombstone.Message.Value);

            Assert.AreEqual(0, ToList(store.Fetch(Key("a"))).Count);
        }

        #endregion

        #region Metered / builder

        [Test]
        public void TypedStoreFromBuilder()
        {
            var store = StoresFactory.SessionStoreBuilder(
                    StoresFactory.InMemorySessionStore(STORE_NAME, TimeSpan.FromSeconds(10)),
                    new StringSerDes(),
                    new Int64SerDes())
                .WithLoggingDisabled()
                .WithCachingDisabled()
                .Build(config);
            store.Init(context, store);

            store.Put(new Windowed<string>("a", new SessionWindow(0, 10)), 5L);
            store.Put(new Windowed<string>("a", new SessionWindow(20, 30)), 7L);
            store.Remove(new Windowed<string>("a", new SessionWindow(20, 30)));

            Assert.AreEqual(5L, store.FetchSession("a", 0, 10));
            Assert.AreEqual(0L, store.FetchSession("a", 20, 30));

            using var sessions = store.FindSessions("a", 0, 100);
            Assert.IsTrue(sessions.MoveNext());
            Assert.AreEqual("a", sessions.Current.Value.Key.Key);
            Assert.AreEqual(new SessionWindow(0, 10), sessions.Current.Value.Key.Window);
            Assert.AreEqual(5L, sessions.Current.Value.Value);
            Assert.IsFalse(sessions.MoveNext());
            store.Close();
        }

        [Test]
        public void TypedCachedStoreFlushListenerGivesSessionKeys()
        {
            var store = StoresFactory.SessionStoreBuilder(
                    StoresFactory.InMemorySessionStore(STORE_NAME, TimeSpan.FromSeconds(10)),
                    new StringSerDes(),
                    new Int64SerDes())
                .WithLoggingDisabled()
                .WithCachingEnabled()
                .Build(config);
            store.Init(context, store);

            var flushed = new List<KeyValuePair<Windowed<string>, Change<long>>>();
            Assert.IsTrue(((ICachedStateStore<Windowed<string>, long>) store).SetFlushListener(flushed.Add, true));
            Assert.IsTrue(((IWrappedStateStore) store).IsCachedStore);

            store.Put(new Windowed<string>("a", new SessionWindow(0, 10)), 5L);
            store.Flush();

            Assert.AreEqual(1, flushed.Count);
            Assert.AreEqual("a", flushed[0].Key.Key);
            Assert.AreEqual(new SessionWindow(0, 10), flushed[0].Key.Window);
            Assert.AreEqual(5L, flushed[0].Value.NewValue);
            store.Close();
        }

        [Test]
        public void SessionStoreBuilderUsesWindowedChangelog()
        {
            var builder = StoresFactory.SessionStoreBuilder(
                StoresFactory.PersistentSessionStore(STORE_NAME, TimeSpan.FromMinutes(10)),
                new StringSerDes(),
                new Int64SerDes());

            Assert.IsTrue(builder.IsWindowStore);
            Assert.AreEqual((long) TimeSpan.FromMinutes(10).TotalMilliseconds, builder.RetentionMs);
            Assert.IsTrue(builder.LoggingEnabled);
        }

        [Test]
        public void StoresRejectNegativeRetention()
        {
            Assert.Throws<ArgumentException>(() => StoresFactory.PersistentSessionStore("s", TimeSpan.FromMilliseconds(-1)));
            Assert.Throws<ArgumentException>(() => StoresFactory.InMemorySessionStore("s", TimeSpan.FromMilliseconds(-1)));
        }

        [Test]
        public void RocksDbSupplierDefaultSegmentInterval()
        {
            Assert.AreEqual(60_000L, new RocksDbSessionBytesStoreSupplier("s", TimeSpan.FromMinutes(1)).SegmentInterval);
            Assert.AreEqual(5 * 60_000L, new RocksDbSessionBytesStoreSupplier("s", TimeSpan.FromMinutes(10)).SegmentInterval);
            Assert.AreEqual("rocksdb-session", new RocksDbSessionBytesStoreSupplier("s", TimeSpan.FromMinutes(1)).MetricsScope);
            Assert.AreEqual("in-memory-session", new InMemorySessionBytesStoreSupplier("s", TimeSpan.FromMinutes(1)).MetricsScope);
        }

        #endregion

        #region Materialized

        [Test]
        public void MaterializedSessionHelpersAndRetention()
        {
            var inMemory = InMemorySessions.As<string, long>("store", TimeSpan.FromMinutes(5));
            Assert.AreEqual("store", inMemory.StoreName);
            Assert.IsInstanceOf<InMemorySessionBytesStoreSupplier>(inMemory.StoreSupplier);
            Assert.AreEqual((long) TimeSpan.FromMinutes(5).TotalMilliseconds,
                ((State.Supplier.ISessionBytesStoreSupplier) inMemory.StoreSupplier).Retention);

            var rocks = RocksDbSessions.As<string, long>("store2").WithRetention(TimeSpan.FromMinutes(7));
            Assert.IsInstanceOf<RocksDbSessionBytesStoreSupplier>(rocks.StoreSupplier);
            Assert.AreEqual((long) TimeSpan.FromMinutes(7).TotalMilliseconds,
                ((State.Supplier.ISessionBytesStoreSupplier) rocks.StoreSupplier).Retention);
        }

        [Test]
        public void SessionStoreMaterializerValidatesRetention()
        {
            var materialized = Materialized<string, long, ISessionStore<Bytes, byte[]>>.Create("store")
                .WithRetention(TimeSpan.FromMinutes(1));

            Assert.Throws<ArgumentException>(() =>
                new SessionStoreMaterializer<string, long>(50_000, 20_000, materialized).Materialize());

            var builder = new SessionStoreMaterializer<string, long>(30_000, 20_000, materialized).Materialize();
            Assert.AreEqual(60_000L, builder.RetentionMs);
            Assert.IsTrue(builder.IsWindowStore);
        }

        [Test]
        public void SessionStoreMaterializerUsesTheGivenSupplier()
        {
            var materialized = InMemorySessions.As<string, long>("store", TimeSpan.FromMinutes(2)).WithLoggingDisabled();

            var builder = new SessionStoreMaterializer<string, long>(60_000, 0, materialized).Materialize();
            Assert.IsFalse(builder.LoggingEnabled);

            var store = builder.Build(config);
            Assert.AreEqual("store", store.Name);
            Assert.IsFalse(store.Persistent); // the in-memory supplier, not the default RocksDB one
        }

        #endregion
    }
}
