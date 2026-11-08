using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Confluent.Kafka;
using Moq;
using NUnit.Framework;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Metrics;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.State.Cache;
using Streamiz.Kafka.Net.State.Enumerator;
using Streamiz.Kafka.Net.State.InMemory;
using Streamiz.Kafka.Net.State.Internal;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Tests.Helpers;

namespace Streamiz.Kafka.Net.Tests.Stores
{
    /// <summary>
    /// Behaviour every session bytes store must have (ported from Kafka Streams' AbstractSessionBytesStoreTest).
    /// </summary>
    public abstract class AbstractSessionBytesStoreTests
    {
        protected const long RETENTION_PERIOD = 10_000L;
        protected const long SEGMENT_INTERVAL = 1_000L;
        protected const string STORE_NAME = "session-store";

        private StreamConfig config;
        private ProcessorStateManager stateManager;
        private ProcessorContext context;
        protected ISessionStore<Bytes, byte[]> store;

        internal abstract ISessionStore<Bytes, byte[]> CreateStore();

        protected virtual bool Persistent => false;

        [SetUp]
        public void Begin()
        {
            config = new StreamConfig {ApplicationId = "unit-test-session-store"};
            config.DefaultStateStoreCacheMaxBytes = 0;
            if (Persistent)
                config.UseRandomRocksDbConfigForTest();
            Configure(config);

            var id = new TaskId {Id = 0, Partition = 0};
            stateManager = new ProcessorStateManager(
                id,
                new List<TopicPartition> {new("source", 0)},
                new Dictionary<string, string> {{STORE_NAME, STORE_NAME + "-changelog"}},
                new MockChangelogRegister(),
                new MockOffsetCheckpointManager());

            var task = new Mock<AbstractTask>();
            task.Setup(k => k.Id).Returns(id);
            context = new ProcessorContext(task.Object, config, stateManager, new StreamMetricsRegistry());

            store = CreateStore();
            store.Init(context, store);
            context.SetRecordMetaData(new RecordContext(new Headers(), 0, 100, 0, "source"));
        }

        protected virtual void Configure(StreamConfig streamConfig)
        {
        }

        [TearDown]
        public void End()
        {
            store.Flush();
            stateManager.Close();
            if (Persistent)
                config.RemoveRocksDbFolderForTest();
        }

        #region Helpers

        protected static Bytes Key(string key) => Bytes.Wrap(Encoding.UTF8.GetBytes(key));

        protected static byte[] Value(long value) => BitConverter.GetBytes(value);

        protected static Windowed<Bytes> Session(string key, long start, long end)
            => new(Key(key), new SessionWindow(start, end));

        protected static List<(string key, long start, long end, long value)> ToList(IKeyValueEnumerator<Windowed<Bytes>, byte[]> enumerator)
        {
            var list = new List<(string, long, long, long)>();
            using (enumerator)
            {
                while (enumerator.MoveNext())
                {
                    var kv = enumerator.Current.Value;
                    list.Add((Encoding.UTF8.GetString(kv.Key.Key.Get), kv.Key.Window.StartMs, kv.Key.Window.EndMs,
                        BitConverter.ToInt64(kv.Value)));
                }
            }

            return list;
        }

        #endregion

        [Test]
        public void ShouldPutAndFindSessionsInRange()
        {
            store.Put(Session("a", 0, 0), Value(1));
            store.Put(Session("a", 10, 10), Value(2));
            store.Put(Session("a", 500, 1000), Value(3));
            store.Put(Session("aa", 0, 0), Value(4));

            CollectionAssert.AreEqual(
                new[] {("a", 0L, 0L, 1L), ("a", 10L, 10L, 2L)},
                ToList(store.FindSessions(Key("a"), 0, 10)));
            CollectionAssert.AreEqual(
                new[] {("a", 500L, 1000L, 3L)},
                ToList(store.FindSessions(Key("a"), 11, 1000)));
            CollectionAssert.AreEqual(
                new[] {("a", 0L, 0L, 1L), ("a", 10L, 10L, 2L), ("a", 500L, 1000L, 3L)},
                ToList(store.Fetch(Key("a"))));
        }

        [Test]
        public void FindSessionsBoundsAreInclusive()
        {
            store.Put(Session("a", 100, 200), Value(1));

            // end >= earliestSessionEndTime and start <= latestSessionStartTime
            Assert.AreEqual(1, ToList(store.FindSessions(Key("a"), 200, 100)).Count);
            Assert.AreEqual(0, ToList(store.FindSessions(Key("a"), 201, 300)).Count);
            Assert.AreEqual(0, ToList(store.FindSessions(Key("a"), 0, 99)).Count);
        }

        [Test]
        public void ShouldFindSessionsToMerge()
        {
            store.Put(Session("a", 0, 100), Value(1));
            store.Put(Session("a", 101, 200), Value(2));
            store.Put(Session("a", 201, 300), Value(3));
            store.Put(Session("a", 301, 400), Value(4));
            store.Put(Session("a", 401, 500), Value(5));

            CollectionAssert.AreEqual(
                new[] {("a", 101L, 200L, 2L), ("a", 201L, 300L, 3L)},
                ToList(store.FindSessions(Key("a"), 150, 300)));
        }

        [Test]
        public void ShouldFetchAllSessionsWithinKeyRange()
        {
            store.Put(Session("aa", 10, 10), Value(2));
            store.Put(Session("b", 1000, 1000), Value(4));
            store.Put(Session("a", 0, 0), Value(1));
            store.Put(Session("bb", 1500, 2000), Value(5));
            store.Put(Session("aaa", 100, 100), Value(3));
            store.Put(Session("bbb", 2500, 3000), Value(6));

            var sessions = ToList(store.Fetch(Key("aa"), Key("bb")));
            CollectionAssert.AreEquivalent(
                new[] {("aa", 10L, 10L, 2L), ("aaa", 100L, 100L, 3L), ("b", 1000L, 1000L, 4L), ("bb", 1500L, 2000L, 5L)},
                sessions);
        }

        [Test]
        public void ShouldFetchWithOpenKeyRange()
        {
            store.Put(Session("a", 0, 0), Value(1));
            store.Put(Session("b", 10, 10), Value(2));
            store.Put(Session("c", 20, 20), Value(3));

            CollectionAssert.AreEquivalent(
                new[] {("a", 0L, 0L, 1L), ("b", 10L, 10L, 2L)},
                ToList(store.Fetch(null, Key("b"))));
            CollectionAssert.AreEquivalent(
                new[] {("b", 10L, 10L, 2L), ("c", 20L, 20L, 3L)},
                ToList(store.Fetch(Key("b"), null)));
            Assert.AreEqual(3, ToList(store.Fetch(null, null)).Count);
        }

        [Test]
        public void ShouldBackwardFetchInReverseOrder()
        {
            store.Put(Session("a", 0, 0), Value(1));
            store.Put(Session("a", 10, 10), Value(2));
            store.Put(Session("a", 500, 1000), Value(3));

            CollectionAssert.AreEqual(
                new[] {("a", 500L, 1000L, 3L), ("a", 10L, 10L, 2L), ("a", 0L, 0L, 1L)},
                ToList(store.BackwardFetch(Key("a"))));
            CollectionAssert.AreEqual(
                new[] {("a", 10L, 10L, 2L), ("a", 0L, 0L, 1L)},
                ToList(store.BackwardFindSessions(Key("a"), 0, 10)));
        }

        [Test]
        public void ShouldFetchExactSession()
        {
            store.Put(Session("a", 0, 4), Value(1));
            store.Put(Session("aa", 0, 3), Value(2));
            store.Put(Session("aa", 0, 4), Value(3));
            store.Put(Session("aa", 1, 4), Value(4));
            store.Put(Session("aaa", 0, 4), Value(5));

            Assert.AreEqual(3L, BitConverter.ToInt64(store.FetchSession(Key("aa"), 0, 4)));
            Assert.IsNull(store.FetchSession(Key("aa"), 0, 5));
            Assert.IsNull(store.FetchSession(Key("b"), 0, 4));
        }

        [Test]
        public void ShouldRemove()
        {
            store.Put(Session("a", 0, 1000), Value(1));
            store.Put(Session("a", 1500, 2500), Value(2));

            store.Remove(Session("a", 0, 1000));

            Assert.AreEqual(0, ToList(store.FindSessions(Key("a"), 0, 1000)).Count);
            CollectionAssert.AreEqual(new[] {("a", 1500L, 2500L, 2L)}, ToList(store.FindSessions(Key("a"), 1400, 2600)));
            Assert.IsNull(store.FetchSession(Key("a"), 0, 1000));
        }

        [Test]
        public void ShouldRemoveOnNullAggregate()
        {
            store.Put(Session("a", 0, 1000), Value(1));
            store.Put(Session("a", 0, 1000), null);

            Assert.AreEqual(0, ToList(store.Fetch(Key("a"))).Count);
        }

        [Test]
        public void ShouldUpdateSessionValue()
        {
            store.Put(Session("a", 0, 10), Value(1));
            store.Put(Session("a", 0, 10), Value(2));

            CollectionAssert.AreEqual(new[] {("a", 0L, 10L, 2L)}, ToList(store.Fetch(Key("a"))));
        }

        [Test]
        public void ShouldReturnEmptyForInvalidKeyRange()
        {
            store.Put(Session("a", 0, 0), Value(1));
            Assert.AreEqual(0, ToList(store.Fetch(Key("b"), Key("a"))).Count);
        }

        [Test]
        public void ShouldNotFindExpiredSessions()
        {
            store.Put(Session("a", 0, 0), Value(1));
            store.Put(Session("a", 1, 1), Value(2));
            // moves the stream time far enough for the first sessions to expire
            store.Put(Session("a", RETENTION_PERIOD + 5 * SEGMENT_INTERVAL, RETENTION_PERIOD + 5 * SEGMENT_INTERVAL), Value(3));
            if (store is CachingSessionStore)
                store.Flush();

            CollectionAssert.AreEqual(
                new[] {("a", RETENTION_PERIOD + 5 * SEGMENT_INTERVAL, RETENTION_PERIOD + 5 * SEGMENT_INTERVAL, 3L)},
                ToList(store.Fetch(Key("a"))));
        }

        [Test]
        public void ShouldRestoreFromChangelogWithBatches()
        {
            var changelog = new TopicPartition(STORE_NAME + "-changelog", 0);
            var metadata = stateManager.GetStoreMetadata(changelog);
            Assert.IsNotNull(metadata.BatchRestoreCallback, "the session store must register a batch restore callback");

            ConsumeResult<byte[], byte[]> Record(long offset, string key, long start, long end, byte[] value)
                => new()
                {
                    TopicPartitionOffset = new TopicPartitionOffset(changelog, offset),
                    Message = new Message<byte[], byte[]>
                    {
                        Key = SessionKeySchema.ToBinary(Encoding.UTF8.GetBytes(key), start, end),
                        Value = value,
                        Timestamp = new Timestamp(end, TimestampType.CreateTime)
                    }
                };

            stateManager.Restore(metadata, new List<ConsumeResult<byte[], byte[]>>
            {
                Record(0, "a", 0, 10, Value(1)),
                Record(1, "a", 20, 30, Value(2)),
                Record(2, "b", 5, 5, Value(3)),
                Record(3, "a", 0, 10, null) // session merged and removed
            });

            CollectionAssert.AreEqual(new[] {("a", 20L, 30L, 2L)}, ToList(store.Fetch(Key("a"))));
            CollectionAssert.AreEqual(new[] {("b", 5L, 5L, 3L)}, ToList(store.Fetch(Key("b"))));
        }
    }

    public class InMemorySessionBytesStoreTests : AbstractSessionBytesStoreTests
    {
        internal override ISessionStore<Bytes, byte[]> CreateStore()
            => new InMemorySessionStore(STORE_NAME, RETENTION_PERIOD);
    }

    public class RocksDbSessionBytesStoreTests : AbstractSessionBytesStoreTests
    {
        protected override bool Persistent => true;

        internal override ISessionStore<Bytes, byte[]> CreateStore()
            => new RocksDbSessionBytesStoreSupplier(STORE_NAME, TimeSpan.FromMilliseconds(RETENTION_PERIOD), SEGMENT_INTERVAL).Get();
    }

    public class CachingInMemorySessionBytesStoreTests : AbstractSessionBytesStoreTests
    {
        protected override void Configure(StreamConfig streamConfig)
            => streamConfig.DefaultStateStoreCacheMaxBytes = 1024 * 1024;

        internal override ISessionStore<Bytes, byte[]> CreateStore()
            => new CachingSessionStore(new InMemorySessionStore(STORE_NAME, RETENTION_PERIOD), 1, null);
    }

    public class CachingRocksDbSessionBytesStoreTests : AbstractSessionBytesStoreTests
    {
        protected override bool Persistent => true;

        protected override void Configure(StreamConfig streamConfig)
            => streamConfig.DefaultStateStoreCacheMaxBytes = 1024 * 1024;

        internal override ISessionStore<Bytes, byte[]> CreateStore()
            => new CachingSessionStore(
                new RocksDbSessionBytesStoreSupplier(STORE_NAME, TimeSpan.FromMilliseconds(RETENTION_PERIOD), SEGMENT_INTERVAL).Get(),
                SEGMENT_INTERVAL,
                null);
    }
}
