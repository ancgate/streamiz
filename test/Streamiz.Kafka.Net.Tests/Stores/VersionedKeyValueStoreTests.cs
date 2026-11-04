using System;
using System.Collections.Generic;
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
using Streamiz.Kafka.Net.State.InMemory;
using Streamiz.Kafka.Net.Tests.Helpers;
using static Streamiz.Kafka.Net.State.VersionedKeyValueStoreReturnCodes;

namespace Streamiz.Kafka.Net.Tests.Stores
{
    /// <summary>
    /// Behaviour every versioned bytes store must have (cases ported from Kafka Streams' RocksDBVersionedStoreTest).
    /// History retention = 10 000 ms: records older than stream time - 10 000 are not put, and can't be read.
    /// </summary>
    public abstract class AbstractVersionedKeyValueStoreTests
    {
        protected const long HISTORY_RETENTION = 10_000L;
        protected const string STORE_NAME = "versioned-store";
        private const string CHANGELOG = STORE_NAME + "-changelog";

        private StreamConfig config;
        private ProcessorStateManager stateManager;
        private ProcessorContext context;
        private IVersionedKeyValueStore<Bytes, byte[]> store;

        internal abstract IVersionedKeyValueStore<Bytes, byte[]> CreateStore();

        protected virtual bool Persistent => false;

        [SetUp]
        public void Begin()
        {
            config = new StreamConfig {ApplicationId = "unit-test-versioned-store"};
            if (Persistent)
                config.UseRandomRocksDbConfigForTest();

            var id = new TaskId {Id = 0, Partition = 0};
            stateManager = new ProcessorStateManager(
                id,
                new List<TopicPartition> {new("source", 0)},
                new Dictionary<string, string> {{STORE_NAME, CHANGELOG}},
                new MockChangelogRegister(),
                new MockOffsetCheckpointManager());

            var task = new Mock<AbstractTask>();
            task.Setup(k => k.Id).Returns(id);
            context = new ProcessorContext(task.Object, config, stateManager, new StreamMetricsRegistry());

            store = CreateStore();
            store.Init(context, store);
            context.SetRecordMetaData(new RecordContext(new Headers(), 0, 100, 0, "source"));
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

        private static Bytes Key(string key) => Bytes.Wrap(Encoding.UTF8.GetBytes(key));

        private static byte[] Value(string value) => value == null ? null : Encoding.UTF8.GetBytes(value);

        private long Put(string key, string value, long timestamp) => store.Put(Key(key), Value(value), timestamp);

        private void AssertLatest(string key, string expectedValue, long expectedTimestamp)
        {
            var record = store.Get(Key(key));
            Assert.IsNotNull(record, $"latest record of {key}");
            Assert.AreEqual(expectedValue, Encoding.UTF8.GetString(record.Value));
            Assert.AreEqual(expectedTimestamp, record.Timestamp);
            Assert.IsNull(record.ValidTo);
        }

        private void AssertAsOf(string key, long asOf, string expectedValue, long expectedTimestamp, long? expectedValidTo)
        {
            var record = store.Get(Key(key), asOf);
            Assert.IsNotNull(record, $"record of {key} as of {asOf}");
            Assert.AreEqual(expectedValue, Encoding.UTF8.GetString(record.Value), $"value of {key} as of {asOf}");
            Assert.AreEqual(expectedTimestamp, record.Timestamp, $"timestamp of {key} as of {asOf}");
            Assert.AreEqual(expectedValidTo, record.ValidTo, $"validTo of {key} as of {asOf}");
        }

        private void AssertAsOfNull(string key, long asOf)
            => Assert.IsNull(store.Get(Key(key), asOf), $"record of {key} as of {asOf}");

        #endregion

        [Test]
        public void PutAndGetLatest()
        {
            Assert.AreEqual(PUT_RETURN_CODE_VALID_TO_UNDEFINED, Put("k", "v1", 10));
            AssertLatest("k", "v1", 10);
            AssertAsOf("k", 10, "v1", 10, null);
            AssertAsOf("k", 50, "v1", 10, null);
            AssertAsOfNull("k", 9);
            Assert.IsNull(store.Get(Key("unknown")));
            AssertAsOfNull("unknown", 10);
        }

        [Test]
        public void NewerRecordBecomesLatestAndOlderOneIsKeptInHistory()
        {
            Put("k", "v1", 10);
            Assert.AreEqual(PUT_RETURN_CODE_VALID_TO_UNDEFINED, Put("k", "v2", 20));

            AssertLatest("k", "v2", 20);
            AssertAsOf("k", 10, "v1", 10, 20);
            AssertAsOf("k", 19, "v1", 10, 20);
            AssertAsOf("k", 20, "v2", 20, null);
            AssertAsOfNull("k", 9);
        }

        [Test]
        public void RecordWithTheSameTimestampReplacesTheExistingOne()
        {
            Put("k", "v1", 10);
            Assert.AreEqual(PUT_RETURN_CODE_VALID_TO_UNDEFINED, Put("k", "v1b", 10));
            AssertLatest("k", "v1b", 10);

            // in the history: v1@10 (valid until 20), v2@20 (latest)
            Put("k", "v2", 20);
            Assert.AreEqual(20, Put("k", "v1c", 10));
            AssertAsOf("k", 15, "v1c", 10, 20);
            AssertLatest("k", "v2", 20);
        }

        [Test]
        public void OutOfOrderRecordIsInsertedInTheHistory()
        {
            Put("k", "v1", 10);
            Put("k", "v3", 30);

            // v2@20 goes between v1@10 and v3@30: it's valid until 30, and v1 is now valid until 20
            Assert.AreEqual(30, Put("k", "v2", 20));

            AssertLatest("k", "v3", 30);
            AssertAsOf("k", 15, "v1", 10, 20);
            AssertAsOf("k", 20, "v2", 20, 30);
            AssertAsOf("k", 29, "v2", 20, 30);
            AssertAsOf("k", 30, "v3", 30, null);
        }

        [Test]
        public void OutOfOrderRecordOlderThanAllTheRecords()
        {
            Put("k", "v3", 30);
            Assert.AreEqual(30, Put("k", "v1", 10));

            AssertAsOfNull("k", 9);
            AssertAsOf("k", 10, "v1", 10, 30);
            AssertLatest("k", "v3", 30);

            // then older again: valid until the oldest one (10)
            Assert.AreEqual(10, Put("k", "v0", 5));
            AssertAsOf("k", 7, "v0", 5, 10);
            AssertAsOf("k", 12, "v1", 10, 30);
        }

        [Test]
        public void LatestTombstone()
        {
            Put("k", "v1", 10);
            Assert.AreEqual(PUT_RETURN_CODE_VALID_TO_UNDEFINED, Put("k", null, 20));

            Assert.IsNull(store.Get(Key("k")));
            AssertAsOf("k", 15, "v1", 10, 20);
            AssertAsOfNull("k", 20);
            AssertAsOfNull("k", 25);
        }

        [Test]
        public void RecordAfterATombstone()
        {
            Put("k", "v1", 10);
            Put("k", null, 20);
            Assert.AreEqual(PUT_RETURN_CODE_VALID_TO_UNDEFINED, Put("k", "v3", 30));

            AssertLatest("k", "v3", 30);
            AssertAsOf("k", 15, "v1", 10, 20);
            AssertAsOfNull("k", 25);
            AssertAsOf("k", 30, "v3", 30, null);
        }

        [Test]
        public void OutOfOrderRecordBeforeATombstone()
        {
            Put("k", "v1", 10);
            Put("k", null, 20);
            Put("k", "v3", 30);

            // v15@15 is valid until the tombstone at 20
            Assert.AreEqual(20, Put("k", "v15", 15));

            AssertAsOf("k", 12, "v1", 10, 15);
            AssertAsOf("k", 17, "v15", 15, 20);
            AssertAsOfNull("k", 25);
            AssertLatest("k", "v3", 30);
        }

        [Test]
        public void OutOfOrderRecordBeforeALatestTombstone()
        {
            // the first record of the key is a tombstone
            Assert.AreEqual(PUT_RETURN_CODE_VALID_TO_UNDEFINED, Put("k", null, 20));
            Assert.IsNull(store.Get(Key("k")));

            Assert.AreEqual(20, Put("k", "v10", 10));
            AssertAsOf("k", 15, "v10", 10, 20);
            AssertAsOfNull("k", 20);
            Assert.IsNull(store.Get(Key("k")));

            // a value replacing the latest tombstone
            Assert.AreEqual(PUT_RETURN_CODE_VALID_TO_UNDEFINED, Put("k", "v20", 20));
            AssertLatest("k", "v20", 20);
            AssertAsOf("k", 15, "v10", 10, 20);
        }

        [Test]
        public void OutOfOrderTombstone()
        {
            Put("k", "v1", 10);
            Put("k", "v3", 30);

            Assert.AreEqual(30, Put("k", null, 20));

            AssertAsOf("k", 15, "v1", 10, 20);
            AssertAsOfNull("k", 25);
            AssertLatest("k", "v3", 30);
        }

        [Test]
        public void DeleteReturnsTheRecordValidAtTheTimestamp()
        {
            Put("k", "v1", 10);
            Put("k", "v2", 20);

            var deleted = store.Delete(Key("k"), 25);
            Assert.AreEqual("v2", Encoding.UTF8.GetString(deleted.Value));
            Assert.AreEqual(20, deleted.Timestamp);
            Assert.IsNull(deleted.ValidTo);
            Assert.IsNull(store.Get(Key("k")));

            // out-of-order delete in the history: v1 was valid until 20
            var deletedInHistory = store.Delete(Key("k"), 15);
            Assert.AreEqual("v1", Encoding.UTF8.GetString(deletedInHistory.Value));
            Assert.AreEqual(10, deletedInHistory.Timestamp);
            Assert.AreEqual(20, deletedInHistory.ValidTo);
            AssertAsOf("k", 12, "v1", 10, 15);
            AssertAsOfNull("k", 17);
            AssertAsOf("k", 22, "v2", 20, 25);

            Assert.IsNull(store.Delete(Key("unknown"), 10));
        }

        [Test]
        public void HistoryRetention()
        {
            Put("k", "v1", 10);
            Put("k", "v2", 20);
            // stream time = 20 000: the history can be read from 10 000
            Put("other", "x", 20_000);

            AssertAsOfNull("k", 15);
            AssertAsOfNull("k", 9_999);
            AssertAsOf("k", 10_000, "v2", 20, null);
            AssertLatest("k", "v2", 20);

            // too old: not put, nor deleted
            Assert.AreEqual(PUT_RETURN_CODE_NOT_PUT, Put("k", "old", 9_999));
            Assert.IsNull(store.Delete(Key("k"), 9_999));
            AssertLatest("k", "v2", 20);

            // at the limit: put (it's newer than the latest record)
            Assert.AreEqual(PUT_RETURN_CODE_VALID_TO_UNDEFINED, Put("k", "v3", 10_000));
            AssertLatest("k", "v3", 10_000);
            AssertAsOf("k", 10_000, "v3", 10_000, null);
        }

        [Test]
        public void HistoryIsReadableAcrossTimeAsStreamTimeAdvances()
        {
            // a version every second from 1 000 to 30 000: older segments are dropped as stream time advances
            for (long ts = 1_000; ts <= 30_000; ts += 1_000)
                Put("k", $"v{ts}", ts);

            // stream time = 30 000: the history can be read from 20 000
            AssertAsOfNull("k", 19_999);
            AssertAsOf("k", 20_000, "v20000", 20_000, 21_000);
            AssertAsOf("k", 20_500, "v20000", 20_000, 21_000);
            AssertAsOf("k", 29_999, "v29000", 29_000, 30_000);
            AssertLatest("k", "v30000", 30_000);

            // an out-of-order record within the history retention
            Assert.AreEqual(26_000, Put("k", "v25500", 25_500));
            AssertAsOf("k", 25_200, "v25000", 25_000, 25_500);
            AssertAsOf("k", 25_700, "v25500", 25_500, 26_000);
        }

        [Test]
        public void KeysAreIndependent()
        {
            // "ab" starts with "a": the records of "a" must not see the ones of "ab"
            Put("a", "x", 10);
            Put("ab", "y", 5);
            Put("b", "z", 7);

            AssertAsOfNull("a", 7);
            AssertAsOf("a", 10, "x", 10, null);
            AssertAsOf("ab", 7, "y", 5, null);
            AssertAsOf("b", 8, "z", 7, null);

            Put("a", null, 20);
            AssertLatest("ab", "y", 5);
            Assert.IsNull(store.Get(Key("a")));
        }

        [Test]
        public void RestoreFromChangelog()
        {
            // changelog records in their order: the same as the puts of OutOfOrderRecordBeforeATombstone
            var records = new List<ConsumeResult<byte[], byte[]>>
            {
                ChangelogRecord(0, "k", "v1", 10),
                ChangelogRecord(1, "k", null, 20),
                ChangelogRecord(2, "k", "v3", 30),
                ChangelogRecord(3, "k", "v15", 15),
                ChangelogRecord(4, "other", "x", 12),
            };

            var metadata = stateManager.GetStoreMetadata(new TopicPartition(CHANGELOG, 0));
            stateManager.Restore(metadata, records);

            AssertAsOf("k", 12, "v1", 10, 15);
            AssertAsOf("k", 17, "v15", 15, 20);
            AssertAsOfNull("k", 25);
            AssertLatest("k", "v3", 30);
            AssertLatest("other", "x", 12);
        }

        private static ConsumeResult<byte[], byte[]> ChangelogRecord(long offset, string key, string value, long timestamp)
            => new()
            {
                TopicPartitionOffset = new TopicPartitionOffset(CHANGELOG, 0, offset),
                Message = new Message<byte[], byte[]>
                {
                    Key = Encoding.UTF8.GetBytes(key),
                    Value = Value(value),
                    Timestamp = new Timestamp(timestamp, TimestampType.CreateTime),
                    Headers = new Headers()
                }
            };
    }

    public class InMemoryVersionedKeyValueStoreTests : AbstractVersionedKeyValueStoreTests
    {
        internal override IVersionedKeyValueStore<Bytes, byte[]> CreateStore()
            => new InMemoryVersionedKeyValueStore(STORE_NAME, HISTORY_RETENTION);
    }

    public class RocksDbVersionedKeyValueStoreTests : AbstractVersionedKeyValueStoreTests
    {
        protected override bool Persistent => true;

        // small segments: the history is spread over several segments, and older ones are dropped
        internal override IVersionedKeyValueStore<Bytes, byte[]> CreateStore()
            => new RocksDbVersionedKeyValueStore(STORE_NAME, HISTORY_RETENTION, 1_000, "rocksdb-versioned");
    }

    public class RocksDbVersionedKeyValueStoreDefaultSegmentTests : AbstractVersionedKeyValueStoreTests
    {
        protected override bool Persistent => true;

        internal override IVersionedKeyValueStore<Bytes, byte[]> CreateStore()
            => new RocksDbVersionedKeyValueBytesStoreSupplier(STORE_NAME, TimeSpan.FromMilliseconds(HISTORY_RETENTION))
                .GetVersionedStore();
    }
}
