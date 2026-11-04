using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
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
using Streamiz.Kafka.Net.State.Cache;
using Streamiz.Kafka.Net.State.InMemory;
using Streamiz.Kafka.Net.State.Logging;

namespace Streamiz.Kafka.Net.Tests.Stores
{
    /// <summary>
    /// The dirty entries evicted by one cache compaction are written to the wrapped store in one batch, then forwarded
    /// one by one. The forwarded changes and the changelog records must be the same as with the eviction of each entry.
    /// </summary>
    public class CachingKeyValueStoreBatchEvictionTests
    {
        private static readonly StringSerDes serdes = new();

        private sealed class Harness : IDisposable
        {
            private readonly SyncKafkaSupplier kafkaSupplier = new();
            private readonly IRecordCollector recordCollector;
            private readonly ProcessorStateManager stateManager;

            public readonly ProcessorContext Context;
            public readonly InMemoryKeyValueStore Inner = new("test-store");
            public readonly CachingKeyValueStore Cache;

            // key, old value, new value, timestamp of the record context when forwarded
            public readonly List<(string key, string oldValue, string newValue, long timestamp)> Forwarded = new();

            // number of entries of each batch eviction
            public readonly List<int> BatchSizes = new();

            public Harness(long cacheSizeBytes, bool batchEviction)
            {
                var config = new StreamConfig {ApplicationId = "unit-test-cache-batch-eviction"};
                config.DefaultStateStoreCacheMaxBytes = cacheSizeBytes;
                var id = new TaskId {Id = 0, Partition = 0};

                var streamsProducer = new StreamsProducer(config, "thread", Guid.NewGuid(), kafkaSupplier, "log");
                recordCollector = new RecordCollector("p-1", config, id, streamsProducer,
                    new NoRunnableSensor("s", "s", MetricsRecordingLevel.DEBUG));

                stateManager = new ProcessorStateManager(
                    id,
                    new List<TopicPartition> {new("source", 0)},
                    new Dictionary<string, string> {{"test-store", "test-store-changelog"}},
                    new MockChangelogRegister(),
                    new MockOffsetCheckpointManager());

                var task = new Mock<AbstractTask>();
                task.Setup(k => k.Id).Returns(id);

                Context = new ProcessorContext(task.Object, config, stateManager, new StreamMetricsRegistry());
                Context.UseRecordCollector(recordCollector);

                Cache = new CachingKeyValueStore(new ChangeLoggingKeyValueBytesStore(Inner), null);
                Cache.Init(Context, Cache);
                var batchCallback = Cache.Cache.BatchEvictionCallback;
                Cache.Cache.BatchEvictionCallback = batchEviction
                    ? entries =>
                    {
                        BatchSizes.Add(entries.Count);
                        batchCallback(entries);
                    }
                    : null;

                Cache.SetFlushListener(kv => Forwarded.Add((
                    serdes.Deserialize(kv.Key, SerializationContext.Empty),
                    kv.Value.OldValue == null ? null : serdes.Deserialize(kv.Value.OldValue, SerializationContext.Empty),
                    kv.Value.NewValue == null ? null : serdes.Deserialize(kv.Value.NewValue, SerializationContext.Empty),
                    Context.Timestamp)), true);
            }

            public void Put(string key, string value, long timestamp)
            {
                Context.SetRecordMetaData(new RecordContext(new Headers(), timestamp, timestamp, 0, "source"));
                Cache.Put(Key(key), value == null ? null : serdes.Serialize(value, SerializationContext.Empty));
            }

            public void Delete(string key, long timestamp)
            {
                Context.SetRecordMetaData(new RecordContext(new Headers(), timestamp, timestamp, 0, "source"));
                Cache.Delete(Key(key));
            }

            public List<(string key, string value, long timestamp)> Changelog()
            {
                recordCollector.Flush();
                var consumer = kafkaSupplier.GetConsumer(new ConsumerConfig {GroupId = Guid.NewGuid().ToString()}, null);
                consumer.Subscribe("test-store-changelog");
                var records = new List<(string, string, long)>();
                ConsumeResult<byte[], byte[]> record;
                while ((record = consumer.Consume(TimeSpan.FromMilliseconds(100))) != null)
                    records.Add((
                        serdes.Deserialize(record.Message.Key, SerializationContext.Empty),
                        record.Message.Value == null ? null : serdes.Deserialize(record.Message.Value, SerializationContext.Empty),
                        record.Message.Timestamp.UnixTimestampMs));
                consumer.Dispose();
                return records;
            }

            public Dictionary<string, string> InnerContent()
                => Inner.All().ToDictionary(
                    kv => serdes.Deserialize(kv.Key.Get, SerializationContext.Empty),
                    kv => serdes.Deserialize(kv.Value, SerializationContext.Empty));

            public void Dispose()
            {
                stateManager.Close();
                recordCollector.Close(false);
            }
        }

        private static Bytes Key(string key) => Bytes.Wrap(serdes.Serialize(key, SerializationContext.Empty));

        [Test]
        public void FlushWritesAndForwardsTheDirtyEntriesInLeastRecentlyUsedOrder()
        {
            using var harness = new Harness(100000, true);
            harness.Inner.Put(Key("x"), serdes.Serialize("old", SerializationContext.Empty));

            harness.Put("a", "1", 100);
            Thread.Sleep(2);
            harness.Put("x", "2", 200);
            Thread.Sleep(2);
            harness.Put("a", "3", 300); // replaces a: a is now the most recently used entry
            Thread.Sleep(2);
            harness.Delete("z", 400); // z exists neither in the cache nor in the store
            harness.Cache.Flush();

            // eviction order x (200), a (300), z (400): z is skipped, it's a delete of an unknown key
            CollectionAssert.AreEqual(new[]
            {
                ("x", "old", "2", 200L),
                ("a", (string) null, "3", 300L)
            }, harness.Forwarded);

            // each changelog record has the timestamp of its own entry, not the one of the current record
            CollectionAssert.AreEqual(new[]
            {
                ("x", "2", 200L),
                ("a", "3", 300L)
            }, harness.Changelog());

            CollectionAssert.AreEquivalent(new Dictionary<string, string> {{"x", "2"}, {"a", "3"}}, harness.InnerContent());
        }

        [Test]
        public void UpdateOfACachedKeyWhenTheCacheIsFullIsNotLost()
        {
            // each entry is about 31 bytes: 3 entries fill the 100 bytes cache
            using var harness = new Harness(100, true);
            harness.Put("k1", "v1", 10);
            Thread.Sleep(2);
            harness.Put("k2", "v2", 20);
            Thread.Sleep(2);
            harness.Put("k3", "v3", 30);
            Thread.Sleep(2);
            harness.Put("k2", "v2b", 40); // over capacity, and k2 is already cached

            Assert.AreEqual("v2b", serdes.Deserialize(harness.Cache.Get(Key("k2")), SerializationContext.Empty));
            Assert.LessOrEqual(harness.Cache.Cache.Size, 100);

            harness.Cache.Flush();
            Assert.AreEqual("v2b", harness.InnerContent()["k2"]);
            Assert.AreEqual(("k2", "v2b"), harness.Changelog().Where(r => r.key == "k2").Select(r => (r.key, r.value)).Last());
        }

        [Test]
        public void FlushWritesAndForwardsTombstones()
        {
            using var harness = new Harness(100000, true);
            harness.Inner.Put(Key("x"), serdes.Serialize("old", SerializationContext.Empty));

            harness.Delete("x", 100);
            harness.Cache.Flush();

            CollectionAssert.AreEqual(new[] {("x", "old", (string) null, 100L)}, harness.Forwarded);
            CollectionAssert.AreEqual(new[] {("x", (string) null, 100L)}, harness.Changelog());
            CollectionAssert.IsEmpty(harness.InnerContent());
        }

        private static (List<(string, string, string, long)> forwarded,
            List<(string, string, long)> changelog,
            Dictionary<string, string> content,
            List<int> batchSizes) RunScenario(bool batchEviction)
        {
            // a small cache: puts trigger compactions when it is over capacity
            using var harness = new Harness(100, batchEviction);
            var keys = new[] {"k1", "k2", "k3", "k4", "k5", "k6"};
            harness.Inner.Put(Key("k3"), serdes.Serialize("stored", SerializationContext.Empty));

            long ts = 0;
            for (int i = 0; i < 30; ++i)
            {
                ts += 10;
                var key = keys[(i * 7) % keys.Length];
                if (i % 5 == 4)
                    harness.Delete(key, ts);
                else
                    harness.Put(key, $"v{i}", ts);
                Thread.Sleep(1); // distinct access times: a deterministic least recently used order
                if (i == 14)
                    harness.Cache.Flush();
            }

            harness.Cache.Flush();
            return (harness.Forwarded.ToList(), harness.Changelog(), harness.InnerContent(), harness.BatchSizes.ToList());
        }

        [Test]
        public void BatchEvictionGivesTheSameResultsAsTheEvictionOfEachEntry()
        {
            var perEntry = RunScenario(false);
            var batch = RunScenario(true);

            Assert.IsNotEmpty(perEntry.forwarded);
            // over capacity compactions and flushes, with several entries per batch
            Assert.Greater(batch.batchSizes.Count, 2);
            Assert.IsTrue(batch.batchSizes.Any(size => size > 1), string.Join(",", batch.batchSizes));
            CollectionAssert.AreEqual(perEntry.forwarded, batch.forwarded);
            CollectionAssert.AreEqual(perEntry.changelog, batch.changelog);
            CollectionAssert.AreEquivalent(perEntry.content, batch.content);
        }
    }
}
