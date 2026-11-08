using System;
using System.Collections.Generic;
using System.Linq;
using NUnit.Framework;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;
using Streamiz.Kafka.Net.Tests.Helpers;

namespace Streamiz.Kafka.Net.Tests.Processors
{
    /// <summary>
    /// Session windows with Count. Sessions are written [start,end], both inclusive.
    /// The expected results follow Kafka Streams' KStreamSessionWindowAggregate: a record at t merges the sessions of its key
    /// which end at or after t - gap and start at or before t + gap; a tombstone is forwarded for each merged session, then the
    /// new session. A record is dropped when its (merged) session ends before stream time - grace - gap.
    /// Count is nullable (long?): the tombstones of merged sessions are forwarded with a null count.
    /// </summary>
    public class SessionWindowKStreamCountTests
    {
        internal static readonly long Gap = 10;

        internal static List<(string Key, long Start, long End, T Value)> Run<T>(
            SessionWindowOptions windows,
            Func<ISessionWindowedKStream<string, string>, IKTable<Windowed<string>, T>> aggregation,
            ISerDes<T> valueSerdes,
            Action<TopologyTestDriver> assertDriver,
            params (string Key, string Value, long Timestamp)[] records)
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = $"test-session-window-{Guid.NewGuid()}"
            };

            var builder = new StreamBuilder();
            aggregation(builder
                    .Stream<string, string>("topic")
                    .GroupByKey()
                    .WindowedBy(windows))
                .ToStream()
                .To("output", new SessionWindowedSerDes<string>(new StringSerDes()), valueSerdes);

            using var driver = new TopologyTestDriver(builder.Build(), config);
            var input = driver.CreateInputTopic<string, string>("topic");
            var output = driver.CreateOutputTopic("output", TimeSpan.FromSeconds(1),
                new SessionWindowedSerDes<string>(new StringSerDes()), valueSerdes);

            foreach (var record in records)
                input.PipeInput(record.Key, record.Value, record.Timestamp.FromMilliseconds());

            assertDriver?.Invoke(driver);
            return output.ReadKeyValueList()
                .Select(r => (r.Message.Key.Key, r.Message.Key.Window.StartMs, r.Message.Key.Window.EndMs, r.Message.Value))
                .ToList();
        }

        private static List<(string, long, long, long?)> Count(SessionWindowOptions windows,
            params (string, string, long)[] records)
            => Run(windows, w => w.Count(InMemorySessions.As<string, long?>("count-store")), new NullableInt64SerDes(), null, records);

        [Test]
        public void FirstRecordStartsASession()
        {
            var results = Count(SessionWindowOptions.Of(Gap), ("A", "a", 0));

            CollectionAssert.AreEqual(new List<(string, long, long, long?)>
            {
                ("A", 0, 0, 1)
            }, results);
        }

        [Test]
        public void RecordWithinTheGapExtendsTheSession()
        {
            var results = Count(SessionWindowOptions.Of(Gap), ("A", "a", 0), ("A", "b", 5), ("A", "c", 15));

            CollectionAssert.AreEqual(new List<(string, long, long, long?)>
            {
                ("A", 0, 0, 1),
                ("A", 0, 0, null),  // 5: [0,0] is merged, tombstone
                ("A", 0, 5, 2),
                ("A", 0, 5, null),  // 15: [0,5] ends at 5 >= 15 - 10, merged
                ("A", 0, 15, 3),
            }, results);
        }

        [Test]
        public void TheGapIsInclusive()
        {
            var results = Count(SessionWindowOptions.Of(Gap), ("A", "a", 0), ("A", "b", 10), ("A", "c", 21));

            CollectionAssert.AreEqual(new List<(string, long, long, long?)>
            {
                ("A", 0, 0, 1),
                ("A", 0, 0, null),   // 10: 10 - 0 = gap, same session
                ("A", 0, 10, 2),
                ("A", 21, 21, 1), // 21: 21 - 10 > gap, new session
            }, results);
        }

        [Test]
        public void RecordBetweenTwoSessionsMergesThem()
        {
            var results = Count(SessionWindowOptions.Of(Gap), ("A", "a", 0), ("A", "b", 20), ("A", "c", 10));

            CollectionAssert.AreEqual(new List<(string, long, long, long?)>
            {
                ("A", 0, 0, 1),
                ("A", 20, 20, 1),
                // 10: close time = 20 - 0 - 10 = 10, [0,20] ends at 20, it is accepted and merges both sessions
                ("A", 0, 0, null),
                ("A", 20, 20, null),
                ("A", 0, 20, 3),
            }, results);
        }

        [Test]
        public void LateRecordIsDropped()
        {
            var results = Count(SessionWindowOptions.Of(Gap), ("A", "a", 0), ("A", "b", 100), ("A", "c", 50), ("A", "d", 85));

            CollectionAssert.AreEqual(new List<(string, long, long, long?)>
            {
                ("A", 0, 0, 1),
                ("A", 100, 100, 1),
                // 50 and 85: close time = 100 - 0 - 10 = 90, their sessions end before: dropped
            }, results);
        }

        [Test]
        public void LateRecordMergingAnOpenSessionIsAccepted()
        {
            var results = Count(SessionWindowOptions.Of(Gap), ("A", "a", 100), ("A", "b", 92));

            CollectionAssert.AreEqual(new List<(string, long, long, long?)>
            {
                ("A", 100, 100, 1),
                // 92: close time = 90, it merges [100,100] into [92,100] which ends at 100
                ("A", 100, 100, null),
                ("A", 92, 100, 2),
            }, results);
        }

        [Test]
        public void OutOfOrderRecordWithinGrace()
        {
            var results = Count(SessionWindowOptions.Of(Gap, 20),
                ("A", "a", 0), ("A", "b", 30), ("A", "c", 15), ("A", "d", 25), ("A", "e", 1));

            CollectionAssert.AreEqual(new List<(string, long, long, long?)>
            {
                ("A", 0, 0, 1),
                ("A", 30, 30, 1),
                // 15: close time = 30 - 20 - 10 = 0, [0,0] ends before 15 - 10 and [30,30] starts after 15 + 10: new session
                ("A", 15, 15, 1),
                // 25: merges [15,15] and [30,30]
                ("A", 15, 15, null),
                ("A", 30, 30, null),
                ("A", 15, 30, 3),
                // 1: merges [0,0] (ends at 0 >= close time 0), [15,30] starts at 15 > 1 + 10
                ("A", 0, 0, null),
                ("A", 0, 1, 2),
            }, results);
        }

        [Test]
        public void KeysHaveTheirOwnSessions()
        {
            var results = Count(SessionWindowOptions.Of(Gap), ("A", "a", 0), ("B", "b", 5), ("A", "c", 8));

            CollectionAssert.AreEqual(new List<(string, long, long, long?)>
            {
                ("A", 0, 0, 1),
                ("B", 5, 5, 1),
                ("A", 0, 0, null),
                ("A", 0, 8, 2),
            }, results);
        }

        [Test]
        public void NullValueIsCounted()
        {
            var results = Count(SessionWindowOptions.Of(Gap), ("A", null, 0), ("A", "b", 5));

            CollectionAssert.AreEqual(new List<(string, long, long, long?)>
            {
                ("A", 0, 0, 1),
                ("A", 0, 0, null),
                ("A", 0, 5, 2),
            }, results);
        }

        [Test]
        public void StoreHoldsTheSessions()
        {
            List<KeyValuePair<Windowed<string>, long?>> sessions = null;
            long? fetched = 0;
            Run(SessionWindowOptions.Of(Gap),
                w => w.Count(InMemorySessions.As<string, long?>("count-store")),
                new NullableInt64SerDes(),
                driver =>
                {
                    var store = driver.GetSessionStore<string, long?>("count-store");
                    fetched = store.FetchSession("A", 0, 20);
                    using var enumerator = store.Fetch("A");
                    sessions = new List<KeyValuePair<Windowed<string>, long?>>();
                    while (enumerator.MoveNext())
                        sessions.Add(enumerator.Current.Value);
                },
                ("A", "a", 0), ("A", "b", 20), ("A", "c", 10), ("A", "d", 40));

            Assert.AreEqual(3, fetched);
            CollectionAssert.AreEqual(new (long, long, long?)[] {(0L, 20L, 3L), (40L, 40L, 1L)},
                sessions.Select(s => (s.Key.Window.StartMs, s.Key.Window.EndMs, s.Value)).ToArray());
        }

        [Test]
        public void CachedStoreForwardsTheLatestSessionsOnCommit()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes> {ApplicationId = "test-session-window-cache"};

            var builder = new StreamBuilder();
            builder.Stream<string, string>("topic")
                .GroupByKey()
                .WindowedBy(SessionWindowOptions.Of(Gap))
                .Count(InMemorySessions.As<string, long?>("count-store").WithCachingEnabled())
                .ToStream()
                .To("output", new SessionWindowedSerDes<string>(new StringSerDes()), new NullableInt64SerDes());

            using var driver = new TopologyTestDriver(builder.Build(), config);
            var input = driver.CreateInputTopic<string, string>("topic");
            var output = driver.CreateOutputTopic("output", TimeSpan.FromSeconds(1),
                new SessionWindowedSerDes<string>(new StringSerDes()), new NullableInt64SerDes());

            input.PipeInput("A", "a", 0L.FromMilliseconds());
            input.PipeInput("A", "b", 5L.FromMilliseconds());
            input.PipeInput("A", "c", 30L.FromMilliseconds());

            Assert.AreEqual(0, output.ReadKeyValueList().Count());

            driver.Commit();

            // [0,0] was merged before being flushed: neither the session nor its tombstone is forwarded
            CollectionAssert.AreEquivalent(new (long, long, long?)[] {(0L, 5L, 2L), (30L, 30L, 1L)},
                output.ReadKeyValueList()
                    .Select(r => (r.Message.Key.Window.StartMs, r.Message.Key.Window.EndMs, r.Message.Value))
                    .ToArray());

            var store = driver.GetSessionStore<string, long?>("count-store");
            Assert.AreEqual(2, store.FetchSession("A", 0, 5));
            Assert.IsNull(store.FetchSession("A", 0, 0));
        }

        [Test]
        public void RocksDbStore()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes> {ApplicationId = "test-session-window-rocksdb"};
            config.UseRandomRocksDbConfigForTest();

            var builder = new StreamBuilder();
            builder.Stream<string, string>("topic")
                .GroupByKey()
                .WindowedBy(SessionWindowOptions.Of(Gap))
                .Count(RocksDbSessions.As<string, long?>("count-store"))
                .ToStream()
                .To("output", new SessionWindowedSerDes<string>(new StringSerDes()), new NullableInt64SerDes());

            try
            {
                using var driver = new TopologyTestDriver(builder.Build(), config);
                var input = driver.CreateInputTopic<string, string>("topic");
                var output = driver.CreateOutputTopic("output", TimeSpan.FromSeconds(1),
                    new SessionWindowedSerDes<string>(new StringSerDes()), new NullableInt64SerDes());

                input.PipeInput("A", "a", 0L.FromMilliseconds());
                input.PipeInput("A", "b", 20L.FromMilliseconds());
                input.PipeInput("A", "c", 10L.FromMilliseconds());

                CollectionAssert.AreEqual(new (long, long, long?)[]
                    {
                        (0L, 0L, 1L), (20L, 20L, 1L), (0L, 0L, null), (20L, 20L, null), (0L, 20L, 3L)
                    },
                    output.ReadKeyValueList()
                        .Select(r => (r.Message.Key.Window.StartMs, r.Message.Key.Window.EndMs, r.Message.Value))
                        .ToArray());

                var store = driver.GetSessionStore<string, long?>("count-store");
                Assert.AreEqual(3, store.FetchSession("A", 0, 20));
            }
            finally
            {
                config.RemoveRocksDbFolderForTest();
            }
        }

        [Test]
        public void DefaultMaterialized()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes> {ApplicationId = "test-session-window-default"};
            config.UseRandomRocksDbConfigForTest();

            var builder = new StreamBuilder();
            builder.Stream<string, string>("topic")
                .GroupByKey()
                .WindowedBy(SessionWindowOptions.Of(TimeSpan.FromMilliseconds(Gap)))
                .Count()
                .ToStream()
                .To("output", new SessionWindowedSerDes<string>(new StringSerDes()), new NullableInt64SerDes());

            try
            {
                using var driver = new TopologyTestDriver(builder.Build(), config);
                var input = driver.CreateInputTopic<string, string>("topic");
                var output = driver.CreateOutputTopic("output", TimeSpan.FromSeconds(1),
                    new SessionWindowedSerDes<string>(new StringSerDes()), new NullableInt64SerDes());

                input.PipeInput("A", "a", 0L.FromMilliseconds());
                input.PipeInput("A", "b", 3L.FromMilliseconds());

                var results = output.ReadKeyValueList().ToList();
                Assert.AreEqual(3, results.Count);
                Assert.AreEqual(2, results[2].Message.Value);
            }
            finally
            {
                config.RemoveRocksDbFolderForTest();
            }
        }

        [Test]
        public void RetentionSmallerThanGapPlusGraceIsRejected()
        {
            var builder = new StreamBuilder();
            var windowed = builder.Stream<string, string>("topic")
                .GroupByKey()
                .WindowedBy(SessionWindowOptions.Of(TimeSpan.FromMinutes(10), TimeSpan.FromMinutes(5)));

            Assert.Throws<ArgumentException>(() =>
                windowed.Count(InMemorySessions.As<string, long?>("count-store", TimeSpan.FromMinutes(14))));
        }

        [Test]
        public void InvalidOptions()
        {
            Assert.Throws<ArgumentException>(() => SessionWindowOptions.Of(0));
            Assert.Throws<ArgumentException>(() => SessionWindowOptions.Of(-1));
            Assert.Throws<ArgumentException>(() => SessionWindowOptions.Of(10, -1));
        }

        [Test]
        public void TombstonesAreNullAndSuppressDropsTheNeverEmittedOnes()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes> {ApplicationId = "test-session-window-count-suppress"};

            var builder = new StreamBuilder();
            builder.Stream<string, string>("topic")
                .GroupByKey()
                .WindowedBy(SessionWindowOptions.Of(Gap))
                .Count(InMemorySessions.As<string, long?>("count-store"))
                .Suppress(SuppressedBuilder.UntilWindowClose<Windowed<string>, long?>(
                        TimeSpan.FromMilliseconds(Gap), StrictBufferConfig.Unbounded())
                    .WithKeySerdes(new SessionWindowedSerDes<string>(new StringSerDes()))
                    .WithValueSerdes(new NullableInt64SerDes()))
                .ToStream()
                .To("output", new SessionWindowedSerDes<string>(new StringSerDes()), new NullableInt64SerDes());

            using var driver = new TopologyTestDriver(builder.Build(), config);
            var input = driver.CreateInputTopic<string, string>("topic");
            var output = driver.CreateOutputTopic("output", TimeSpan.FromSeconds(1),
                new SessionWindowedSerDes<string>(new StringSerDes()), new NullableInt64SerDes());

            input.PipeInput("A", "a", 0L.FromMilliseconds());
            // 5: [0,0] is merged into [0,5]; its null tombstone is dropped by suppress, [0,0] was never emitted
            input.PipeInput("A", "b", 5L.FromMilliseconds());
            // 15: 15 - 10 = 5, [0,5] is closed and emitted: only the final count, no tombstone
            input.PipeInput("B", "x", 15L.FromMilliseconds());

            CollectionAssert.AreEqual(new (string, long, long, long?)[] {("A", 0L, 5L, 2L)},
                output.ReadKeyValueList()
                    .Select(r => (r.Message.Key.Key, r.Message.Key.Window.StartMs, r.Message.Key.Window.EndMs, r.Message.Value))
                    .ToArray());
        }

        [Test]
        public void TombstonesAreWrittenAsNullValues()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes> {ApplicationId = "test-session-window-count-tombstone"};

            var builder = new StreamBuilder();
            builder.Stream<string, string>("topic")
                .GroupByKey()
                .WindowedBy(SessionWindowOptions.Of(Gap))
                .Count(InMemorySessions.As<string, long?>("count-store"))
                .ToStream()
                .To("output", new SessionWindowedSerDes<string>(new StringSerDes()), new NullableInt64SerDes());

            using var driver = new TopologyTestDriver(builder.Build(), config);
            var input = driver.CreateInputTopic<string, string>("topic");
            // raw bytes: a tombstone must be a null value in the topic, not 8 bytes for 0
            var output = driver.CreateOutputTopic("output", TimeSpan.FromSeconds(1),
                new SessionWindowedSerDes<string>(new StringSerDes()), new ByteArraySerDes());

            input.PipeInput("A", "a", 0L.FromMilliseconds());
            input.PipeInput("A", "b", 5L.FromMilliseconds());

            var records = output.ReadKeyValueList().ToList();
            Assert.AreEqual(3, records.Count);
            Assert.AreEqual(8, records[0].Message.Value.Length);
            Assert.IsNull(records[1].Message.Value); // tombstone of [0,0]
            Assert.AreEqual(2L, BitConverter.ToInt64(records[2].Message.Value, 0));
        }

        [Test]
        public void SuppressUntilWindowCloseEmitsTheFinalSessions()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes> {ApplicationId = "test-session-window-suppress"};

            var builder = new StreamBuilder();
            builder.Stream<string, string>("topic")
                .GroupByKey()
                .WindowedBy(SessionWindowOptions.Of(Gap))
                .Aggregate(() => (string) null,
                    (_, v, agg) => agg == null ? v : agg + v,
                    (_, agg1, agg2) => agg1 + agg2,
                    InMemorySessions.As<string, string>("agg-store").WithValueSerdes(new StringSerDes()))
                // a session is closed at end + gap + grace
                .Suppress(SuppressedBuilder.UntilWindowClose<Windowed<string>, string>(
                        TimeSpan.FromMilliseconds(Gap), StrictBufferConfig.Unbounded())
                    .WithKeySerdes(new SessionWindowedSerDes<string>(new StringSerDes()))
                    .WithValueSerdes(new StringSerDes()))
                .ToStream()
                .To("output", new SessionWindowedSerDes<string>(new StringSerDes()), new StringSerDes());

            using var driver = new TopologyTestDriver(builder.Build(), config);
            var input = driver.CreateInputTopic<string, string>("topic");
            var output = driver.CreateOutputTopic("output", TimeSpan.FromSeconds(1),
                new SessionWindowedSerDes<string>(new StringSerDes()), new StringSerDes());

            // a session is emitted once its end <= stream time - (gap + grace), like Kafka Streams
            input.PipeInput("A", "a", 0L.FromMilliseconds());
            input.PipeInput("A", "b", 5L.FromMilliseconds());
            // 14: 14 - 10 = 4 < 5, [0,5] is still buffered. The tombstone of [0,0] is dropped, it was never emitted
            input.PipeInput("B", "x", 14L.FromMilliseconds());
            Assert.AreEqual(0, output.ReadKeyValueList().Count());

            // 15: 15 - 10 = 5, [0,5] is emitted
            input.PipeInput("B", "y", 15L.FromMilliseconds());
            CollectionAssert.AreEqual(new[] {("A", 0L, 5L, "ab")},
                output.ReadKeyValueList()
                    .Select(r => (r.Message.Key.Key, r.Message.Key.Window.StartMs, r.Message.Key.Window.EndMs, r.Message.Value))
                    .ToArray());
        }
    }
}
