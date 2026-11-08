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
    /// Sliding windows (KIP-450) with Count. Windows are written [start,end], both inclusive, end = start + time difference.
    /// The expected results follow Kafka Streams' KStreamSlidingWindowAggregate: for each record at t,
    /// existing windows containing t are updated, then the right window of the previous record, the left window [t-td,t]
    /// and the right window [t+1,t+1+td] of the record are created when they contain records.
    /// </summary>
    public class SlidingWindowKStreamCountTests
    {
        internal static readonly long TimeDifference = 10;

        internal static List<(string Key, long Start, long End, T Value)> Run<T>(
            SlidingWindowOptions windows,
            Func<ITimeWindowedKStream<string, string>, IKTable<Windowed<string>, T>> aggregation,
            ISerDes<T> valueSerdes,
            Action<TopologyTestDriver> assertDriver,
            params (string Key, string Value, long Timestamp)[] records)
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = $"test-sliding-window-{Guid.NewGuid()}"
            };

            var builder = new StreamBuilder();
            aggregation(builder
                    .Stream<string, string>("topic")
                    .GroupByKey()
                    .WindowedBy(windows))
                .ToStream()
                .To("output", new TimeWindowedSerDes<string>(new StringSerDes(), windows.TimeDifferenceMs), valueSerdes);

            using var driver = new TopologyTestDriver(builder.Build(), config);
            var input = driver.CreateInputTopic<string, string>("topic");
            var output = driver.CreateOutputTopic("output", TimeSpan.FromSeconds(1),
                new TimeWindowedSerDes<string>(new StringSerDes(), windows.TimeDifferenceMs), valueSerdes);

            foreach (var record in records)
                input.PipeInput(record.Key, record.Value, record.Timestamp.FromMilliseconds());

            var results = output.ReadKeyValueList()
                .Select(r => (r.Message.Key.Key, r.Message.Key.Window.StartMs, r.Message.Key.Window.EndMs, r.Message.Value))
                .ToList();
            assertDriver?.Invoke(driver);
            return results;
        }

        private static List<(string, long, long, long)> Count(SlidingWindowOptions windows,
            params (string, string, long)[] records)
            => Run(windows, w => w.Count(InMemoryWindows.As<string, long>("count-store")), new Int64SerDes(), null, records);

        [Test]
        public void InOrderRecordsCreateLeftAndRightWindows()
        {
            var results = Count(SlidingWindowOptions.Of(TimeDifference, 100),
                ("A", "a", 10), ("A", "b", 15), ("A", "c", 30));

            CollectionAssert.AreEqual(new List<(string, long, long, long)>
            {
                ("A", 0, 10, 1),  // 10: left window
                ("A", 11, 21, 1), // 15: right window of 10, holds 15
                ("A", 5, 15, 2),  // 15: left window, holds 10 and 15
                ("A", 20, 30, 1), // 30: left window, 15 is too far
            }, results);
        }

        [Test]
        public void RecordsAtTheTimeDifferenceAreInTheSameWindow()
        {
            var results = Count(SlidingWindowOptions.Of(TimeDifference, 100),
                ("A", "a", 10), ("A", "b", 20), ("A", "c", 21));

            CollectionAssert.AreEqual(new List<(string, long, long, long)>
            {
                ("A", 0, 10, 1),  // 10: left window
                ("A", 11, 21, 1), // 20: right window of 10
                ("A", 10, 20, 2), // 20: left window holds 10 too (inclusive start)
                ("A", 11, 21, 2), // 21: already its left window (inclusive end), now holds 20 and 21
                ("A", 21, 31, 1), // 21: right window of 20
            }, results);
        }

        [Test]
        public void OutOfOrderRecordUpdatesExistingWindowsAndCreatesItsWindows()
        {
            var results = Count(SlidingWindowOptions.Of(TimeDifference, 20),
                ("A", "a", 10), ("A", "b", 20), ("A", "c", 15));

            CollectionAssert.AreEqual(new List<(string, long, long, long)>
            {
                ("A", 0, 10, 1),
                ("A", 11, 21, 1),
                ("A", 10, 20, 2),
                ("A", 10, 20, 3), // 15: existing windows containing 15 are updated
                ("A", 11, 21, 2),
                ("A", 5, 15, 2),  // 15: left window, holds 10 and 15
                ("A", 16, 26, 1), // 15: right window, holds 20
            }, results);
        }

        [Test]
        public void LateRecordIsDropped()
        {
            var results = Count(SlidingWindowOptions.Of(TimeDifference),
                ("A", "a", 10), ("A", "b", 30), ("A", "c", 15));

            // no grace: at 15 the stream time is 30, every window of 15 ends before 30
            CollectionAssert.AreEqual(new List<(string, long, long, long)>
            {
                ("A", 0, 10, 1),
                ("A", 20, 30, 1),
            }, results);
        }

        [Test]
        public void OnlyTheWindowsWhichAreNotClosedAreUpdated()
        {
            var results = Count(SlidingWindowOptions.Of(TimeDifference, 5),
                ("A", "a", 10), ("A", "b", 30), ("A", "c", 22));

            // at 22, windows ending before 30 - 5 = 25 are closed: its left window [12,22] is not created
            CollectionAssert.AreEqual(new List<(string, long, long, long)>
            {
                ("A", 0, 10, 1),
                ("A", 20, 30, 1),
                ("A", 20, 30, 2), // still open, updated
                ("A", 23, 33, 1), // right window of 22, holds 30
            }, results);
        }

        [Test]
        public void EarlyRecordsGoInTheFirstWindow()
        {
            var results = Count(SlidingWindowOptions.Of(TimeDifference, 100),
                ("A", "a", 2), ("A", "b", 5), ("A", "c", 12));

            CollectionAssert.AreEqual(new List<(string, long, long, long)>
            {
                ("A", 0, 10, 1),  // 2: early records go in [0,10]
                ("A", 3, 13, 1),  // 5: right window of 2
                ("A", 0, 10, 2),  // 5: [0,10] holds 2 and 5
                ("A", 3, 13, 2),  // 12: updated
                ("A", 6, 16, 1),  // 12: right window of 5
                ("A", 2, 12, 3),  // 12: left window, holds 2, 5 and 12
            }, results);
        }

        [Test]
        public void EachKeyHasItsOwnWindows()
        {
            var results = Count(SlidingWindowOptions.Of(TimeDifference, 100),
                ("A", "a", 10), ("B", "b", 12), ("A", "c", 15));

            CollectionAssert.AreEqual(new List<(string, long, long, long)>
            {
                ("A", 0, 10, 1),
                ("B", 2, 12, 1),
                ("A", 11, 21, 1),
                ("A", 5, 15, 2),
            }, results);
        }

        [Test]
        public void NullValueIsSkipped()
        {
            var results = Count(SlidingWindowOptions.Of(TimeDifference, 100),
                ("A", "a", 10), ("A", null, 12));

            CollectionAssert.AreEqual(new List<(string, long, long, long)> {("A", 0, 10, 1)}, results);
        }

        [Test]
        public void StoreHoldsTheWindowsByStart()
        {
            List<KeyValuePair<long, long>> windows = null;
            Run(SlidingWindowOptions.Of(TimeDifference, 20),
                w => w.Count(InMemoryWindows.As<string, long>("count-store")),
                new Int64SerDes(),
                driver =>
                {
                    var store = driver.GetWindowStore<string, long>("count-store");
                    using var enumerator = store.Fetch("A", 0, 100);
                    windows = new List<KeyValuePair<long, long>>();
                    while (enumerator.MoveNext())
                        windows.Add(enumerator.Current.Value);
                },
                ("A", "a", 10), ("A", "b", 20), ("A", "c", 15));

            CollectionAssert.AreEqual(new List<KeyValuePair<long, long>>
            {
                new(0, 1), new(5, 2), new(10, 3), new(11, 2), new(16, 1)
            }, windows);
        }

        [Test]
        public void CachedStoreHoldsTheSameWindows()
        {
            List<KeyValuePair<long, long>> windows = null;
            Run(SlidingWindowOptions.Of(TimeDifference, 20),
                w => w.Count(InMemoryWindows.As<string, long>("count-store").WithCachingEnabled()),
                new Int64SerDes(),
                driver =>
                {
                    var store = driver.GetWindowStore<string, long>("count-store");
                    using var enumerator = store.Fetch("A", 0, 100);
                    windows = new List<KeyValuePair<long, long>>();
                    while (enumerator.MoveNext())
                        windows.Add(enumerator.Current.Value);
                },
                ("A", "a", 10), ("A", "b", 20), ("A", "c", 15));

            CollectionAssert.AreEqual(new List<KeyValuePair<long, long>>
            {
                new(0, 1), new(5, 2), new(10, 3), new(11, 2), new(16, 1)
            }, windows);
        }

        [Test]
        public void RocksDbStore()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes> {ApplicationId = "test-sliding-window-rocksdb"};
            config.UseRandomRocksDbConfigForTest();

            var builder = new StreamBuilder();
            builder.Stream<string, string>("topic")
                .GroupByKey()
                .WindowedBy(SlidingWindowOptions.Of(TimeDifference, 20))
                .Count(RocksDbWindows.As<string, long>("count-store"))
                .ToStream()
                .To("output", new TimeWindowedSerDes<string>(new StringSerDes(), TimeDifference), new Int64SerDes());

            try
            {
                using var driver = new TopologyTestDriver(builder.Build(), config);
                var input = driver.CreateInputTopic<string, string>("topic");
                var output = driver.CreateOutputTopic("output", TimeSpan.FromSeconds(1),
                    new TimeWindowedSerDes<string>(new StringSerDes(), TimeDifference), new Int64SerDes());
                input.PipeInput("A", "a", 10L.FromMilliseconds());
                input.PipeInput("A", "b", 20L.FromMilliseconds());
                input.PipeInput("A", "c", 15L.FromMilliseconds());

                var results = output.ReadKeyValueList()
                    .Select(r => (r.Message.Key.Window.StartMs, r.Message.Value))
                    .ToList();
                CollectionAssert.AreEqual(new List<(long, long)>
                {
                    (0, 1), (11, 1), (10, 2), (10, 3), (11, 2), (5, 2), (16, 1)
                }, results);
            }
            finally
            {
                config.RemoveRocksDbFolderForTest();
            }
        }

        [Test]
        public void RetentionMustHoldTwoTimeDifferencesAndTheGrace()
        {
            var windows = SlidingWindowOptions.Of(TimeSpan.FromHours(10), TimeSpan.FromHours(5));

            // 2 * 10h + 5h > 1 day (default retention)
            var builder = new StreamBuilder();
            Assert.Throws<ArgumentException>(() => builder.Stream<string, string>("topic")
                .GroupByKey()
                .WindowedBy(windows)
                .Count(Materialized<string, long, IWindowStore<Bytes, byte[]>>.Create("count-store")));

            Assert.DoesNotThrow(() => new StreamBuilder().Stream<string, string>("topic")
                .GroupByKey()
                .WindowedBy(windows)
                .Count(Materialized<string, long, IWindowStore<Bytes, byte[]>>.Create("count-store")
                    .WithRetention(TimeSpan.FromHours(25))));
        }

        [Test]
        public void InvalidOptions()
        {
            Assert.Throws<ArgumentException>(() => SlidingWindowOptions.Of(-1));
            Assert.Throws<ArgumentException>(() => SlidingWindowOptions.Of(10, -1));
            Assert.AreEqual(0, SlidingWindowOptions.Of(TimeSpan.FromSeconds(1)).GracePeriodMs);
            Assert.AreEqual(1000, SlidingWindowOptions.Of(TimeSpan.FromSeconds(1)).TimeDifferenceMs);
            Assert.AreEqual(2000, SlidingWindowOptions.Of(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(2)).GracePeriodMs);
        }
    }
}
