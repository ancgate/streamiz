using System.Collections.Generic;
using NUnit.Framework;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Kafka.Net.Tests.Processors
{
    /// <summary>
    /// Sliding windows (KIP-450) with Aggregate. Windows are written [start,end], both inclusive.
    /// </summary>
    public class SlidingWindowKStreamAggTests
    {
        private static readonly SlidingWindowOptions Windows =
            SlidingWindowOptions.Of(SlidingWindowKStreamCountTests.TimeDifference, 20);

        [Test]
        public void AggregateConcatenatesTheValuesOfEachWindow()
        {
            var results = SlidingWindowKStreamCountTests.Run(Windows,
                w => w.Aggregate(
                    () => "",
                    (_, v, agg) => agg + v,
                    InMemoryWindows.As<string, string>("agg-store").WithValueSerdes<StringSerDes>()),
                new StringSerDes(),
                null,
                ("A", "a", 10), ("A", "b", 20), ("A", "c", 15));

            CollectionAssert.AreEqual(new List<(string, long, long, string)>
            {
                ("A", 0, 10, "a"),
                ("A", 11, 21, "b"),   // 20: right window of 10
                ("A", 10, 20, "ab"),  // 20: left window starts from the aggregate of 10's window
                ("A", 10, 20, "abc"), // 15 (out of order): added to the existing windows
                ("A", 11, 21, "bc"),
                ("A", 5, 15, "ac"),   // 15: left window, from the aggregate of [0,10]
                ("A", 16, 26, "b"),   // 15: right window, the aggregate of 20
            }, results);
        }

        [Test]
        public void AggregateWithSerdesType()
        {
            var results = SlidingWindowKStreamCountTests.Run(Windows,
                w => w.Aggregate<int, Int32SerDes>(() => 0, (_, v, agg) => agg + v.Length),
                new Int32SerDes(),
                null,
                ("A", "aa", 10), ("A", "bbb", 15));

            CollectionAssert.AreEqual(new List<(string, long, long, int)>
            {
                ("A", 0, 10, 2),
                ("A", 11, 21, 3),
                ("A", 5, 15, 5),
            }, results);
        }
    }
}
