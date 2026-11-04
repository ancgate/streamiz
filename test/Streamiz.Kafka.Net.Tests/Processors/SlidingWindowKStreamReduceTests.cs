using System.Collections.Generic;
using NUnit.Framework;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Kafka.Net.Tests.Processors
{
    /// <summary>
    /// Sliding windows (KIP-450) with Reduce. Windows are written [start,end], both inclusive.
    /// </summary>
    public class SlidingWindowKStreamReduceTests
    {
        private static readonly SlidingWindowOptions Windows =
            SlidingWindowOptions.Of(SlidingWindowKStreamCountTests.TimeDifference, 20);

        [Test]
        public void ReduceConcatenatesTheValuesOfEachWindow()
        {
            var results = SlidingWindowKStreamCountTests.Run(Windows,
                w => w.Reduce((v1, v2) => v1 + v2, InMemoryWindows.As<string, string>("reduce-store")),
                new StringSerDes(),
                null,
                ("A", "a", 10), ("A", "b", 20), ("A", "c", 15));

            // same windows as the aggregate: a reduce starts each new window with the first value
            CollectionAssert.AreEqual(new List<(string, long, long, string)>
            {
                ("A", 0, 10, "a"),
                ("A", 11, 21, "b"),
                ("A", 10, 20, "ab"),
                ("A", 10, 20, "abc"),
                ("A", 11, 21, "bc"),
                ("A", 5, 15, "ac"),
                ("A", 16, 26, "b"),
            }, results);
        }

        [Test]
        public void ReduceWithoutMaterialized()
        {
            var results = SlidingWindowKStreamCountTests.Run(Windows,
                w => w.Reduce((v1, v2) => v1 + v2),
                new StringSerDes(),
                null,
                ("A", "a", 10), ("A", "b", 15));

            CollectionAssert.AreEqual(new List<(string, long, long, string)>
            {
                ("A", 0, 10, "a"),
                ("A", 11, 21, "b"),
                ("A", 5, 15, "ab"),
            }, results);
        }
    }
}
