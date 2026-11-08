using System.Collections.Generic;
using NUnit.Framework;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;
using static Streamiz.Kafka.Net.Tests.Processors.SessionWindowKStreamCountTests;

namespace Streamiz.Kafka.Net.Tests.Processors
{
    /// <summary>
    /// Session windows with Aggregate: values are concatenated, and the merger concatenates the aggregates of the merged sessions
    /// (in the order the sessions are found: by end time). Tombstones of merged sessions are null.
    /// </summary>
    public class SessionWindowKStreamAggTests
    {
        private static List<(string, long, long, string)> Concat(SessionWindowOptions windows, params (string, string, long)[] records)
            => Run(windows,
                w => w.Aggregate(
                    () => "",
                    (_, v, agg) => agg + v,
                    (_, agg1, agg2) => agg1 + agg2,
                    InMemorySessions.As<string, string>("agg-store").WithValueSerdes(new StringSerDes())),
                new StringSerDes(), null, records);

        [Test]
        public void AggregateWithMerger()
        {
            var results = Concat(SessionWindowOptions.Of(Gap), ("A", "a", 0), ("A", "b", 20), ("A", "c", 10), ("B", "d", 25));

            CollectionAssert.AreEqual(new List<(string, long, long, string)>
            {
                ("A", 0, 0, "a"),
                ("A", 20, 20, "b"),
                // 10: merger("", "a") = "a", merger("a", "b") = "ab", then "ab" + "c"
                ("A", 0, 0, null),
                ("A", 20, 20, null),
                ("A", 0, 20, "abc"),
                ("B", 25, 25, "d"),
            }, results);
        }

        [Test]
        public void AggregateWithValueSerdesType()
        {
            var results = Run(SessionWindowOptions.Of(Gap),
                w => w.Aggregate<string, StringSerDes>(() => "", (_, v, agg) => agg + v, (_, agg1, agg2) => agg1 + agg2),
                new StringSerDes(), null, ("A", "a", 0), ("A", "b", 3));

            CollectionAssert.AreEqual(new List<(string, long, long, string)>
            {
                ("A", 0, 0, "a"),
                ("A", 0, 0, null),
                ("A", 0, 3, "ab"),
            }, results);
        }
    }
}
