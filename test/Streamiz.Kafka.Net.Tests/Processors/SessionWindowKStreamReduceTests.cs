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
    /// Session windows with Reduce: the reducer is also used to merge sessions,
    /// the aggregates of the merged sessions are reduced in the order the sessions are found (by end time).
    /// </summary>
    public class SessionWindowKStreamReduceTests
    {
        [Test]
        public void ReduceMergesSessionsWithTheReducer()
        {
            var results = Run(SessionWindowOptions.Of(Gap),
                w => w.Reduce((v1, v2) => v1 + "-" + v2,
                    InMemorySessions.As<string, string>("reduce-store")),
                new StringSerDes(), null,
                ("A", "a", 0), ("A", "b", 20), ("A", "c", 10), ("A", "d", 12));

            CollectionAssert.AreEqual(new List<(string, long, long, string)>
            {
                ("A", 0, 0, "a"),
                ("A", 20, 20, "b"),
                // 10: reduce("a", "b") = "a-b", then "a-b" + "c"
                ("A", 0, 0, null),
                ("A", 20, 20, null),
                ("A", 0, 20, "a-b-c"),
                // 12: falls into [0,20]
                ("A", 0, 20, null),
                ("A", 0, 20, "a-b-c-d"),
            }, results);
        }

        [Test]
        public void ReduceWithoutMaterialized()
        {
            // default store: RocksDB session store
            var results = Run(SessionWindowOptions.Of(Gap),
                w => w.Reduce((v1, v2) => v1 + v2),
                new StringSerDes(), null, ("A", "a", 0), ("A", "b", 1));

            CollectionAssert.AreEqual(new List<(string, long, long, string)>
            {
                ("A", 0, 0, "a"),
                ("A", 0, 0, null),
                ("A", 0, 1, "ab"),
            }, results);
        }
    }
}
