using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Confluent.Kafka;
using NUnit.Framework;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Kafka.Net.Tests.Processors
{
    /// <summary>
    /// Stream-table join with a grace period (KIP-923): grace = 1 s, table history retention = 10 s.
    /// A stream record is buffered until the stream time reaches its timestamp + grace, then joined with the table
    /// value valid at its timestamp.
    /// </summary>
    public class KStreamTableJoinGracePeriodTests
    {
        private static readonly TimeSpan Grace = TimeSpan.FromSeconds(1);
        private static readonly TimeSpan HistoryRetention = TimeSpan.FromSeconds(10);
        private static readonly DateTime Epoch = DateTimeOffset.FromUnixTimeMilliseconds(0).UtcDateTime;

        private static DateTime At(long ms) => Epoch.AddMilliseconds(ms);

        private static StreamConfig<StringSerDes, StringSerDes> Config()
            => new() {ApplicationId = $"test-stream-table-join-grace-{Guid.NewGuid()}"};

        private static StreamTableJoinProps<string, string, string> Props(TimeSpan grace)
            => new StreamTableJoinProps<string, string, string>(new StringSerDes(), new StringSerDes(), new StringSerDes())
                .WithGracePeriod(grace);

        private static (TopologyTestDriver driver, TestInputTopic<string, string> table, TestInputTopic<string, string> stream, TestOutputTopic<string, string> output)
            Create(bool leftJoin, TimeSpan grace, bool versioned = true)
        {
            var builder = new StreamBuilder();
            var table = versioned
                ? builder.Table("table", InMemoryVersioned.As<string, string>("table-store", HistoryRetention))
                : builder.Table("table", InMemory.As<string, string>("table-store"));
            var stream = builder.Stream<string, string>("stream");
            var joined = leftJoin
                ? stream.LeftJoin(table, (s, t) => $"{s}-{t ?? "null"}", Props(grace))
                : stream.Join(table, (s, t) => $"{s}-{t}", Props(grace));
            joined.To("output");

            var driver = new TopologyTestDriver(builder.Build(), Config());
            return (driver,
                driver.CreateInputTopic<string, string>("table"),
                driver.CreateInputTopic<string, string>("stream"),
                driver.CreateOutputTopic<string, string>("output"));
        }

        private static List<(string, long)> Read(TestOutputTopic<string, string> output)
            => output.ReadKeyValueList().Select(r => (r.Message.Value, r.Message.Timestamp.UnixTimestampMs)).ToList();

        [Test]
        public void StreamRecordsAreJoinedWithTheTableValueValidAtTheirTimestamp()
        {
            var (driver, table, stream, output) = Create(false, Grace);
            using (driver)
            {
                table.PipeInput("k", "t1", At(1_000));
                stream.PipeInput("k", "s1", At(2_000)); // stream time 2 000, deadline 1 000: buffered
                table.PipeInput("k", "t2", At(3_000)); // later table version, arrives before s1 is joined
                Assert.AreEqual(0, Read(output).Count);

                stream.PipeInput("k", "s2", At(3_500)); // deadline 2 500: s1 (2 000) is joined as of 2 000 => t1
                CollectionAssert.AreEqual(new[] {("s1-t1", 2_000L)}, Read(output));

                stream.PipeInput("k", "s3", At(2_800)); // out-of-order, 2 800 >= deadline 2 500: buffered
                Assert.AreEqual(0, Read(output).Count);

                // deadline 4 000: s3 (2 800, as of 2 800 => t1) then s2 (3 500 => t2), in timestamp order. s4 waits.
                stream.PipeInput("k", "s4", At(5_000));
                CollectionAssert.AreEqual(new[] {("s3-t1", 2_800L), ("s2-t2", 3_500L)}, Read(output));
            }
        }

        [Test]
        public void RecordIsEmittedOnceTheStreamTimePassesItsTimestampPlusTheGracePeriod()
        {
            var (driver, table, stream, output) = Create(false, Grace);
            using (driver)
            {
                table.PipeInput("k", "t1", At(1_000));
                stream.PipeInput("k", "s1", At(2_000));
                stream.PipeInput("k", "s2", At(2_999)); // deadline 1 999 < 2 000
                Assert.AreEqual(0, Read(output).Count);

                stream.PipeInput("k", "s3", At(3_000)); // deadline 2 000: s1 is due
                CollectionAssert.AreEqual(new[] {("s1-t1", 2_000L)}, Read(output));
            }
        }

        [Test]
        public void RecordOlderThanTheGracePeriodIsJoinedRightAway()
        {
            var (driver, table, stream, output) = Create(false, Grace);
            using (driver)
            {
                table.PipeInput("k", "t1", At(1_000));
                table.PipeInput("k", "t2", At(4_000));
                stream.PipeInput("k", "s1", At(5_000)); // buffered until 6 000
                stream.PipeInput("k", "s2", At(1_500)); // 1 500 < deadline 4 000: joined now, as of 1 500 => t1

                CollectionAssert.AreEqual(new[] {("s2-t1", 1_500L)}, Read(output));
            }
        }

        [Test]
        public void LeftJoinWithoutTableValueAtTheRecordTimestamp()
        {
            var (driver, table, stream, output) = Create(true, Grace);
            using (driver)
            {
                table.PipeInput("k", "t1", At(3_000));
                stream.PipeInput("k", "s1", At(2_000)); // no table value as of 2 000
                stream.PipeInput("k", "s2", At(4_000)); // deadline 3 000: s1 emitted, s2 waits

                CollectionAssert.AreEqual(new[] {("s1-null", 2_000L)}, Read(output));
            }
        }

        [Test]
        public void InnerJoinDropsRecordWithoutTableValueAtItsTimestamp()
        {
            var (driver, table, stream, output) = Create(false, Grace);
            using (driver)
            {
                table.PipeInput("k", "t1", At(3_000));
                stream.PipeInput("k", "s1", At(2_000));
                stream.PipeInput("k", "s2", At(5_000)); // deadline 4 000: s1 has no table value as of 2 000

                Assert.AreEqual(0, Read(output).Count);
            }
        }

        [Test]
        public void JoinedRecordKeepsTheHeadersOfTheBufferedRecord()
        {
            var (driver, table, stream, output) = Create(false, Grace);
            using (driver)
            {
                table.PipeInput("k", "t1", At(1_000));
                var headers = new Headers {{"origin", Encoding.UTF8.GetBytes("s1")}};
                stream.PipeInput("k", "s1", At(2_000), headers);
                stream.PipeInput("k", "s2", At(4_000));

                var record = output.ReadKeyValue();
                Assert.AreEqual("s1-t1", record.Message.Value);
                Assert.AreEqual(2_000L, record.Message.Timestamp.UnixTimestampMs);
                Assert.AreEqual("s1", Encoding.UTF8.GetString(record.Message.Headers.GetLastBytes("origin")));
            }
        }

        [Test]
        public void GracePeriodNeedsAVersionedTable()
        {
            var builder = new StreamBuilder();
            var table = builder.Table("table", InMemory.As<string, string>("table-store"));
            var stream = builder.Stream<string, string>("stream");

            Assert.Throws<TopologyException>(() => stream.Join(table, (s, t) => $"{s}-{t}", Props(Grace)));
        }

        [Test]
        public void GracePeriodCantBeLargerThanTheHistoryRetention()
        {
            var builder = new StreamBuilder();
            var table = builder.Table("table", InMemoryVersioned.As<string, string>("table-store", HistoryRetention));
            var stream = builder.Stream<string, string>("stream");

            Assert.Throws<TopologyException>(() =>
                stream.Join(table, (s, t) => $"{s}-{t}", Props(TimeSpan.FromSeconds(20))));
        }

        [Test]
        public void NegativeGracePeriodIsRejected()
        {
            Assert.Throws<ArgumentException>(() => Props(TimeSpan.FromSeconds(-1)));
        }

        [Test]
        public void ZeroGracePeriodJoinsRightAwayWithANonVersionedTable()
        {
            var (driver, table, stream, output) = Create(false, TimeSpan.Zero, versioned: false);
            using (driver)
            {
                table.PipeInput("k", "t1", At(1_000));
                table.PipeInput("k", "t2", At(3_000));
                stream.PipeInput("k", "s1", At(2_000)); // no buffer, latest table value

                CollectionAssert.AreEqual(new[] {("s1-t2", 2_000L)}, Read(output));
            }
        }

        [Test]
        public void BufferStoreIsPartOfTheTopology()
        {
            var builder = new StreamBuilder();
            var table = builder.Table("table", InMemoryVersioned.As<string, string>("table-store", HistoryRetention));
            builder.Stream<string, string>("stream")
                .Join(table, (s, t) => $"{s}-{t}", Props(Grace), "join")
                .To("output");

            var description = builder.Build().Describe().ToString();
            StringAssert.Contains("join-buffer", description);
        }
    }
}
