using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Moq;
using NUnit.Framework;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Metrics;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.Mock.Sync;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;
using Streamiz.Kafka.Net.Table.Internal;
using Streamiz.Kafka.Net.Tests.Helpers;

namespace Streamiz.Kafka.Net.Tests.Processors
{
    /// <summary>
    /// Source KTables materialized as versioned stores (KIP-889 / KIP-914).
    /// History retention = 10 s.
    /// </summary>
    public class KTableVersionedSourceTests
    {
        private static readonly TimeSpan HistoryRetention = TimeSpan.FromSeconds(10);
        private static readonly DateTime Epoch = DateTimeOffset.FromUnixTimeMilliseconds(0).UtcDateTime;

        private static DateTime At(long ms) => Epoch.AddMilliseconds(ms);

        private static StreamConfig<StringSerDes, StringSerDes> Config()
            => new() {ApplicationId = $"test-versioned-ktable-{Guid.NewGuid()}"};

        private static Materialized<string, string, IKeyValueStore<Bytes, byte[]>> Versioned(bool rocksDb)
            => rocksDb
                ? RocksDbVersioned.As<string, string>("table-store", HistoryRetention)
                : InMemoryVersioned.As<string, string>("table-store", HistoryRetention);

        [TestCase(false)]
        [TestCase(true)]
        public void OutOfOrderRecordsAreWrittenInTheHistoryAndForwardedAsNotLatest(bool rocksDb)
        {
            var config = Config();
            if (rocksDb)
                config.UseRandomRocksDbConfigForTest();

            var builder = new StreamBuilder();
            builder.Table("input", Versioned(rocksDb)).ToStream().To("output");

            try
            {
                using var driver = new TopologyTestDriver(builder.Build(), config);
                var input = driver.CreateInputTopic<string, string>("input");
                var output = driver.CreateOutputTopic<string, string>("output");

                input.PipeInput("k", "v1", At(1_000));
                input.PipeInput("k", "v3", At(3_000));
                input.PipeInput("k", "v2", At(2_000)); // out-of-order: written in the history, forwarded with IsLatest = false
                input.PipeInput("k2", "x", At(500));   // another key, older than stream time but within the history retention

                // KIP-914: ToStream forwards the out-of-order record too, with its own timestamp
                var records = output.ReadKeyValueList().ToList();
                CollectionAssert.AreEqual(new[] {("k", "v1"), ("k", "v3"), ("k", "v2"), ("k2", "x")},
                    records.Select(r => (r.Message.Key, r.Message.Value)).ToList());
                Assert.AreEqual(2_000, records[2].Message.Timestamp.UnixTimestampMs);

                var store = driver.GetVersionedKeyValueStore<string, string>("table-store");
                Assert.IsNotNull(store);
                Assert.AreEqual("v3", store.Get("k").Value);
                Assert.AreEqual(3_000, store.Get("k").Timestamp);
                var v2 = store.Get("k", 2_500);
                Assert.AreEqual("v2", v2.Value);
                Assert.AreEqual(2_000, v2.Timestamp);
                Assert.AreEqual(3_000, v2.ValidTo);
                Assert.AreEqual("v1", store.Get("k", 1_500).Value);
                Assert.AreEqual(2_000, store.Get("k", 1_500).ValidTo);
                Assert.IsNull(store.Get("k", 999));

                // tombstone (latest): forwarded
                input.PipeInput("k", null, At(4_000));
                var tombstone = output.ReadKeyValue();
                Assert.AreEqual("k", tombstone.Message.Key);
                Assert.IsNull(tombstone.Message.Value);
                Assert.IsNull(store.Get("k"));
                Assert.AreEqual("v3", store.Get("k", 3_500).Value);

                // stream time = 20 000: a record older than 10 000 is not put, nor forwarded
                input.PipeInput("k3", "y", At(20_000));
                input.PipeInput("k", "too-old", At(9_999));
                CollectionAssert.AreEqual(new[] {("k3", "y")},
                    output.ReadKeyValueList().Select(r => (r.Message.Key, r.Message.Value)).ToList());
                Assert.IsNull(store.Get("k"));
            }
            finally
            {
                if (rocksDb)
                    config.RemoveRocksDbFolderForTest();
            }
        }

        [Test]
        public void OutOfOrderRecordsGoThroughMapValuesAndFilter()
        {
            var builder = new StreamBuilder();
            builder.Table("input", Versioned(false))
                .MapValues((v, _) => v.ToUpper())
                .Filter((k, v, _) => v != "DROP")
                .ToStream()
                .To("output");

            using var driver = new TopologyTestDriver(builder.Build(), Config());
            var input = driver.CreateInputTopic<string, string>("input");
            var output = driver.CreateOutputTopic<string, string>("output");

            input.PipeInput("k", "a", At(1_000));
            input.PipeInput("k", "b", At(3_000));
            input.PipeInput("k", "old", At(2_000)); // out-of-order: forwarded with IsLatest = false, the table still is "b"
            input.PipeInput("k", "drop", At(4_000));

            var results = output.ReadKeyValueList().Select(r => r.Message.Value).ToList();
            // "drop" is filtered out: the filter forwards a tombstone
            CollectionAssert.AreEqual(new[] {"A", "B", "OLD", null}, results);
        }

        [Test]
        public void TableTableJoinIgnoresOutOfOrderRecordsOfBothSides()
        {
            var builder = new StreamBuilder();
            var left = builder.Table("left", InMemoryVersioned.As<string, string>("left-store", HistoryRetention));
            var right = builder.Table("right", InMemoryVersioned.As<string, string>("right-store", HistoryRetention));
            left.Join(right, (l, r) => $"{l}-{r}").ToStream().To("output");

            using var driver = new TopologyTestDriver(builder.Build(), Config());
            var leftInput = driver.CreateInputTopic<string, string>("left");
            var rightInput = driver.CreateInputTopic<string, string>("right");
            var output = driver.CreateOutputTopic<string, string>("output");

            rightInput.PipeInput("k", "r1", At(1_000));
            leftInput.PipeInput("k", "a", At(1_000));   // a-r1
            rightInput.PipeInput("k", "r3", At(3_000)); // a-r3
            rightInput.PipeInput("k", "r2", At(2_000)); // right out-of-order: ignored
            leftInput.PipeInput("k", "old", At(500));   // left out-of-order: ignored

            CollectionAssert.AreEqual(new[] {"a-r1", "a-r3"},
                output.ReadKeyValueList().Select(r => r.Message.Value).ToList());
        }

        [Test]
        public void GroupByAggregationIgnoresOutOfOrderRecords()
        {
            var builder = new StreamBuilder();
            builder.Table("input", Versioned(false))
                .GroupBy((k, v, _) => KeyValuePair.Create(v, v))
                .Count(InMemory.As<string, long>("count-store"))
                .ToStream()
                .To("output", new StringSerDes(), new Int64SerDes());

            using var driver = new TopologyTestDriver(builder.Build(), Config());
            var input = driver.CreateInputTopic<string, string>("input");

            input.PipeInput("k1", "x", At(1_000)); // x = 1
            input.PipeInput("k1", "y", At(3_000)); // x = 0, y = 1
            input.PipeInput("k1", "x", At(2_000)); // out-of-order: k1 is still "y", nothing changes

            var store = driver.GetKeyValueStore<string, long>("count-store");
            Assert.AreEqual(0, store.Get("x"));
            Assert.AreEqual(1, store.Get("y"));
        }

        [Test]
        public void ForeignKeyJoinIgnoresOutOfOrderRecordsOfBothSides()
        {
            var builder = new StreamBuilder();
            var orders = builder.Table("orders", InMemoryVersioned.As<string, string>("orders-store", HistoryRetention));
            var products = builder.Table("products", InMemoryVersioned.As<string, string>("products-store", HistoryRetention));
            orders.Join(products, order => order, (order, product) => $"{order}-{product}")
                .ToStream()
                .To("output");

            using var driver = new TopologyTestDriver(builder.Build(), Config());
            var orderInput = driver.CreateInputTopic<string, string>("orders");
            var productInput = driver.CreateInputTopic<string, string>("products");
            var output = driver.CreateOutputTopic<string, string>("output");

            productInput.PipeInput("p1", "P1", At(1_000));
            orderInput.PipeInput("o1", "p1", At(1_000));     // p1-P1
            productInput.PipeInput("p1", "P1b", At(3_000));  // p1-P1b
            productInput.PipeInput("p1", "P1old", At(2_000)); // product out-of-order: ignored
            orderInput.PipeInput("o1", "p2", At(500));       // order out-of-order: ignored (o1 still joins p1)

            CollectionAssert.AreEqual(new[] {"p1-P1", "p1-P1b"},
                output.ReadKeyValueList().Select(r => r.Message.Value).ToList());
        }

        [Test]
        public void StreamTableJoinReadsTheValueAsOfTheStreamRecordTimestamp()
        {
            var builder = new StreamBuilder();
            var table = builder.Table("table", Versioned(false));
            builder.Stream<string, string>("stream")
                .Join(table, (s, t) => $"{s}-{t}")
                .To("output");

            using var driver = new TopologyTestDriver(builder.Build(), Config());
            var tableInput = driver.CreateInputTopic<string, string>("table");
            var streamInput = driver.CreateInputTopic<string, string>("stream");
            var output = driver.CreateOutputTopic<string, string>("output");

            tableInput.PipeInput("k", "t1", At(1_000));
            tableInput.PipeInput("k", "t3", At(3_000));
            tableInput.PipeInput("k", "t2", At(2_000)); // history only
            streamInput.PipeInput("k", "s", At(2_500));

            // KIP-914: with a versioned table, the join reads the table value as of the stream record's timestamp
            // (2 500 => t2, valid from 2 000 to 3 000), even without a grace period
            Assert.AreEqual("s-t2", output.ReadKeyValue().Message.Value);
        }

        [Test]
        public void ValueGetterReadsAsOfATimestamp()
        {
            var config = Config();
            var builder = new StreamBuilder();
            builder.Table("input", Versioned(false));
            var topology = builder.Build();

            using var driver = new TopologyTestDriver(topology, config);
            var input = driver.CreateInputTopic<string, string>("input");
            input.PipeInput("k", "v1", At(1_000));
            input.PipeInput("k", "v2", At(2_000));

            var getter = new KTableSourceValueGetterSupplier<string, string>("table-store").Get() as IKTableVersionedValueGetter<string, string>;
            Assert.IsNotNull(getter);

            var task = new Mock<AbstractTask>();
            var context = new Mock<ProcessorContext>();
            context.Setup(c => c.GetStateStore("table-store"))
                .Returns((IStateStore) driver.GetVersionedKeyValueStore<string, string>("table-store"));
            getter.Init(context.Object);

            Assert.IsTrue(getter.IsVersioned);
            Assert.AreEqual("v2", getter.Get("k").Value);
            Assert.AreEqual("v1", getter.Get("k", 1_500).Value);
            Assert.AreEqual(1_000, getter.Get("k", 1_500).Timestamp);
            Assert.IsNull(getter.Get("k", 500));
        }

        private class CaptureProcessor<K, V> : AbstractProcessor<K, V>
        {
            public readonly List<V> Values = new();
            public override void Process(K key, V value) => Values.Add(value);
        }

        [Test]
        public void FilterAndMapValuesKeepIsLatest()
        {
            var task = new Mock<AbstractTask>();
            task.Setup(t => t.Id).Returns(new TaskId {Id = 0, Partition = 0});
            var context = new ProcessorContext(task.Object, new StreamConfig {ApplicationId = "app"}, null,
                new StreamMetricsRegistry());
            context.SetRecordMetaData(new RecordContext(new Headers(), 0, 100, 0, "source"));

            var filter = new KTableFilterProcessor<string, string>((_, _, _) => true, false, null, false);
            var mapValues = new KTableMapValuesProcessor<string, string, string>(
                new WrappedValueMapperWithKey<string, string, string>((_, v, _) => v.ToUpper()), false, null);
            var capture = new CaptureProcessor<string, Change<string>>();
            filter.AddNextProcessor(mapValues);
            mapValues.AddNextProcessor(capture);
            filter.Init(context);

            filter.Process("k", new Change<string>(null, "a", false));
            filter.Process("k", new Change<string>(null, "b"));

            Assert.AreEqual(2, capture.Values.Count);
            Assert.AreEqual("A", capture.Values[0].NewValue);
            Assert.IsFalse(capture.Values[0].IsLatest);
            Assert.IsTrue(capture.Values[1].IsLatest);
        }

        [Test]
        public async Task QueryVersionedStoreOfARunningStream()
        {
            var config = Config();
            config.BootstrapServers = "127.0.0.1";
            config.PollMs = 10;

            var supplier = new SyncKafkaSupplier();
            var producer = supplier.GetProducer(config.ToProducerConfig());

            var builder = new StreamBuilder();
            builder.Table("input", Versioned(false));

            var stream = new KafkaStream(builder.Build(), config, supplier);
            bool running = false;
            stream.StateChanged += (_, @new) => running |= @new.Equals(KafkaStream.State.RUNNING);
            await stream.StartAsync();
            AssertExtensions.WaitUntil(() => running, TimeSpan.FromSeconds(10), TimeSpan.FromMilliseconds(50));
            Assert.IsTrue(running);

            var serdes = new StringSerDes();
            foreach (var (value, ts) in new[] {("v1", 1_000L), ("v2", 2_000L)})
                producer.Produce("input", new Message<byte[], byte[]>
                {
                    Key = serdes.Serialize("k", new SerializationContext()),
                    Value = serdes.Serialize(value, new SerializationContext()),
                    Timestamp = new Timestamp(ts, TimestampType.CreateTime)
                });

            var store = stream.Store(StoreQueryParameters.FromNameAndType("table-store",
                QueryableStoreTypes.VersionedKeyValueStore<string, string>()));
            Assert.IsNotNull(store);

            AssertExtensions.WaitUntil(() => store.Get("k")?.Value == "v2", TimeSpan.FromSeconds(5), TimeSpan.FromMilliseconds(50));
            Assert.AreEqual("v2", store.Get("k").Value);
            Assert.AreEqual("v1", store.Get("k", 1_500).Value);

            stream.Dispose();
        }
    }
}
