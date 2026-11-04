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
        public void OutOfOrderRecordsAreWrittenInTheHistoryButNotForwarded(bool rocksDb)
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
                input.PipeInput("k", "v2", At(2_000)); // out-of-order: history only
                input.PipeInput("k2", "x", At(500));   // another key, older than stream time but within the history retention

                var results = output.ReadKeyValueList().Select(r => (r.Message.Key, r.Message.Value)).ToList();
                CollectionAssert.AreEqual(new[] {("k", "v1"), ("k", "v3"), ("k2", "x")}, results);

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
        public void OldValuesAndDownstreamOperationsSeeOnlyTheLatestValues()
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
            input.PipeInput("k", "old", At(2_000)); // out-of-order: the table still is "b"
            input.PipeInput("k", "drop", At(4_000));

            var results = output.ReadKeyValueList().Select(r => r.Message.Value).ToList();
            // "drop" is filtered out: the filter forwards a tombstone
            CollectionAssert.AreEqual(new[] {"A", "B", null}, results);
        }

        [Test]
        public void StreamTableJoinReadsTheLatestValue()
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

            // without a grace period (KIP-923), the join reads the latest value of the table
            Assert.AreEqual("s-t3", output.ReadKeyValue().Message.Value);
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
