using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using NUnit.Framework;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.Mock.Kafka;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Table;
using Streamiz.Kafka.Net.Table.Internal.ForeignKeyJoin;

namespace Streamiz.Kafka.Net.Tests.Processors
{
    /// <summary>
    /// Orders (left table, "orderId" -> "productId:payload") joined with products (right table, "productId" -> name)
    /// on the product id. The joiner returns "order+product".
    /// </summary>
    public class KTableKTableForeignKeyJoinTests
    {
        private const string ApplicationId = "test-fk-join";

        private static string ForeignKey(string order)
        {
            var fk = order.Split(':')[0];
            return string.IsNullOrEmpty(fk) ? null : fk;
        }

        private static string Joiner(string order, string product) => $"{order}+{product ?? "null"}";

        private static StreamBuilder Topology(bool leftJoin, Materialized<string, string, IKeyValueStore<Bytes, byte[]>> materialized = null, string named = null)
        {
            var builder = new StreamBuilder();
            var products = builder.Table<string, string>("products");
            var orders = builder.Table<string, string>("orders");

            var joined = leftJoin
                ? orders.LeftJoin(products, ForeignKey, Joiner, materialized, named)
                : orders.Join(products, ForeignKey, Joiner, materialized, named);

            joined.ToStream().To("output");
            return builder;
        }

        private class Fixture : IDisposable
        {
            public TopologyTestDriver Driver { get; }
            public TestInputTopic<string, string> Orders { get; }
            public TestInputTopic<string, string> Products { get; }
            public TestOutputTopic<string, string> Output { get; }

            public Fixture(StreamBuilder builder)
            {
                var config = new StreamConfig<StringSerDes, StringSerDes> {ApplicationId = ApplicationId};
                Driver = new TopologyTestDriver(builder.Build(), config);
                Orders = Driver.CreateInputTopic<string, string>("orders");
                Products = Driver.CreateInputTopic<string, string>("products");
                Output = Driver.CreateOutputTopic<string, string>("output");
            }

            public List<(string, string)> Read()
                => Output.ReadKeyValueList().Select(r => (r.Message.Key, r.Message.Value)).ToList();

            public void Dispose() => Driver.Dispose();
        }

        [Test]
        public void InnerJoinWhenTheForeignRecordExists()
        {
            using var f = new Fixture(Topology(false));
            f.Products.PipeInput("p1", "P1");
            f.Orders.PipeInput("o1", "p1:a");

            CollectionAssert.AreEqual(new[] {("o1", "p1:a+P1")}, f.Read());
        }

        [Test]
        public void InnerJoinWhenTheForeignRecordArrivesAfter()
        {
            using var f = new Fixture(Topology(false));
            f.Orders.PipeInput("o1", "p1:a"); // no product yet: no result
            Assert.IsEmpty(f.Read());

            f.Products.PipeInput("p1", "P1"); // the subscription of o1 is found by the prefix scan
            CollectionAssert.AreEqual(new[] {("o1", "p1:a+P1")}, f.Read());
        }

        [Test]
        public void ForeignUpdatePropagatesToAllTheSubscribedRecords()
        {
            using var f = new Fixture(Topology(false));
            f.Products.PipeInput("p1", "P1");
            f.Products.PipeInput("p2", "P2");
            f.Orders.PipeInput("o1", "p1:a");
            f.Orders.PipeInput("o2", "p1:b");
            f.Orders.PipeInput("o3", "p2:c");
            CollectionAssert.AreEqual(new[] {("o1", "p1:a+P1"), ("o2", "p1:b+P1"), ("o3", "p2:c+P2")}, f.Read());

            // only the orders of p1, in primary key order (prefix scan of the subscription store)
            f.Products.PipeInput("p1", "P1v2");
            CollectionAssert.AreEqual(new[] {("o1", "p1:a+P1v2"), ("o2", "p1:b+P1v2")}, f.Read());
        }

        [Test]
        public void ForeignKeyChangeUnsubscribesTheOldForeignKey()
        {
            using var f = new Fixture(Topology(false));
            f.Products.PipeInput("p1", "P1");
            f.Products.PipeInput("p2", "P2");
            f.Orders.PipeInput("o1", "p1:a");
            CollectionAssert.AreEqual(new[] {("o1", "p1:a+P1")}, f.Read());

            // DELETE_KEY_NO_PROPAGATE to p1, PROPAGATE_NULL_IF_NO_FK_VAL_AVAILABLE to p2
            f.Orders.PipeInput("o1", "p2:a");
            CollectionAssert.AreEqual(new[] {("o1", "p2:a+P2")}, f.Read());

            // o1 doesn't refer to p1 anymore
            f.Products.PipeInput("p1", "P1v2");
            Assert.IsEmpty(f.Read());

            f.Products.PipeInput("p2", "P2v2");
            CollectionAssert.AreEqual(new[] {("o1", "p2:a+P2v2")}, f.Read());
        }

        [Test]
        public void InnerJoinForeignKeyChangeToAMissingForeignRecordDeletesTheResult()
        {
            using var f = new Fixture(Topology(false));
            f.Products.PipeInput("p1", "P1");
            f.Orders.PipeInput("o1", "p1:a");
            f.Orders.PipeInput("o1", "p3:a"); // p3 doesn't exist: the previous result is "unset"

            CollectionAssert.AreEqual(new[] {("o1", "p1:a+P1"), ("o1", (string) null)}, f.Read());
        }

        [Test]
        public void LeftDeleteDeletesTheResult()
        {
            using var f = new Fixture(Topology(false));
            f.Products.PipeInput("p1", "P1");
            f.Orders.PipeInput("o1", "p1:a");
            f.Orders.PipeInput("o1", null); // DELETE_KEY_AND_PROPAGATE

            CollectionAssert.AreEqual(new[] {("o1", "p1:a+P1"), ("o1", (string) null)}, f.Read());

            // the subscription is removed
            f.Products.PipeInput("p1", "P1v2");
            Assert.IsEmpty(f.Read());
        }

        [Test]
        public void InnerJoinForeignDeleteDeletesTheResult()
        {
            using var f = new Fixture(Topology(false));
            f.Products.PipeInput("p1", "P1");
            f.Orders.PipeInput("o1", "p1:a");
            f.Products.PipeInput("p1", null);

            CollectionAssert.AreEqual(new[] {("o1", "p1:a+P1"), ("o1", (string) null)}, f.Read());
        }

        [Test]
        public void InnerJoinSkipsNullForeignKey()
        {
            using var f = new Fixture(Topology(false));
            f.Products.PipeInput("p1", "P1");
            f.Orders.PipeInput("o1", ":a");

            Assert.IsEmpty(f.Read());
        }

        [Test]
        public void LeftJoinWithoutForeignRecord()
        {
            using var f = new Fixture(Topology(true));
            f.Orders.PipeInput("o1", "p9:a"); // PROPAGATE_NULL_IF_NO_FK_VAL_AVAILABLE: joiner(value, null)
            CollectionAssert.AreEqual(new[] {("o1", "p9:a+null")}, f.Read());

            f.Products.PipeInput("p9", "P9");
            CollectionAssert.AreEqual(new[] {("o1", "p9:a+P9")}, f.Read());
        }

        [Test]
        public void LeftJoinForeignDeleteCallsTheJoinerWithNull()
        {
            using var f = new Fixture(Topology(true));
            f.Products.PipeInput("p1", "P1");
            f.Orders.PipeInput("o1", "p1:a");
            f.Products.PipeInput("p1", null);

            CollectionAssert.AreEqual(new[] {("o1", "p1:a+P1"), ("o1", "p1:a+null")}, f.Read());
        }

        [Test]
        public void LeftJoinWithNullForeignKey()
        {
            using var f = new Fixture(Topology(true));
            f.Orders.PipeInput("o1", ":a");

            CollectionAssert.AreEqual(new[] {("o1", ":a+null")}, f.Read());
        }

        [Test]
        public void LeftJoinLeftDeleteDeletesTheResult()
        {
            using var f = new Fixture(Topology(true));
            f.Products.PipeInput("p1", "P1");
            f.Orders.PipeInput("o1", "p1:a");
            f.Orders.PipeInput("o1", null);

            CollectionAssert.AreEqual(new[] {("o1", "p1:a+P1"), ("o1", (string) null)}, f.Read());
        }

        [Test]
        public void MaterializedResultCanBeQueried()
        {
            using var f = new Fixture(Topology(false, InMemory.As<string, string>("fk-result")));
            f.Products.PipeInput("p1", "P1");
            f.Orders.PipeInput("o1", "p1:a");
            f.Orders.PipeInput("o2", "p1:b");

            var store = f.Driver.GetKeyValueStore<string, string>("fk-result");
            Assert.AreEqual("p1:a+P1", store.Get("o1"));
            Assert.AreEqual("p1:b+P1", store.Get("o2"));

            f.Orders.PipeInput("o1", null);
            Assert.IsNull(store.Get("o1"));
        }

        [Test]
        public void ResultTableCanBeJoinedAgain()
        {
            // the value getter of the result table forces its materialization
            var builder = new StreamBuilder();
            var products = builder.Table<string, string>("products");
            var orders = builder.Table<string, string>("orders");
            var extras = builder.Table<string, string>("extras");
            orders.Join(products, ForeignKey, Joiner)
                .Join(extras, (v, e) => $"{v}+{e}")
                .ToStream()
                .To("output");

            using var f = new Fixture(builder);
            var extrasInput = f.Driver.CreateInputTopic<string, string>("extras");
            f.Products.PipeInput("p1", "P1");
            f.Orders.PipeInput("o1", "p1:a");
            extrasInput.PipeInput("o1", "E1");

            CollectionAssert.AreEqual(new[] {("o1", "p1:a+P1+E1")}, f.Read());
        }

        [Test]
        public void StaleResponseIsDropped()
        {
            // a response whose hash doesn't match the current left value is stale (the left value changed in the meantime)
            using var f = new Fixture(Topology(false, null, "fk"));
            f.Products.PipeInput("p1", "P1");
            f.Orders.PipeInput("o1", "p1:a");
            CollectionAssert.AreEqual(new[] {("o1", "p1:a+P1")}, f.Read());

            var responseSerdes = new SubscriptionResponseWrapperSerDes<string>("fv", new StringSerDes());
            var responses = f.Driver.CreateInputTopic($"{ApplicationId}-fk-subscription-response-topic",
                new StringSerDes(), responseSerdes);

            var staleHash = Murmur3.Hash128(Encoding.UTF8.GetBytes("p1:old"));
            responses.PipeInput("o1", new SubscriptionResponseWrapper<string>(staleHash, "P-stale", 0));
            Assert.IsEmpty(f.Read());

            var currentHash = Murmur3.Hash128(Encoding.UTF8.GetBytes("p1:a"));
            responses.PipeInput("o1", new SubscriptionResponseWrapper<string>(currentHash, "P-current", 0));
            CollectionAssert.AreEqual(new[] {("o1", "p1:a+P-current")}, f.Read());
        }

        [Test]
        public void TopologyContainsTheInternalTopicsAndTheSubscriptionStore()
        {
            var description = Topology(false, null, "fk").Build().Describe().ToString();

            StringAssert.Contains("fk-subscription-registration-topic", description);
            StringAssert.Contains("fk-subscription-response-topic", description);
            StringAssert.Contains("fk-subscription-store", description);
            StringAssert.Contains("fk-result", description);

            var generated = Topology(false).Build().Describe().ToString();
            StringAssert.Contains("KTABLE-FK-JOIN-SUBSCRIPTION-REGISTRATION-", generated);
            StringAssert.Contains("KTABLE-FK-JOIN-SUBSCRIPTION-RESPONSE-", generated);
        }

        [Test]
        public void ForeignKeyJoinWithSeveralPartitions()
        {
            // orders have 3 partitions and products 2: the registration topic must be co-partitioned with products
            // and the response topic with orders, and every response must come back to the partition of its order
            var supplier = new MockKafkaSupplier(1, 0);
            var config = new StreamConfig<StringSerDes, StringSerDes> {ApplicationId = ApplicationId};
            supplier.GetAdmin(config.ToAdminConfig("admin")).CreateTopicsAsync(new[]
            {
                new TopicSpecification {Name = "orders", NumPartitions = 3},
                new TopicSpecification {Name = "products", NumPartitions = 2},
                new TopicSpecification {Name = "output", NumPartitions = 1}
            }).GetAwaiter().GetResult();

            using var driver = new TopologyTestDriver(Topology(false, null, "fk").Build(), config, supplier);
            var orders = driver.CreateInputTopic<string, string>("orders");
            var products = driver.CreateInputTopic<string, string>("products");
            var output = driver.CreateOutputTopic<string, string>("output");

            for (int p = 0; p < 4; ++p)
                products.PipeInput($"p{p}", $"P{p}");
            for (int o = 0; o < 12; ++o)
                orders.PipeInput($"o{o}", $"p{o % 4}:x");

            var results = IntegrationTestUtils.WaitUntilMinKeyValueRecordsReceived(output, 12, TimeSpan.FromSeconds(30))
                .ToDictionary(r => r.Message.Key, r => r.Message.Value);
            Assert.AreEqual(12, results.Count);
            for (int o = 0; o < 12; ++o)
                Assert.AreEqual($"p{o % 4}:x+P{o % 4}", results[$"o{o}"]);

            var admin = supplier.GetAdmin(config.ToAdminConfig("admin-check"));
            int Partitions(string topic)
                => admin.GetMetadata(topic, TimeSpan.FromSeconds(1)).Topics.Single().Partitions.Count;
            Assert.AreEqual(2, Partitions($"{ApplicationId}-fk-subscription-registration-topic"));
            Assert.AreEqual(3, Partitions($"{ApplicationId}-fk-subscription-response-topic"));
        }
    }
}
