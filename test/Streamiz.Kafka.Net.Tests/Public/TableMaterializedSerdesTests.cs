using NUnit.Framework;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Kafka.Net.Tests.Public
{
    /// <summary>
    /// Table(topic, materialized) and GlobalTable(topic, materialized) read the topic with the serdes of
    /// the Materialized when no serdes are given explicitly, like Kafka Streams.
    /// </summary>
    public class TableMaterializedSerdesTests
    {
        private static StreamConfig<StringSerDes, StringSerDes> Config()
            => new() {ApplicationId = "test-table-materialized-serdes"};

        [Test]
        public void TableReadsTheTopicWithTheMaterializedSerdes()
        {
            var builder = new StreamBuilder();
            builder.Table("balances",
                InMemory.As<string, long>("balances-store")
                    .WithKeySerdes(new StringSerDes())
                    .WithValueSerdes(new Int64SerDes()));

            using var driver = new TopologyTestDriver(builder.Build(), Config());
            var input = driver.CreateInputTopic("balances", new StringSerDes(), new Int64SerDes());
            input.PipeInput("alice", 120L);

            var store = driver.GetKeyValueStore<string, long>("balances-store");
            Assert.AreEqual(120L, store.Get("alice"));
        }

        [Test]
        public void GlobalTableReadsTheTopicWithTheMaterializedSerdes()
        {
            var builder = new StreamBuilder();
            builder.GlobalTable("rates",
                InMemory.As<string, long>("rates-store")
                    .WithKeySerdes(new StringSerDes())
                    .WithValueSerdes(new Int64SerDes()));

            using var driver = new TopologyTestDriver(builder.Build(), Config());
            var input = driver.CreateInputTopic("rates", new StringSerDes(), new Int64SerDes());
            input.PipeInput("EUR", 108L);

            var store = driver.GetKeyValueStore<string, long>("rates-store");
            Assert.AreEqual(108L, store.Get("EUR"));
        }
    }
}
