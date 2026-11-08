using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Confluent.Kafka;
using NUnit.Framework;
using Streamiz.Kafka.Net.Metrics;
using Streamiz.Kafka.Net.Mock.Kafka;
using Streamiz.Kafka.Net.Mock.Sync;
using Streamiz.Kafka.Net.Kafka.Internal;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Kafka.Net.Tests.Private
{
    public class StandbyPlacementTests
    {
        private static TaskId Task(int partition, int subTopology = 0) => new() {Id = subTopology, Partition = partition};

        private static List<TaskId> Tasks(int count) => Enumerable.Range(0, count).Select(p => Task(p)).ToList();

        private static (string, ISet<TaskId>) Instance(string id, params TaskId[] active)
            => (id, new HashSet<TaskId>(active));

        [Test]
        public void SameResultWhateverTheOrderOfTheInputs()
        {
            var tasks = Tasks(12);
            var instances = new[] {Instance("a", Task(0)), Instance("b", Task(1)), Instance("c"), Instance("d")};

            var placement = StandbyPlacement.Compute(tasks, instances, 2);
            var shuffled = StandbyPlacement.Compute(tasks.AsEnumerable().Reverse(), instances.Reverse(), 2);

            CollectionAssert.AreEquivalent(placement.Keys, shuffled.Keys);
            foreach (var instance in placement.Keys)
                CollectionAssert.AreEquivalent(placement[instance], shuffled[instance]);
        }

        [Test]
        public void NeverOnTheInstanceWhichOwnsTheActiveTask()
        {
            var tasks = Tasks(20);
            var instances = new[]
            {
                Instance("a", tasks.Where(t => t.Partition % 3 == 0).ToArray()),
                Instance("b", tasks.Where(t => t.Partition % 3 == 1).ToArray()),
                Instance("c", tasks.Where(t => t.Partition % 3 == 2).ToArray())
            };

            var placement = StandbyPlacement.Compute(tasks, instances, 1);

            foreach (var (instance, active) in instances)
                CollectionAssert.IsEmpty(placement[instance].Intersect(active), instance);
            // each task has exactly one standby
            foreach (var task in tasks)
                Assert.AreEqual(1, placement.Values.Count(s => s.Contains(task)), task.ToString());
        }

        [Test]
        public void StandbysAreSpreadOverTheInstances()
        {
            var tasks = Tasks(300);
            var instances = new[] {Instance("a"), Instance("b"), Instance("c")};

            var placement = StandbyPlacement.Compute(tasks, instances, 1);

            // about 100 each : rendezvous hashing spreads them
            foreach (var standbys in placement.Values)
                Assert.That(standbys.Count, Is.InRange(70, 130));
        }

        [Test]
        public void OnlyTheStandbysOfALeavingInstanceMove()
        {
            var tasks = Tasks(100);
            var before = StandbyPlacement.Compute(tasks, new[] {Instance("a"), Instance("b"), Instance("c"), Instance("d")}, 1);
            var after = StandbyPlacement.Compute(tasks, new[] {Instance("a"), Instance("b"), Instance("c")}, 1);

            foreach (var instance in new[] {"a", "b", "c"})
                CollectionAssert.IsSubsetOf(before[instance], after[instance], instance);
            CollectionAssert.AreEquivalent(tasks, after.Values.SelectMany(s => s));
        }

        [Test]
        public void MoreReplicasThanOtherInstances()
        {
            var tasks = Tasks(4);
            var instances = new[] {Instance("a", Task(0), Task(1)), Instance("b", Task(2), Task(3))};

            // 3 replicas wanted, only one other instance for each task
            var placement = StandbyPlacement.Compute(tasks, instances, 3);

            CollectionAssert.AreEquivalent(new[] {Task(2), Task(3)}, placement["a"]);
            CollectionAssert.AreEquivalent(new[] {Task(0), Task(1)}, placement["b"]);
        }

        [Test]
        public void NoReplica()
        {
            var placement = StandbyPlacement.Compute(Tasks(4), new[] {Instance("a"), Instance("b")}, 0);
            Assert.IsTrue(placement.Values.All(s => s.Count == 0));
        }

        [Test]
        public void OnlyTheSubTopologiesWithChangelogsAreStateful()
        {
            var builder = new StreamBuilder();
            builder.Stream<string, string>("stateless").To("output");
            builder.Table("logged", InMemory.As<string, string>("logged-store"));
            builder.Table("not-logged", InMemory.As<string, string>("not-logged-store").WithLoggingDisabled());
            var topology = builder.Build();
            topology.Builder.RewriteTopology(new StreamConfig {ApplicationId = "test-stateful"});

            var stateful = topology.Builder.StatefulSubTopologiesSourceTopics();

            Assert.AreEqual(1, stateful.Count);
            CollectionAssert.AreEquivalent(new[] {"logged"}, stateful.Values.Single());
        }
    }

    public class StandbyTasksManagerTests
    {
        private const string ApplicationId = "test-standby-manager";
        private const string Changelog = ApplicationId + "-store-changelog";
        private readonly StringSerDes serdes = new();
        private StreamConfig<StringSerDes, StringSerDes> config;
        private MockKafkaSupplier supplier;
        private Topology topology;
        private readonly HashSet<TaskId> localActive = new();
        private StandbyTasksManager manager;

        private static readonly TaskId Task0 = new() {Id = 0, Partition = 0};

        [SetUp]
        public void Init()
        {
            config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = ApplicationId,
                StateDir = Path.Combine(".", Guid.NewGuid().ToString()),
                PollMs = 10,
                CommitIntervalMs = 0
            };
            supplier = new MockKafkaSupplier(1);

            var builder = new StreamBuilder();
            builder.Table("input", RocksDb.As<string, string>("store"));
            topology = builder.Build();
            topology.Builder.RewriteTopology(config);
            topology.Builder.BuildTopology(config);

            localActive.Clear();
            manager = new StandbyTasksManager("client", config, supplier, topology.Builder, new StreamMetricsRegistry(),
                () => new HashSet<TaskId>(localActive));
            manager.UseConsumer(supplier.GetRestoreConsumer(config.ToRestoreConsumerConfig("standby")));
        }

        [TearDown]
        public void Dispose()
        {
            manager.Dispose();
            supplier.Destroy();
            if (Directory.Exists(config.StateDir))
                Directory.Delete(config.StateDir, true);
        }

        private void ProduceToChangelog(int from, int count)
        {
            var producer = supplier.GetProducer(new ProducerConfig());
            for (int i = from; i < from + count; ++i)
                producer.Produce(new TopicPartition(Changelog, 0), new Message<byte[], byte[]>
                {
                    Key = serdes.Serialize($"key{i}", new SerializationContext()),
                    Value = serdes.Serialize($"value{i}", new SerializationContext())
                });
        }

        private void Desire(params TaskId[] tasks)
            => manager.SetDesiredStandbyTasks(tasks.ToDictionary(t => t,
                t => (IList<TopicPartition>) new List<TopicPartition> {new("input", t.Partition)}));

        private string StandbyValue(string key)
        {
            var store = manager.StandbyTasks.Single().GetStore("store") as ITimestampedKeyValueStore<string, string>;
            return store?.Get(key)?.Value;
        }

        private void RunUntil(Func<bool> condition, int maxIterations = 200)
        {
            for (int i = 0; i < maxIterations && !condition(); ++i)
                manager.RunOnce();
        }

        private IDictionary<TopicPartition, long> Checkpoint()
            => new OffsetCheckpointFile(Path.Combine(config.StateDir, ApplicationId, "0-0")).Read(Task0);

        [Test]
        public void StandbyIsUpdatedContinuously()
        {
            ProduceToChangelog(0, 5);
            Desire(Task0);

            RunUntil(() => manager.StandbyTasks.Count == 1 && StandbyValue("key4") != null);
            Assert.AreEqual(1, manager.StandbyTasks.Count);
            Assert.AreEqual("value0", StandbyValue("key0"));
            Assert.AreEqual("value4", StandbyValue("key4"));

            // a standby never completes : later records are applied too
            ProduceToChangelog(5, 5);
            RunUntil(() => StandbyValue("key9") != null);
            Assert.AreEqual("value9", StandbyValue("key9"));
        }

        [Test]
        public void StandbyIsClosedWhenItIsNotPlacedHereAnymore()
        {
            ProduceToChangelog(0, 3);
            Desire(Task0);
            RunUntil(() => manager.StandbyTasks.Count == 1 && StandbyValue("key2") != null);
            var task = manager.StandbyTasks.Single();

            Desire();
            manager.RunOnce();

            CollectionAssert.IsEmpty(manager.StandbyTasks);
            Assert.IsTrue(task.IsClosed);
            // its checkpoint is written : the next standby or active task starts after the last applied record
            Assert.AreEqual(3, Checkpoint()[new TopicPartition(Changelog, 0)]);
        }

        [Test]
        public void PromotionClosesTheStandbyWithItsCheckpointAndBlocksItUntilTheActiveTaskIsClosed()
        {
            ProduceToChangelog(0, 4);
            Desire(Task0);
            RunUntil(() => manager.StandbyTasks.Count == 1 && StandbyValue("key3") != null);

            // the task becomes active here
            manager.BeforeActiveTasksCreated(new[] {Task0});
            CollectionAssert.IsEmpty(manager.StandbyTasks);
            Assert.AreEqual(4, Checkpoint()[new TopicPartition(Changelog, 0)]);

            // the placement may still list it : no standby is created while the active task runs
            for (int i = 0; i < 5; ++i)
                manager.RunOnce();
            CollectionAssert.IsEmpty(manager.StandbyTasks);

            // the active task is closed : the standby is created again and resumes from the checkpoint
            ProduceToChangelog(4, 2);
            manager.ActiveTasksClosed(new[] {Task0});
            RunUntil(() => manager.StandbyTasks.Count == 1 && StandbyValue("key5") != null);
            Assert.AreEqual("value0", StandbyValue("key0"));
            Assert.AreEqual("value5", StandbyValue("key5"));
        }

        [Test]
        public void NoStandbyForATaskActiveOnThisInstance()
        {
            ProduceToChangelog(0, 1);
            localActive.Add(Task0);
            Desire(Task0);
            for (int i = 0; i < 5; ++i)
                manager.RunOnce();
            CollectionAssert.IsEmpty(manager.StandbyTasks);

            localActive.Clear();
            RunUntil(() => manager.StandbyTasks.Count == 1);
            Assert.AreEqual(1, manager.StandbyTasks.Count);
        }
    }

    public class TaskManagerStandbyListenerTests
    {
        private class RecordingListener : IStandbyTasksListener
        {
            public readonly List<string> Events = new();
            public Func<int> ActiveTasksCount;

            public void BeforeActiveTasksCreated(IEnumerable<TaskId> taskIds)
                => Events.Add($"before-create:{string.Join(",", taskIds.OrderBy(t => t.Partition))}:active={ActiveTasksCount()}");

            public void ActiveTasksClosed(IEnumerable<TaskId> taskIds)
                => Events.Add($"closed:{string.Join(",", taskIds.OrderBy(t => t.Partition))}");
        }

        [Test]
        public void StandbysAreToldBeforeActiveTasksAreCreatedAndWhenTheyAreClosed()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes> {ApplicationId = "test-app-standby-listener"};
            var builder = new StreamBuilder();
            builder.Stream<string, string>("topic").To("topic2");
            var topology = builder.Build();

            var supplier = new SyncKafkaSupplier();
            var consumer = supplier.GetConsumer(config.ToConsumerConfig(), null);
            var restoreConsumer = supplier.GetRestoreConsumer(config.ToConsumerConfig());
            var reader = new StoreChangelogReader(config, restoreConsumer, "thread-0", new StatestoreRestoreManager(null), new StreamMetricsRegistry());
            var producer = new StreamsProducer(config, "thread-0", Guid.NewGuid(), supplier, "");
            var taskCreator = new TaskCreator(topology.Builder, config, "thread-0", supplier, reader, new StreamMetricsRegistry());
            var taskManager = new TaskManager(topology.Builder, taskCreator, supplier.GetAdmin(config.ToAdminConfig("admin")), consumer, reader, producer);

            var listener = new RecordingListener {ActiveTasksCount = () => taskManager.ActiveTasks.Count()};
            taskManager.StandbyTasks = listener;

            taskManager.CreateTasks(new List<TopicPartition> {new("topic", 0), new("topic", 1)});
            taskManager.RevokeTasks(new List<TopicPartition> {new("topic", 1)});
            taskManager.Close();

            CollectionAssert.AreEqual(new[]
            {
                // the standbys are released before the active tasks (and their stores) are created
                "before-create:0-0,0-1:active=0",
                "closed:0-1",
                "closed:0-0"
            }, listener.Events);
        }
    }
}
