using Confluent.Kafka;
using NUnit.Framework;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Processors.Internal;
using System;
using System.Collections.Generic;


namespace Streamiz.Kafka.Net.Tests.Private
{
    public class ProcessorStateManagerTests
    {
        private class TestStateStore : IStateStore
        {
            public TestStateStore(string name)
            {
                Name = name;
            }

            public string Name { get; }

            public bool Persistent => false;
            public bool IsLocally => true;

            public bool IsOpen => true;

            public void Close()
            {
            }

            public void Flush()
            {
            }

            public void Init(ProcessorContext context, IStateStore root)
            {
            }
        }

        [Test]
        public void RegisterStateStoreAlreadyRegistered()
        {
            TaskId id = new TaskId {Id = 1, Partition = 0};
            TopicPartition partition = new TopicPartition("topic", 0);
            ProcessorStateManager stateMgt =
                new ProcessorStateManager(id, new List<TopicPartition> {partition}, null, null, null);

            stateMgt.Register(new TestStateStore("state-store1"), null);
            Assert.Throws<ArgumentException>(() => stateMgt.Register(new TestStateStore("state-store1"), null));
        }

        [Test]
        public void RegisterStateStoreAndGetIt()
        {
            TaskId id = new TaskId {Id = 1, Partition = 0};
            TopicPartition partition = new TopicPartition("topic", 0);
            ProcessorStateManager stateMgt =
                new ProcessorStateManager(id, new List<TopicPartition> {partition}, null, null, null);

            stateMgt.Register(new TestStateStore("state-store1"), null);
            var store = stateMgt.GetStore("state-store1");
            Assert.IsNotNull(store);
            Assert.AreEqual("state-store1", store.Name);
        }

        [Test]
        public void RegisterStateStoreAndGetUnknown()
        {
            TaskId id = new TaskId {Id = 1, Partition = 0};
            TopicPartition partition = new TopicPartition("topic", 0);
            ProcessorStateManager stateMgt =
                new ProcessorStateManager(id, new List<TopicPartition> {partition}, null, null, null);

            stateMgt.Register(new TestStateStore("state-store1"), null);
            var store = stateMgt.GetStore("sdfdfre1");
            Assert.IsNull(store);
        }

        [Test]
        public void GetChangelogTopicPartition()
        {
            TaskId id = new TaskId {Id = 0, Partition = 2};
            TopicPartition partition = new TopicPartition("topic", 0);
            ProcessorStateManager stateMgt = new ProcessorStateManager(
                id,
                new List<TopicPartition> {partition},
                new Dictionary<string, string> {{"state-store1", "state-store1-cl"}},
                new MockChangelogRegister(),
                new MockOffsetCheckpointManager());

            stateMgt.Register(new TestStateStore("state-store1"), null);
            var tp = stateMgt.GetRegisteredChangelogPartitionFor("state-store1");

            Assert.AreEqual(2, tp.Partition.Value);
            Assert.AreEqual("state-store1-cl", tp.Topic);
        }

        private static ProcessorStateManager CreateStateManagerWithChangelogs(params string[] stores)
        {
            var changelogs = new Dictionary<string, string>();
            foreach (var s in stores)
                changelogs.Add(s, $"{s}-cl");

            return new ProcessorStateManager(
                new TaskId {Id = 0, Partition = 0},
                new List<TopicPartition> {new("topic", 0)},
                changelogs,
                new MockChangelogRegister(),
                new MockOffsetCheckpointManager());
        }

        private static List<ConsumeResult<byte[], byte[]>> ChangelogRecords(string topic, params (string key, string value)[] records)
        {
            var list = new List<ConsumeResult<byte[], byte[]>>();
            long offset = 0;
            foreach (var r in records)
            {
                list.Add(new ConsumeResult<byte[], byte[]>
                {
                    TopicPartitionOffset = new TopicPartitionOffset(topic, 0, offset++),
                    Message = new Message<byte[], byte[]>
                    {
                        Key = System.Text.Encoding.UTF8.GetBytes(r.key),
                        Value = r.value == null ? null : System.Text.Encoding.UTF8.GetBytes(r.value),
                        Timestamp = new Timestamp(offset, TimestampType.CreateTime)
                    }
                });
            }
            return list;
        }

        [Test]
        public void RestoreWithBatchCallbackCallsItOncePerBatchWithAllRecords()
        {
            var stateMgt = CreateStateManagerWithChangelogs("store");
            int perRecordCalls = 0;
            var batches = new List<IReadOnlyList<ConsumeResult<byte[], byte[]>>>();
            stateMgt.Register(new TestStateStore("store"), _ => ++perRecordCalls, batches.Add);

            var metadata = stateMgt.GetStoreMetadata(new TopicPartition("store-cl", 0));
            stateMgt.Restore(metadata, ChangelogRecords("store-cl", ("k1", "v1"), ("k2", "v2"), ("k1", null)));

            Assert.AreEqual(0, perRecordCalls);
            Assert.AreEqual(1, batches.Count);
            Assert.AreEqual(3, batches[0].Count);
            Assert.AreEqual("k1", System.Text.Encoding.UTF8.GetString(batches[0][0].Message.Key));
            Assert.AreEqual("k2", System.Text.Encoding.UTF8.GetString(batches[0][1].Message.Key));
            Assert.AreEqual("k1", System.Text.Encoding.UTF8.GetString(batches[0][2].Message.Key));
            Assert.IsNull(batches[0][2].Message.Value);
            Assert.AreEqual(2, metadata.Offset);
        }

        [Test]
        public void RestoreWithoutBatchCallbackUsesPerRecordCallback()
        {
            var stateMgt = CreateStateManagerWithChangelogs("store");
            var restored = new List<string>();
            stateMgt.Register(new TestStateStore("store"),
                r => restored.Add(System.Text.Encoding.UTF8.GetString(r.Message.Key)));

            var metadata = stateMgt.GetStoreMetadata(new TopicPartition("store-cl", 0));
            stateMgt.Restore(metadata, ChangelogRecords("store-cl", ("k1", "v1"), ("k2", null)));

            CollectionAssert.AreEqual(new[] {"k1", "k2"}, restored);
            Assert.AreEqual(1, metadata.Offset);
        }

        [Test]
        public void RestoreMixedStoresEachUsesItsOwnCallback()
        {
            var stateMgt = CreateStateManagerWithChangelogs("batch-store", "record-store");
            int perRecordCallsOnBatchStore = 0, perRecordCalls = 0, batchCalls = 0;
            stateMgt.Register(new TestStateStore("batch-store"), _ => ++perRecordCallsOnBatchStore, _ => ++batchCalls);
            stateMgt.Register(new TestStateStore("record-store"), _ => ++perRecordCalls);

            stateMgt.Restore(stateMgt.GetStoreMetadata(new TopicPartition("batch-store-cl", 0)),
                ChangelogRecords("batch-store-cl", ("k1", "v1"), ("k2", "v2")));
            stateMgt.Restore(stateMgt.GetStoreMetadata(new TopicPartition("record-store-cl", 0)),
                ChangelogRecords("record-store-cl", ("k1", "v1"), ("k2", "v2")));

            Assert.AreEqual(1, batchCalls);
            Assert.AreEqual(0, perRecordCallsOnBatchStore);
            Assert.AreEqual(2, perRecordCalls);
        }
    }
}