using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using NUnit.Framework;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Kafka.Internal;
using Streamiz.Kafka.Net.Metrics;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.Mock.Sync;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;

namespace Streamiz.Kafka.Net.Tests.Processors
{
    /// <summary>
    /// Async processors running inside the stream task (AsyncProcessingMode.V2)
    /// </summary>
    public class AsyncProcessingV2Tests
    {
        private static StreamConfig<StringSerDes, StringSerDes> Config(Action<StreamConfig> configure = null)
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = $"test-async-v2-{Guid.NewGuid()}",
                AsyncProcessingMode = AsyncProcessingMode.V2,
                PollMs = 10,
                CommitIntervalMs = 100
            };
            configure?.Invoke(config);
            return config;
        }

        #region Topology

        [TestCase(AsyncProcessingMode.V1, true)]
        [TestCase(AsyncProcessingMode.V2, false)]
        public void RequestTopicOnlyInV1(AsyncProcessingMode mode, bool expectRequestTopic)
        {
            var builder = new StreamBuilder();
            var stream = builder.Stream<string, string>("input");
            stream.MapValuesAsync(async (record, _) => await Task.FromResult(record.Value.ToUpper()), named: "map")
                .To("output");
            stream.ForeachAsync(async (_, _) => await Task.CompletedTask, named: "foreach");

            var topology = builder.Build();
            var config = Config(c => c.AsyncProcessingMode = mode);
            topology.Builder.RewriteTopology(config);

            Assert.AreEqual(expectRequestTopic, topology.Builder.ExternalCall);
            Assert.AreEqual(expectRequestTopic ? 2 : 0, topology.Builder.GetRequestTopics().Count());
            // the response topic of MapValuesAsync is kept in both modes
            CollectionAssert.Contains(topology.Builder.GetSourceTopics().ToList(),
                $"{config.ApplicationId}-map-response");

            // in V2 the async processors are in the sub-topology of the input topic
            var taskId = topology.Builder.GetTaskIdFromPartition(new TopicPartition("input", 0));
            var processorTopology = topology.Builder.BuildTopology(taskId, config);
            Assert.AreEqual(!expectRequestTopic, processorTopology.ProcessorOperators.ContainsKey("map"));
            Assert.AreEqual(!expectRequestTopic, processorTopology.ProcessorOperators.ContainsKey("foreach"));
        }

        #endregion

        #region Task level : offsets, back-pressure, close

        private class TaskHarness
        {
            public StreamTask Task { get; }
            private readonly TopicPartition partition = new("input", 0);
            private long offset;

            public TaskHarness(Action<StreamBuilder> build, StreamConfig config)
            {
                var builder = new StreamBuilder();
                build(builder);
                var topology = builder.Build();
                topology.Builder.RewriteTopology(config);

                var id = new TaskId {Id = 0, Partition = 0};
                var supplier = new SyncKafkaSupplier();
                var producer = new StreamsProducer(config, "thread-0", Guid.NewGuid(), supplier, "log-prefix");
                Task = new StreamTask("thread-0", id, new List<TopicPartition> {partition},
                    topology.Builder.BuildTopology(id, config),
                    supplier.GetConsumer(config.ToConsumerConfig(), null),
                    config, supplier, producer, new MockChangelogRegister(), new StreamMetricsRegistry());
                Task.InitializeStateStores();
                Task.InitializeTopology();
                Task.RestorationIfNeeded();
                Task.CompleteRestoration();
            }

            public void Add(string key, string value)
            {
                var serdes = new StringSerDes();
                Task.AddRecord(new ConsumeResult<byte[], byte[]>
                {
                    Topic = partition.Topic,
                    TopicPartitionOffset = new TopicPartitionOffset(partition, offset++),
                    Message = new Message<byte[], byte[]>
                    {
                        Key = serdes.Serialize(key, new SerializationContext()),
                        Value = serdes.Serialize(value, new SerializationContext()),
                        Timestamp = new Timestamp(DateTime.Now),
                        Headers = new Headers()
                    }
                });
            }

            public int ProcessAll()
            {
                int processed = 0;
                while (Task.CanProcess(DateTime.Now.GetMilliseconds()) && Task.Process())
                    ++processed;
                return processed;
            }

            public void DrainUntil(Func<bool> condition)
            {
                var end = DateTime.Now.AddSeconds(10);
                while (!condition() && DateTime.Now < end)
                {
                    Task.DrainAsyncCompletions();
                    Thread.Sleep(10);
                }
                Task.DrainAsyncCompletions();
                Assert.IsTrue(condition(), "condition not reached");
            }

            public long CommittedOffset()
            {
                Task.RequestCommit();
                var offsets = Task.PrepareCommit().ToList();
                Assert.AreEqual(1, offsets.Count);
                return offsets[0].Offset.Value;
            }
        }

        [Test]
        public void CommitIsHeldByTheOldestRecordInFlight()
        {
            var slow = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            int completed = 0;
            var harness = new TaskHarness(builder => builder.Stream<string, string>("input")
                .ForeachAsync(async (record, _) =>
                {
                    if (record.Value == "v5")
                        await slow.Task;
                    Interlocked.Increment(ref completed);
                }), Config());

            for (int i = 0; i < 10; ++i)
                harness.Add($"k{i}", $"v{i}");

            Assert.AreEqual(10, harness.ProcessAll());
            harness.DrainUntil(() => Volatile.Read(ref completed) == 9 && !harness.Task.AsyncCoordinator.HasCompletions);

            // offsets 0..4 and 6..9 are completed, 5 is not : the commit stays at 5
            Assert.AreEqual(5, harness.CommittedOffset());

            slow.SetResult(true);
            harness.DrainUntil(() => !harness.Task.HasAsyncWork);
            Assert.AreEqual(10, harness.CommittedOffset());
        }

        [Test]
        public void BackPressureAtMaxConcurrency()
        {
            var release = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            int running = 0, maxRunning = 0;
            var harness = new TaskHarness(builder => builder.Stream<string, string>("input")
                .ForeachAsync(async (_, _) =>
                    {
                        int now = Interlocked.Increment(ref running);
                        int max;
                        while ((max = Volatile.Read(ref maxRunning)) < now &&
                               Interlocked.CompareExchange(ref maxRunning, now, max) != max) { }
                        await release.Task;
                        Interlocked.Decrement(ref running);
                    },
                    RetryPolicy.NewBuilder().MaxConcurrency(3).Build()), Config());

            for (int i = 0; i < 10; ++i)
                harness.Add($"k{i}", $"v{i}");

            // only 3 records are accepted, the other ones stay in the record queue
            Assert.AreEqual(3, harness.ProcessAll());
            Assert.IsFalse(harness.Task.CanProcess(DateTime.Now.GetMilliseconds()));
            Assert.AreEqual(7, harness.Task.Grouper.NumBuffered());
            Assert.AreEqual(0, harness.CommittedOffset());

            release.SetResult(true);
            harness.DrainUntil(() => !harness.Task.HasAsyncWork);
            Assert.AreEqual(3, harness.CommittedOffset());

            int processed = 0;
            var end = DateTime.Now.AddSeconds(10);
            while (processed < 7 && DateTime.Now < end)
            {
                processed += harness.ProcessAll();
                harness.Task.DrainAsyncCompletions();
            }

            harness.DrainUntil(() => !harness.Task.HasAsyncWork);
            Assert.AreEqual(7, processed);
            Assert.AreEqual(10, harness.CommittedOffset());
            Assert.LessOrEqual(maxRunning, 3);
        }

        [Test]
        public void CloseWaitsForTheCallsInProgress()
        {
            int completed = 0;
            var harness = new TaskHarness(builder => builder.Stream<string, string>("input")
                .ForeachAsync(async (_, _) =>
                {
                    await Task.Delay(200);
                    Interlocked.Increment(ref completed);
                }), Config());

            for (int i = 0; i < 5; ++i)
                harness.Add($"k{i}", $"v{i}");
            Assert.AreEqual(5, harness.ProcessAll());
            Assert.AreEqual(0, harness.CommittedOffset());

            // what the task manager does before committing a revoked or closed task
            harness.Task.CompleteAsyncProcessing(TimeSpan.FromSeconds(10));

            Assert.AreEqual(5, completed);
            Assert.IsFalse(harness.Task.HasAsyncWork);
            Assert.AreEqual(5, harness.CommittedOffset());
        }

        [Test]
        public void DirtyCloseIgnoresTheCallsInProgress()
        {
            var release = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            var harness = new TaskHarness(builder => builder.Stream<string, string>("input")
                .MapValuesAsync(async (record, _) =>
                {
                    await release.Task;
                    return record.Value;
                })
                .To("output"), Config());

            harness.Add("k1", "v1");
            Assert.AreEqual(1, harness.ProcessAll());

            harness.Task.CancelAsyncProcessing();
            release.SetResult(true);
            Thread.Sleep(100);

            Assert.AreEqual(0, harness.Task.DrainAsyncCompletions());
            Assert.IsFalse(harness.Task.HasAsyncWork);
        }

        #endregion

        #region Driver level : ordering, concurrency, retries, record context, throughput

        [Test]
        public void SameKeyInOrderEvenWhenALaterCallIsFaster()
        {
            var builder = new StreamBuilder();
            builder.Stream<string, string>("input")
                .MapValuesAsync(async (record, _) =>
                {
                    // a1 is slow, a2 is fast, b1 is in between
                    await Task.Delay(record.Value switch {"a1" => 300, "b1" => 100, _ => 0});
                    return record.Value.ToUpper();
                })
                .To("output");

            using var driver = new TopologyTestDriver(builder.Build(), Config());
            var input = driver.CreateInputTopic<string, string>("input");
            var output = driver.CreateOutputTopic<string, string>("output");

            input.PipeInputs(new List<KeyValuePair<string, string>>
            {
                new("A", "a1"), new("A", "a2"), new("B", "b1")
            });

            // B doesn't wait for A, and a2 waits for a1 : B1, A1, A2
            CollectionAssert.AreEqual(new[] {"B1", "A1", "A2"}, output.ReadValueList().ToList());
        }

        [Test]
        public void DifferentKeysRunConcurrentlyUpToMaxConcurrency()
        {
            int running = 0, maxRunning = 0;
            var builder = new StreamBuilder();
            builder.Stream<string, string>("input")
                .MapValuesAsync(async (record, _) =>
                    {
                        int now = Interlocked.Increment(ref running);
                        int max;
                        while ((max = Volatile.Read(ref maxRunning)) < now &&
                               Interlocked.CompareExchange(ref maxRunning, now, max) != max) { }
                        await Task.Delay(50);
                        Interlocked.Decrement(ref running);
                        return record.Value;
                    },
                    RetryPolicy.NewBuilder().MaxConcurrency(4).Build())
                .To("output");

            using var driver = new TopologyTestDriver(builder.Build(), Config());
            var input = driver.CreateInputTopic<string, string>("input");
            var output = driver.CreateOutputTopic<string, string>("output");

            input.PipeInputs(Enumerable.Range(0, 20).Select(i => new KeyValuePair<string, string>($"k{i}", $"v{i}")));

            Assert.AreEqual(20, output.ReadValueList().Count());
            Assert.LessOrEqual(maxRunning, 4);
            Assert.Greater(maxRunning, 1);
        }

        [Test]
        public void ThroughputScalesWithConcurrency()
        {
            var builder = new StreamBuilder();
            builder.Stream<string, string>("input")
                .MapValuesAsync(async (record, _) =>
                    {
                        await Task.Delay(50);
                        return record.Value;
                    },
                    RetryPolicy.NewBuilder().MaxConcurrency(16).Build())
                .To("output");

            using var driver = new TopologyTestDriver(builder.Build(), Config());
            var input = driver.CreateInputTopic<string, string>("input");
            var output = driver.CreateOutputTopic<string, string>("output");

            var watch = Stopwatch.StartNew();
            input.PipeInputs(Enumerable.Range(0, 100).Select(i => new KeyValuePair<string, string>($"k{i}", $"v{i}")));
            watch.Stop();

            Assert.AreEqual(100, output.ReadValueList().Count());
            TestContext.Progress.WriteLine($"100 calls of 50 ms with max concurrency 16 : {watch.ElapsedMilliseconds} ms");
            // one by one it would take at least 5000 ms
            Assert.Less(watch.ElapsedMilliseconds, 2500);
        }

        [Test]
        public void ResultsKeepTheRecordContext()
        {
            var builder = new StreamBuilder();
            builder.Stream<string, string>("input")
                .MapValuesAsync(async (record, _) =>
                {
                    await Task.Delay(20);
                    return record.Value.ToUpper();
                })
                .To("output");

            using var driver = new TopologyTestDriver(builder.Build(), Config());
            var input = driver.CreateInputTopic<string, string>("input");
            var output = driver.CreateOutputTopic<string, string>("output");

            var timestamp = new DateTime(2026, 1, 2, 3, 4, 5, DateTimeKind.Utc);
            input.PipeInput("k1", "v1", timestamp, new Headers {{"h", Encoding.UTF8.GetBytes("header")}});

            var record = output.ReadKeyValue();
            Assert.AreEqual("V1", record.Message.Value);
            Assert.AreEqual(timestamp.GetMilliseconds(), record.Message.Timestamp.UnixTimestampMs);
            Assert.AreEqual("header", Encoding.UTF8.GetString(record.Message.Headers.GetLastBytes("h")));
        }

        [Test]
        public void SkipAfterRetriesSendsTheRecordToTheDeadLetterQueue()
        {
            int calls = 0;
            var builder = new StreamBuilder();
            builder.Stream<string, string>("input")
                .MapValuesAsync(async (record, _) =>
                    {
                        if (record.Value == "poison")
                        {
                            Interlocked.Increment(ref calls);
                            throw new StreamsException("external service error");
                        }
                        return await Task.FromResult(record.Value.ToUpper());
                    },
                    RetryPolicy.NewBuilder()
                        .RetriableException<StreamsException>()
                        .NumberOfRetry(3)
                        .RetryBackOffMs(1)
                        .RetryBehavior(EndRetryBehavior.SKIP)
                        .Build())
                .To("output");

            using var driver = new TopologyTestDriver(builder.Build(), Config(c => c.DeadLetterQueueTopicName = "dlq"));
            var input = driver.CreateInputTopic<string, string>("input");
            var output = driver.CreateOutputTopic<string, string>("output");
            var dlq = driver.CreateOutputTopic<string, string>("dlq");

            input.PipeInput("k1", "poison");
            input.PipeInput("k2", "ok");

            Assert.AreEqual(3, calls);
            CollectionAssert.AreEqual(new[] {"OK"}, output.ReadValueList().ToList());
            var dead = dlq.ReadKeyValueList().ToList();
            Assert.AreEqual(1, dead.Count);
            Assert.AreEqual("poison", dead[0].Message.Value);
            Assert.AreEqual(typeof(NoneRetryableException).FullName,
                Encoding.UTF8.GetString(dead[0].Message.Headers.GetLastBytes(DeadLetterQueue.EXCEPTION_HEADER)));
        }

        [Test]
        public void FailAfterRetriesGoesThroughTheProcessingExceptionHandler()
        {
            ErrorHandlerContext errorContext = null;
            var builder = new StreamBuilder();
            builder.Stream<string, string>("input")
                .MapValuesAsync(async (record, _) =>
                    {
                        if (record.Value == "poison")
                            throw new StreamsException("external service error");
                        return await Task.FromResult(record.Value.ToUpper());
                    },
                    RetryPolicy.NewBuilder()
                        .RetriableException<StreamsException>()
                        .NumberOfRetry(2)
                        .RetryBackOffMs(1)
                        .RetryBehavior(EndRetryBehavior.FAIL)
                        .Build())
                .To("output");

            var config = Config(c => c.ProcessingExceptionHandler = (context, _) =>
            {
                errorContext = context;
                return ExceptionHandlerResponse.CONTINUE;
            });
            using var driver = new TopologyTestDriver(builder.Build(), config);
            var input = driver.CreateInputTopic<string, string>("input");
            var output = driver.CreateOutputTopic<string, string>("output");

            input.PipeInput("k1", "poison");
            input.PipeInput("k2", "ok");

            Assert.IsNotNull(errorContext);
            Assert.AreEqual("input", errorContext.Topic);
            Assert.AreEqual(0, errorContext.Offset);
            Assert.AreEqual("poison", Encoding.UTF8.GetString(errorContext.SourceRawValue));
            CollectionAssert.AreEqual(new[] {"OK"}, output.ReadValueList().ToList());
        }

        [Test]
        public void FailAfterRetriesWithoutHandlerRaisesTheError()
        {
            var builder = new StreamBuilder();
            builder.Stream<string, string>("input")
                .ForeachAsync(async (_, _) =>
                    {
                        await Task.Yield();
                        throw new StreamsException("external service error");
                    },
                    RetryPolicy.NewBuilder()
                        .RetriableException<StreamsException>()
                        .NumberOfRetry(2)
                        .RetryBackOffMs(1)
                        .RetryBehavior(EndRetryBehavior.FAIL)
                        .Build());

            using var driver = new TopologyTestDriver(builder.Build(), Config());
            var input = driver.CreateInputTopic<string, string>("input");

            Assert.Throws<NoneRetryableException>(() => input.PipeInput("k1", "v1"));
        }

        [Test]
        public void InClusterInMemoryDriver()
        {
            var builder = new StreamBuilder();
            builder.Stream<string, string>("input")
                .MapValuesAsync(async (record, _) =>
                {
                    await Task.Delay(record.Value == "a1" ? 200 : 0);
                    return record.Value.ToUpper();
                })
                .To("output");

            using var driver = new TopologyTestDriver(builder.Build(), Config(),
                TopologyTestDriver.Mode.ASYNC_CLUSTER_IN_MEMORY);
            var input = driver.CreateInputTopic<string, string>("input");
            var output = driver.CreateOutputTopic<string, string>("output");

            input.PipeInput("A", "a1");
            input.PipeInput("A", "a2");
            input.PipeInput("B", "b1");

            var values = IntegrationTestUtils.WaitUntilMinKeyValueRecordsReceived(output, 3)
                .Select(r => r.Message.Value).ToList();
            Assert.AreEqual(3, values.Count);
            Assert.Less(values.IndexOf("A1"), values.IndexOf("A2"));
        }

        [Test]
        public void BackPressurePausesAndResumesInClusterInMemoryDriver()
        {
            var builder = new StreamBuilder();
            builder.Stream<string, string>("input")
                .MapValuesAsync(async (record, _) =>
                    {
                        await Task.Delay(10);
                        return record.Value;
                    },
                    RetryPolicy.NewBuilder().MaxConcurrency(2).Build())
                .To("output");

            using var driver = new TopologyTestDriver(builder.Build(), Config(c => c.MaxPollRecords = 5),
                TopologyTestDriver.Mode.ASYNC_CLUSTER_IN_MEMORY);
            var input = driver.CreateInputTopic<string, string>("input");
            var output = driver.CreateOutputTopic<string, string>("output");

            // the task saturates after 2 records : its partitions are paused and resumed until every record is processed
            for (int i = 0; i < 50; ++i)
                input.PipeInput($"k{i % 5}", $"v{i}");

            var values = IntegrationTestUtils
                .WaitUntilMinKeyValueRecordsReceived(output, 50, TimeSpan.FromSeconds(30))
                .ToList();
            Assert.AreEqual(50, values.Count);
            // per key, the records are in order
            foreach (var group in values.GroupBy(r => r.Message.Key))
            {
                var numbers = group.Select(r => int.Parse(r.Message.Value.Substring(1))).ToList();
                CollectionAssert.AreEqual(numbers.OrderBy(n => n).ToList(), numbers);
            }
        }

        #endregion
    }
}
