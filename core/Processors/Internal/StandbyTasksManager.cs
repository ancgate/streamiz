using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Kafka;
using Streamiz.Kafka.Net.Metrics;

namespace Streamiz.Kafka.Net.Processors.Internal
{
    /// <summary>
    /// What the stream threads tell the standby replicas : a task becomes active here, or is not active here anymore
    /// </summary>
    internal interface IStandbyTasksListener
    {
        /// <summary>
        /// The tasks are about to become active on this instance : their standby replicas (if any) are closed cleanly first,
        /// so the active tasks open the stores and restore from the standby's checkpoint. No standby of these tasks is
        /// created until <see cref="ActiveTasksClosed"/>.
        /// </summary>
        void BeforeActiveTasksCreated(IEnumerable<TaskId> taskIds);

        /// <summary>
        /// The active tasks are closed on this instance : they can have a standby replica here again
        /// </summary>
        void ActiveTasksClosed(IEnumerable<TaskId> taskIds);
    }

    /// <summary>
    /// The standby replicas hosted by this instance (<see cref="IStreamConfig.NumStandbyReplicas"/>). A dedicated thread with
    /// its own restore consumer creates and closes the <see cref="StandbyTask"/>s chosen by the placement
    /// (<see cref="StandbyPlacement"/>, computed from the metadata topic), and keeps their stores up to date by reading
    /// their changelog partitions continuously. Being separate from the stream threads, it never slows down the processing
    /// or the restoration of the active tasks.
    /// </summary>
    internal class StandbyTasksManager : IChangelogRegister, IStandbyTasksListener, IDisposable
    {
        private static readonly TimeSpan RetryCreationAfter = TimeSpan.FromSeconds(1);

        private readonly ILogger log = Logger.GetLogger(typeof(StandbyTasksManager));
        private readonly string logPrefix;
        private readonly IStreamConfig config;
        private readonly IKafkaSupplier kafkaSupplier;
        private readonly InternalTopologyBuilder builder;
        private readonly StreamMetricsRegistry metricsRegistry;
        private readonly Func<ISet<TaskId>> localActiveTasks;
        private readonly object @lock = new();

        // guarded by the lock
        private readonly Dictionary<TaskId, StandbyTask> tasks = new();
        private readonly Dictionary<TopicPartition, StandbyTask> changelogToTask = new();
        private readonly HashSet<TaskId> claimedByActiveTasks = new();
        private readonly Dictionary<TaskId, DateTime> creationFailures = new();
        private readonly List<TopicPartitionOffset> toAssign = new();
        private readonly List<TopicPartition> toUnassign = new();
        private IDictionary<TaskId, IList<TopicPartition>> desired = new Dictionary<TaskId, IList<TopicPartition>>();
        private bool reconcileNeeded;
        private volatile IReadOnlyList<StandbyTask> snapshot = new List<StandbyTask>();

        // only used by the standby thread
        private IConsumer<byte[], byte[]> consumer;
        private readonly HashSet<TopicPartition> assignment = new();
        private Thread thread;
        private volatile bool running;
        private DateTime lastCheckpoint = DateTime.Now;
        private StandbyTask registeringTask;

        internal StandbyTasksManager(
            string clientId,
            IStreamConfig config,
            IKafkaSupplier kafkaSupplier,
            InternalTopologyBuilder builder,
            StreamMetricsRegistry metricsRegistry,
            Func<ISet<TaskId>> localActiveTasks)
        {
            this.config = config;
            this.kafkaSupplier = kafkaSupplier;
            this.builder = builder;
            this.metricsRegistry = metricsRegistry;
            this.localActiveTasks = localActiveTasks;
            ThreadName = $"{clientId}-standby-thread";
            logPrefix = $"standby-tasks[{ThreadName}] ";
        }

        internal string ThreadName { get; }

        /// <summary>
        /// Standby tasks currently hosted by this instance
        /// </summary>
        internal IReadOnlyList<StandbyTask> StandbyTasks => snapshot;

        /// <summary>
        /// Standby tasks this instance must host (from the placement) and their source partitions
        /// </summary>
        internal void SetDesiredStandbyTasks(IDictionary<TaskId, IList<TopicPartition>> standbyTasks)
        {
            lock (@lock)
            {
                if (desired.Count == standbyTasks.Count && standbyTasks.Keys.All(desired.ContainsKey))
                    return;

                log.LogInformation("{LogPrefix}Standby tasks of this instance : {Tasks}", logPrefix,
                    string.Join(",", standbyTasks.Keys.OrderBy(t => t.Id).ThenBy(t => t.Partition)));
                desired = new Dictionary<TaskId, IList<TopicPartition>>(standbyTasks);
                reconcileNeeded = true;
            }
        }

        internal IEnumerable<TaskId> DesiredStandbyTasks
        {
            get
            {
                lock (@lock)
                    return desired.Keys.ToList();
            }
        }

        #region IStandbyTasksListener

        public void BeforeActiveTasksCreated(IEnumerable<TaskId> taskIds)
        {
            lock (@lock)
            {
                foreach (var id in taskIds)
                {
                    claimedByActiveTasks.Add(id);
                    if (tasks.TryGetValue(id, out var task))
                    {
                        log.LogInformation("{LogPrefix}Task {TaskId} becomes active on this instance : its standby is closed and its state reused", logPrefix, id);
                        CloseTask(task, false);
                    }
                }
            }
        }

        public void ActiveTasksClosed(IEnumerable<TaskId> taskIds)
        {
            lock (@lock)
            {
                foreach (var id in taskIds)
                    claimedByActiveTasks.Remove(id);
                reconcileNeeded = true;
            }
        }

        #endregion

        #region IChangelogRegister

        public void Register(TopicPartition topicPartition, ProcessorStateManager processorStateManager)
        {
            // called while the stores of a standby task are opened (under the lock)
            if (registeringTask != null)
                changelogToTask[topicPartition] = registeringTask;
        }

        public void Unregister(IEnumerable<TopicPartition> topicPartitions)
        {
            foreach (var partition in topicPartitions)
            {
                changelogToTask.Remove(partition);
                toUnassign.Add(partition);
            }
        }

        #endregion

        internal void Start()
        {
            if (running)
                return;

            consumer = kafkaSupplier.GetRestoreConsumer(config.ToRestoreConsumerConfig($"{ThreadName}-restore-consumer"));
            running = true;
            thread = new Thread(Run) {Name = ThreadName, IsBackground = true};
            thread.Start();
        }

        private void Run()
        {
            while (running)
            {
                try
                {
                    RunOnce();
                }
                catch (Exception e)
                {
                    // the standby replicas are a best effort : an error never stops the thread, it retries
                    log.LogWarning(e, "{LogPrefix}Error while updating the standby replicas, retrying", logPrefix);
                    Thread.Sleep(RetryCreationAfter);
                }
            }
        }

        // internal for testing : one iteration of the standby thread
        internal void RunOnce()
        {
            lock (@lock)
            {
                Reconcile();
                UpdateAssignment();
            }

            if (assignment.Count == 0)
            {
                Thread.Sleep(TimeSpan.FromMilliseconds(Math.Max(1, config.PollMs)));
                return;
            }

            // read outside of the lock : the stream threads can close a standby task meanwhile
            var records = consumer
                .ConsumeRecords(TimeSpan.FromMilliseconds(config.PollMs), config.MaxPollRestoringRecords)
                .ToList();

            lock (@lock)
            {
                foreach (var group in records.GroupBy(r => r.TopicPartition))
                {
                    // the records of a standby closed meanwhile are dropped
                    if (changelogToTask.TryGetValue(group.Key, out var task) && !task.IsClosed)
                        task.Apply(group.Key, group.ToList());
                }

                if (DateTime.Now - lastCheckpoint >= TimeSpan.FromMilliseconds(config.CommitIntervalMs))
                {
                    foreach (var task in tasks.Values)
                        task.Checkpoint();
                    lastCheckpoint = DateTime.Now;
                }
            }
        }

        // under the lock
        private void Reconcile()
        {
            var active = localActiveTasks();
            var wanted = desired
                .Where(t => !claimedByActiveTasks.Contains(t.Key) && !active.Contains(t.Key))
                .ToDictionary(t => t.Key, t => t.Value);

            foreach (var task in tasks.Values.Where(t => !wanted.ContainsKey(t.Id)).ToList())
            {
                log.LogInformation("{LogPrefix}Closing the standby task {TaskId}, it's not placed on this instance anymore", logPrefix, task.Id);
                CloseTask(task, false);
            }

            foreach (var kv in wanted.Where(t => !tasks.ContainsKey(t.Key)))
            {
                if (creationFailures.TryGetValue(kv.Key, out var failure) && DateTime.Now - failure < RetryCreationAfter && !reconcileNeeded)
                    continue;
                CreateTask(kv.Key, kv.Value);
            }

            reconcileNeeded = false;
        }

        // under the lock
        private void CreateTask(TaskId id, IList<TopicPartition> partitions)
        {
            StandbyTask task = null;
            try
            {
                task = new StandbyTask(ThreadName, id, partitions, builder.BuildTopology(id.Id, config), config, this,
                    metricsRegistry);
                registeringTask = task;
                task.InitializeStateStores();
                registeringTask = null;

                tasks[id] = task;
                creationFailures.Remove(id);
                foreach (var changelog in task.ChangelogPartitions)
                {
                    var offset = task.NextOffset(changelog);
                    toAssign.Add(new TopicPartitionOffset(changelog, offset.HasValue ? new Offset(offset.Value) : Offset.Beginning));
                }

                snapshot = tasks.Values.ToList();
                log.LogInformation("{LogPrefix}Created the standby task {TaskId} for the changelogs {Changelogs}", logPrefix, id,
                    string.Join(",", task.ChangelogPartitions));
            }
            catch (Exception e)
            {
                // e.g. the stores of the task are still open by its active task, which is closing : retry later
                registeringTask = null;
                log.LogWarning(e, "{LogPrefix}Unable to create the standby task {TaskId}, it will be retried", logPrefix, id);
                creationFailures[id] = DateTime.Now;
                if (task != null)
                {
                    try
                    {
                        task.Close(true);
                    }
                    catch (Exception closeException)
                    {
                        log.LogDebug(closeException, "{LogPrefix}Error while closing the standby task {TaskId}", logPrefix, id);
                    }
                }
            }
        }

        // under the lock
        private void CloseTask(StandbyTask task, bool dirty)
        {
            try
            {
                task.Close(dirty);
            }
            catch (Exception e)
            {
                log.LogWarning(e, "{LogPrefix}Error while closing the standby task {TaskId}", logPrefix, task.Id);
            }

            tasks.Remove(task.Id);
            foreach (var changelog in changelogToTask.Where(kv => kv.Value == task).Select(kv => kv.Key).ToList())
            {
                changelogToTask.Remove(changelog);
                toUnassign.Add(changelog);
            }

            snapshot = tasks.Values.ToList();
        }

        // under the lock, from the standby thread (the consumer is only used by this thread)
        private void UpdateAssignment()
        {
            var unassign = toUnassign.Where(assignment.Contains).Distinct().ToList();
            if (unassign.Any())
            {
                consumer.IncrementalUnassign(unassign);
                foreach (var partition in unassign)
                    assignment.Remove(partition);
            }
            toUnassign.Clear();

            // a partition unassigned then assigned again (task recreated) starts from its new offset
            var assign = toAssign.Where(t => changelogToTask.ContainsKey(t.TopicPartition)).ToList();
            if (assign.Any())
            {
                foreach (var partition in assign.Where(t => assignment.Contains(t.TopicPartition)).ToList())
                {
                    consumer.IncrementalUnassign(new[] {partition.TopicPartition});
                    assignment.Remove(partition.TopicPartition);
                }

                consumer.IncrementalAssign(assign);
                foreach (var partition in assign)
                    assignment.Add(partition.TopicPartition);
            }
            toAssign.Clear();
        }

        public void Dispose()
        {
            if (running)
            {
                running = false;
                thread?.Join(TimeSpan.FromSeconds(30));
            }

            lock (@lock)
            {
                foreach (var task in tasks.Values.ToList())
                    CloseTask(task, false);
                toAssign.Clear();
                toUnassign.Clear();
            }

            try
            {
                consumer?.Close();
                consumer?.Dispose();
            }
            catch (Exception e)
            {
                log.LogWarning(e, "{LogPrefix}Error while closing the standby restore consumer", logPrefix);
            }

            consumer = null;
        }

        // for testing
        internal void UseConsumer(IConsumer<byte[], byte[]> restoreConsumer) => consumer = restoreConsumer;
    }
}
