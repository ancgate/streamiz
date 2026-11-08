using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Streamiz.Kafka.Net.Crosscutting;

namespace Streamiz.Kafka.Net.Processors.Internal
{
    /// <summary>
    /// Placement of the standby replicas. Every instance computes it from the same inputs (the live instances of the
    /// metadata topic and their active tasks), so they agree without coordination.
    /// For each task, the instances which don't own it as active are ranked by rendezvous hashing of (task, instance), and
    /// the first <c>num.standby.replicas</c> host a standby replica. Rendezvous hashing spreads the standbys over the
    /// instances, and when an instance joins or leaves, only the standbys which ranked it first move.
    /// </summary>
    internal static class StandbyPlacement
    {
        /// <param name="tasks">the stateful tasks of the application</param>
        /// <param name="instances">live instances with their active tasks</param>
        /// <param name="numStandbyReplicas">standby replicas per task</param>
        /// <returns>standby tasks of each instance (every instance is present, maybe with no task)</returns>
        internal static IDictionary<string, ISet<TaskId>> Compute(
            IEnumerable<TaskId> tasks,
            IEnumerable<(string InstanceId, ISet<TaskId> ActiveTasks)> instances,
            int numStandbyReplicas)
        {
            var instanceList = instances
                .GroupBy(i => i.InstanceId)
                .Select(g => (g.Key, (ISet<TaskId>) new HashSet<TaskId>(g.SelectMany(i => i.ActiveTasks))))
                .ToList();
            var result = instanceList.ToDictionary(i => i.Key, _ => (ISet<TaskId>) new HashSet<TaskId>());
            if (numStandbyReplicas <= 0)
                return result;

            foreach (var task in tasks.Distinct())
            {
                var standbys = instanceList
                    .Where(i => !i.Item2.Contains(task))
                    .OrderByDescending(i => Rank(task, i.Key))
                    .ThenBy(i => i.Key, StringComparer.Ordinal)
                    .Take(numStandbyReplicas);

                foreach (var standby in standbys)
                    result[standby.Key].Add(task);
            }

            return result;
        }

        /// <summary>
        /// Rendezvous weight of the instance for the task : the same on every instance and every platform
        /// </summary>
        internal static long Rank(TaskId task, string instanceId)
            => Murmur3.Hash128(Encoding.UTF8.GetBytes($"{task.Id}-{task.Partition}|{instanceId}"))[0];
    }
}
