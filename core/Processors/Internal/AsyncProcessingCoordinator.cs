using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Confluent.Kafka;

namespace Streamiz.Kafka.Net.Processors.Internal
{
    /// <summary>
    /// An async processor running inside a stream task (<see cref="AsyncProcessingMode.V2"/>).
    /// </summary>
    internal interface IInTaskAsyncProcessor
    {
        /// <summary>
        /// Number of records accepted and not completed yet (running, retrying or waiting behind a record with the same key).
        /// </summary>
        int Pending { get; }

        /// <summary>
        /// Maximum number of pending records
        /// </summary>
        int MaxConcurrency { get; }

        /// <summary>
        /// True when <see cref="Pending"/> reached the max concurrency: the task must not process new records.
        /// </summary>
        bool Saturated { get; }

        /// <summary>
        /// Cancel the calls in progress and forget the waiting records (dirty close).
        /// </summary>
        void Cancel();
    }

    /// <summary>
    /// The result (or the error) of one async call, handled on the stream thread.
    /// </summary>
    internal interface IAsyncCompletion
    {
        /// <summary>
        /// Source record which started the call, null if it's unknown (e.g. a record forwarded by a punctuator)
        /// </summary>
        ConsumeResult<byte[], byte[]> SourceRecord { get; }

        /// <summary>
        /// Forward the results downstream with the record context of the source record, or throw the error of the call.
        /// Always called on the stream thread.
        /// </summary>
        void Complete();
    }

    /// <summary>
    /// Coordinates the async processors of one stream task (<see cref="AsyncProcessingMode.V2"/>):
    /// - the calls run on the thread pool and their completions are queued here, the stream thread drains them and
    ///   forwards the results, so downstream processors, stores and the producer are only used by the stream thread;
    /// - the offsets of the source records whose calls are not completed are tracked, so the task never commits past them.
    /// </summary>
    internal class AsyncProcessingCoordinator
    {
        private readonly ConcurrentQueue<IAsyncCompletion> completions = new();
        private readonly SemaphoreSlim completionSignal = new(0);
        private readonly List<IInTaskAsyncProcessor> processors = new();

        // only used by the stream thread : partition -> offset -> number of calls in progress for this source record
        private readonly Dictionary<TopicPartition, SortedDictionary<long, int>> inFlightOffsets = new();

        private volatile bool cancelled;

        internal void Register(IInTaskAsyncProcessor processor) => processors.Add(processor);

        /// <summary>
        /// True if one async processor reached its max concurrency
        /// </summary>
        internal bool IsSaturated => processors.Any(p => p.Saturated);

        /// <summary>
        /// True if every async processor is at most half full : the partitions paused because of the back-pressure can be resumed
        /// </summary>
        internal bool CanResume => processors.All(p => p.Pending <= p.MaxConcurrency / 2);

        /// <summary>
        /// Number of records accepted by the async processors and not completed yet
        /// </summary>
        internal int Pending => processors.Sum(p => p.Pending);

        internal bool Cancelled => cancelled;

        #region Offsets (stream thread)

        internal void TrackSourceRecord(ConsumeResult<byte[], byte[]> sourceRecord)
        {
            if (sourceRecord == null)
                return;

            if (!inFlightOffsets.TryGetValue(sourceRecord.TopicPartition, out var offsets))
            {
                offsets = new SortedDictionary<long, int>();
                inFlightOffsets.Add(sourceRecord.TopicPartition, offsets);
            }

            long offset = sourceRecord.Offset.Value;
            offsets[offset] = offsets.TryGetValue(offset, out var count) ? count + 1 : 1;
        }

        internal void UntrackSourceRecord(ConsumeResult<byte[], byte[]> sourceRecord)
        {
            if (sourceRecord == null ||
                !inFlightOffsets.TryGetValue(sourceRecord.TopicPartition, out var offsets))
                return;

            long offset = sourceRecord.Offset.Value;
            if (!offsets.TryGetValue(offset, out var count))
                return;

            if (count <= 1)
                offsets.Remove(offset);
            else
                offsets[offset] = count - 1;

            if (offsets.Count == 0)
                inFlightOffsets.Remove(sourceRecord.TopicPartition);
        }

        /// <summary>
        /// For each partition with calls in progress, the offset of the oldest source record not completed :
        /// the task can't commit past it.
        /// </summary>
        internal IEnumerable<KeyValuePair<TopicPartition, long>> LowestInFlightOffsets()
            => inFlightOffsets
                .Where(kv => kv.Value.Count > 0)
                .Select(kv => new KeyValuePair<TopicPartition, long>(kv.Key, kv.Value.Keys.First()));

        #endregion

        #region Completions

        /// <summary>
        /// Called by the thread which completed the call
        /// </summary>
        internal void Enqueue(IAsyncCompletion completion)
        {
            if (cancelled)
                return;
            completions.Enqueue(completion);
            completionSignal.Release();
        }

        internal bool TryDequeue(out IAsyncCompletion completion)
        {
            if (cancelled)
            {
                completion = null;
                return false;
            }

            return completions.TryDequeue(out completion);
        }

        internal bool HasCompletions => !completions.IsEmpty;

        /// <summary>
        /// Wait until a completion is queued, or the timeout
        /// </summary>
        internal void WaitForCompletion(TimeSpan timeout)
        {
            if (completions.IsEmpty)
                completionSignal.Wait(timeout);
        }

        #endregion

        /// <summary>
        /// Dirty close : cancel the calls in progress, their results will be ignored
        /// </summary>
        internal void Cancel()
        {
            cancelled = true;
            foreach (var processor in processors)
                processor.Cancel();
            while (completions.TryDequeue(out _)) { }
            inFlightOffsets.Clear();
        }
    }
}
