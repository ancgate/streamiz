using System;
using System.Collections.Generic;
using Microsoft.Extensions.Logging;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table.Internal;

namespace Streamiz.Kafka.Net.Processors
{
    /// <summary>
    /// Sliding windows aggregation (KIP-450), ported from Kafka Streams' KStreamSlidingWindowAggregate.
    /// Windows are [start, start + timeDifference], both ends inclusive, and are stored by their start.
    /// For each record, the windows of its key which can contain it are fetched (forward), then:
    /// - the existing windows containing the record are updated,
    /// - the record's left window [ts - timeDifference, ts] is created if needed, with the aggregate of the previous records it contains,
    /// - the record's right window [ts + 1, ts + 1 + timeDifference] is created if later records fall into it,
    /// - the right window of the previous record is created if the record falls into it.
    /// </summary>
    internal class KStreamSlidingWindowAggregateProcessor<K, V, Agg> : AbstractProcessor<K, V>
    {
        private readonly SlidingWindowOptions windows;
        private readonly Initializer<Agg> initializer;
        private readonly Aggregator<K, V, Agg> aggregator;
        private readonly string storeName;
        private readonly bool sendOldValues;

        private long observedStreamTime = -1;
        private ITimestampedWindowStore<K, Agg> windowStore;
        private TimestampedTupleForwarder<Windowed<K>, Agg> tupleForwarder;

        public KStreamSlidingWindowAggregateProcessor(SlidingWindowOptions windows,
            Initializer<Agg> initializer,
            Aggregator<K, V, Agg> aggregator,
            string storeName,
            bool sendOldValues)
        {
            this.windows = windows;
            this.initializer = initializer;
            this.aggregator = aggregator;
            this.storeName = storeName;
            this.sendOldValues = sendOldValues;
        }

        private long TimeDifferenceMs => windows.TimeDifferenceMs;

        public override void Init(ProcessorContext context)
        {
            base.Init(context);
            windowStore = (ITimestampedWindowStore<K, Agg>) context.GetStateStore(storeName);
            tupleForwarder = new TimestampedTupleForwarder<Windowed<K>, Agg>(
                windowStore,
                this,
                kv =>
                {
                    context.CurrentProcessor = this;
                    Forward(kv.Key,
                        new Change<Agg>(sendOldValues ? kv.Value.OldValue.Value : default, kv.Value.NewValue.Value),
                        kv.Value.NewValue.Timestamp);
                },
                sendOldValues);
        }

        public override void Process(K key, V value)
        {
            if (key == null || value == null)
            {
                log.LogWarning(
                    "{LogPrefix}Skipping record due to null key or value. topic=[{Topic}] partition=[{Partition}] offset=[{Offset}]",
                    logPrefix, Context.Topic, Context.Partition, Context.Offset);
                droppedRecordsSensor.Record();
                return;
            }

            long inputRecordTimestamp = Context.Timestamp;
            observedStreamTime = Math.Max(observedStreamTime, inputRecordTimestamp);
            long windowCloseTime = observedStreamTime - windows.GracePeriodMs;

            // all the windows which could contain the record are closed (the latest one is its right window)
            if (inputRecordTimestamp + 1L + TimeDifferenceMs <= windowCloseTime)
            {
                LogSkippedRecordForExpiredWindow(inputRecordTimestamp, windowCloseTime,
                    $"[{inputRecordTimestamp - TimeDifferenceMs},{inputRecordTimestamp + 1 + TimeDifferenceMs}]");
                return;
            }

            if (inputRecordTimestamp < TimeDifferenceMs)
                ProcessEarly(key, value, inputRecordTimestamp, windowCloseTime);
            else
                ProcessInOrder(key, value, inputRecordTimestamp, windowCloseTime);
        }

        // The windows are collected before being updated: the fetched range is at most 2 * timeDifference + 1 long,
        // and some stores don't support writes while one of their enumerators is open.
        private List<KeyValuePair<long, ValueAndTimestamp<Agg>>> FetchWindows(K key, long from, long to)
        {
            var result = new List<KeyValuePair<long, ValueAndTimestamp<Agg>>>();
            using var enumerator = windowStore.Fetch(key, from, to);
            while (enumerator.MoveNext())
            {
                var current = enumerator.Current;
                if (current.HasValue && current.Value.Value != null)
                    result.Add(current.Value);
            }

            // windows are processed by start time
            result.Sort((w1, w2) => w1.Key.CompareTo(w2.Key));
            return result;
        }

        private void ProcessInOrder(K key, V value, long inputRecordTimestamp, long windowCloseTime)
        {
            var windowStartTimes = new HashSet<long>();

            // aggregate that will go in the current record's left/right window (if needed)
            ValueAndTimestamp<Agg> leftWinAgg = null;
            ValueAndTimestamp<Agg> rightWinAgg = null;

            // if current record's left/right windows already exist
            bool leftWinAlreadyCreated = false;
            bool rightWinAlreadyCreated = false;

            long? previousRecordTimestamp = null;

            // add 1 to upper bound to catch the current record's right window, if it exists
            var windowsToProcess = FetchWindows(key,
                Math.Max(0, inputRecordTimestamp - 2 * TimeDifferenceMs),
                inputRecordTimestamp + 1);

            foreach (var windowBeingProcessed in windowsToProcess)
            {
                long startTime = windowBeingProcessed.Key;
                windowStartTimes.Add(startTime);
                long endTime = startTime + TimeDifferenceMs;
                long windowMaxRecordTimestamp = windowBeingProcessed.Value.Timestamp;

                if (endTime < inputRecordTimestamp)
                {
                    leftWinAgg = windowBeingProcessed.Value;
                    previousRecordTimestamp = windowMaxRecordTimestamp;
                }
                else if (endTime == inputRecordTimestamp)
                {
                    leftWinAlreadyCreated = true;
                    if (windowMaxRecordTimestamp < inputRecordTimestamp)
                        previousRecordTimestamp = windowMaxRecordTimestamp;
                    UpdateWindowAndForward(Window(startTime), windowBeingProcessed.Value, key, value,
                        inputRecordTimestamp, windowCloseTime);
                }
                else if (endTime > inputRecordTimestamp && startTime <= inputRecordTimestamp)
                {
                    rightWinAgg = windowBeingProcessed.Value;
                    UpdateWindowAndForward(Window(startTime), windowBeingProcessed.Value, key, value,
                        inputRecordTimestamp, windowCloseTime);
                }
                else if (startTime == inputRecordTimestamp + 1)
                {
                    rightWinAlreadyCreated = true;
                }
                else
                {
                    log.LogError(
                        "{LogPrefix}Unexpected window with start {StartTime} found when processing record at {Timestamp} in sliding windows aggregate",
                        logPrefix, startTime, inputRecordTimestamp);
                    throw new IllegalStateException("Unexpected window found when processing sliding windows");
                }
            }

            CreateWindows(key, value, inputRecordTimestamp, windowCloseTime, windowStartTimes, rightWinAgg, leftWinAgg,
                leftWinAlreadyCreated, rightWinAlreadyCreated, previousRecordTimestamp);
        }

        // Records with 0 <= timestamp < timeDifference would create windows with a negative start: they all go in
        // the [0, timeDifference] window instead, and their right windows are created or updated as new records come in.
        private void ProcessEarly(K key, V value, long inputRecordTimestamp, long windowCloseTime)
        {
            if (inputRecordTimestamp < 0 || inputRecordTimestamp >= TimeDifferenceMs)
            {
                log.LogError(
                    "{LogPrefix}Early record for sliding windows must fall between 0 <= inputRecordTimestamp < timeDifference. Timestamp {Timestamp} does not fall between 0 <= {TimeDifference}",
                    logPrefix, inputRecordTimestamp, TimeDifferenceMs);
                throw new ArgumentException(
                    "Early record for sliding windows must fall between 0 <= inputRecordTimestamp < timeDifference");
            }

            // the window [0, timeDifference] which holds all the early records
            KeyValuePair<long, ValueAndTimestamp<Agg>>? combinedWindow = null;
            ValueAndTimestamp<Agg> rightWinAgg = null;
            bool rightWinAlreadyCreated = false;
            var windowStartTimes = new HashSet<long>();

            long? previousRecordTimestamp = null;

            // add 1 to upper bound to catch the current record's right window, if it exists
            var windowsToProcess = FetchWindows(key, 0, inputRecordTimestamp + 1);

            foreach (var windowBeingProcessed in windowsToProcess)
            {
                long startTime = windowBeingProcessed.Key;
                windowStartTimes.Add(startTime);
                long windowMaxRecordTimestamp = windowBeingProcessed.Value.Timestamp;

                if (startTime == 0)
                {
                    combinedWindow = windowBeingProcessed;
                    // previousRecordTimestamp is not needed if maxRecordTimestamp >= timestamp, because the previous
                    // record's right window (if there is a previous record) is already created by maxRecordTimestamp
                    if (windowMaxRecordTimestamp < inputRecordTimestamp)
                        previousRecordTimestamp = windowMaxRecordTimestamp;
                }
                else if (startTime <= inputRecordTimestamp)
                {
                    rightWinAgg = windowBeingProcessed.Value;
                    UpdateWindowAndForward(Window(startTime), windowBeingProcessed.Value, key, value,
                        inputRecordTimestamp, windowCloseTime);
                }
                else if (startTime == inputRecordTimestamp + 1)
                {
                    rightWinAlreadyCreated = true;
                }
                else
                {
                    log.LogError(
                        "{LogPrefix}Unexpected window with start {StartTime} found when processing record at {Timestamp} in sliding windows aggregate",
                        logPrefix, startTime, inputRecordTimestamp);
                    throw new IllegalStateException("Unexpected window found when processing sliding windows");
                }
            }

            // If no right window aggregate was found and the new record needs a right window, the aggregate of the
            // combined window goes in it: the combined window then only holds records of the record's right window,
            // because a record both before and after the current one would have given a right window above,
            // and if there are only records before it, the combined window's timestamp is smaller than the record's.
            if (rightWinAgg == null && combinedWindow.HasValue && combinedWindow.Value.Value.Timestamp > inputRecordTimestamp)
                rightWinAgg = combinedWindow.Value.Value;

            if (!rightWinAlreadyCreated && RightWindowIsNotEmpty(rightWinAgg, inputRecordTimestamp))
                CreateCurrentRecordRightWindow(inputRecordTimestamp, rightWinAgg, key);

            // create the right window of the previous record, if it exists and isn't created yet
            if (previousRecordTimestamp.HasValue && !windowStartTimes.Contains(previousRecordTimestamp.Value + 1))
                CreatePreviousRecordRightWindow(previousRecordTimestamp.Value + 1, key, value, inputRecordTimestamp,
                    windowCloseTime);

            if (!combinedWindow.HasValue)
            {
                var valueAndTime = ValueAndTimestamp<Agg>.Make(initializer.Apply(), inputRecordTimestamp);
                UpdateWindowAndForward(Window(0), valueAndTime, key, value, inputRecordTimestamp, windowCloseTime);
            }
            else
            {
                // update the combined window with the new aggregate
                UpdateWindowAndForward(Window(0), combinedWindow.Value.Value, key, value, inputRecordTimestamp,
                    windowCloseTime);
            }
        }

        private void CreateWindows(K key,
            V value,
            long inputRecordTimestamp,
            long windowCloseTime,
            HashSet<long> windowStartTimes,
            ValueAndTimestamp<Agg> rightWinAgg,
            ValueAndTimestamp<Agg> leftWinAgg,
            bool leftWinAlreadyCreated,
            bool rightWinAlreadyCreated,
            long? previousRecordTimestamp)
        {
            // create the right window of the previous record
            if (previousRecordTimestamp.HasValue)
            {
                long previousRightWinStart = previousRecordTimestamp.Value + 1;
                if (PreviousRecordRightWindowDoesNotExistAndIsNotEmpty(windowStartTimes, previousRightWinStart,
                        inputRecordTimestamp))
                    CreatePreviousRecordRightWindow(previousRightWinStart, key, value, inputRecordTimestamp,
                        windowCloseTime);
            }

            // create the left window of the new record
            if (!leftWinAlreadyCreated)
            {
                var valueAndTime = LeftWindowNotEmpty(previousRecordTimestamp, inputRecordTimestamp)
                    ? ValueAndTimestamp<Agg>.Make(leftWinAgg.Value, inputRecordTimestamp)
                    : ValueAndTimestamp<Agg>.Make(initializer.Apply(), inputRecordTimestamp);
                UpdateWindowAndForward(Window(inputRecordTimestamp - TimeDifferenceMs), valueAndTime, key, value,
                    inputRecordTimestamp, windowCloseTime);
            }

            // create the right window of the new record, if needed
            if (!rightWinAlreadyCreated && RightWindowIsNotEmpty(rightWinAgg, inputRecordTimestamp))
                CreateCurrentRecordRightWindow(inputRecordTimestamp, rightWinAgg, key);
        }

        private void CreateCurrentRecordRightWindow(long inputRecordTimestamp, ValueAndTimestamp<Agg> rightWinAgg, K key)
        {
            var window = Window(inputRecordTimestamp + 1);
            windowStore.Put(key, rightWinAgg, window.StartMs);
            tupleForwarder.MaybeForward(new Windowed<K>(key, window), rightWinAgg.Value, default, rightWinAgg.Timestamp);
        }

        private void CreatePreviousRecordRightWindow(long windowStart, K key, V value, long inputRecordTimestamp,
            long windowCloseTime)
        {
            var valueAndTime = ValueAndTimestamp<Agg>.Make(initializer.Apply(), inputRecordTimestamp);
            UpdateWindowAndForward(Window(windowStart), valueAndTime, key, value, inputRecordTimestamp, windowCloseTime);
        }

        // the previous record falls into the current record's left window
        private bool LeftWindowNotEmpty(long? previousRecordTimestamp, long inputRecordTimestamp)
            => previousRecordTimestamp.HasValue &&
               inputRecordTimestamp - TimeDifferenceMs <= previousRecordTimestamp.Value;

        // the previous record's right window doesn't exist yet and the current record falls into it
        private bool PreviousRecordRightWindowDoesNotExistAndIsNotEmpty(HashSet<long> windowStartTimes,
            long previousRightWindowStart, long inputRecordTimestamp)
            => !windowStartTimes.Contains(previousRightWindowStart) &&
               previousRightWindowStart + TimeDifferenceMs >= inputRecordTimestamp;

        // the aggregate found has records which fall into the current record's right window
        private static bool RightWindowIsNotEmpty(ValueAndTimestamp<Agg> rightWinAgg, long inputRecordTimestamp)
            => rightWinAgg != null && rightWinAgg.Timestamp > inputRecordTimestamp;

        private void UpdateWindowAndForward(TimeWindow window,
            ValueAndTimestamp<Agg> valueAndTime,
            K key,
            V value,
            long inputRecordTimestamp,
            long windowCloseTime)
        {
            if (window.EndMs >= windowCloseTime)
            {
                // aggregate of the existing window
                Agg oldAgg = valueAndTime == null ? default : valueAndTime.Value;
                Agg newAgg = aggregator.Apply(key, value, oldAgg);

                long newTimestamp = oldAgg == null
                    ? inputRecordTimestamp
                    : Math.Max(inputRecordTimestamp, valueAndTime.Timestamp);
                windowStore.Put(key, ValueAndTimestamp<Agg>.Make(newAgg, newTimestamp), window.StartMs);
                tupleForwarder.MaybeForward(new Windowed<K>(key, window), newAgg, sendOldValues ? oldAgg : default,
                    newTimestamp);
            }
            else
                LogSkippedRecordForExpiredWindow(inputRecordTimestamp, windowCloseTime,
                    $"[{window.StartMs},{window.EndMs}]");
        }

        private TimeWindow Window(long start) => new(start, start + TimeDifferenceMs);

        private void LogSkippedRecordForExpiredWindow(long timestamp, long windowCloseTime, string window)
        {
            log.LogDebug(
                "{LogPrefix}Skipping record for expired window. topic=[{Topic}] partition=[{Partition}] offset=[{Offset}] timestamp=[{Timestamp}] window={Window} expiration=[{Expiration}] streamTime=[{StreamTime}]",
                logPrefix, Context.Topic, Context.Partition, Context.Offset, timestamp, window, windowCloseTime,
                observedStreamTime);
            droppedRecordsSensor.Record();
        }
    }
}
