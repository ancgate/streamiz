using System;
using System.Threading;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Metrics;
using Streamiz.Kafka.Net.Metrics.Internal;
using Streamiz.Kafka.Net.Processors.Public;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State.Suppress;
using Streamiz.Kafka.Net.Table;
using Streamiz.Kafka.Net.Table.Internal;

namespace Streamiz.Kafka.Net.Processors
{
    internal class KTableSuppressProcessor<K, V> : AbstractProcessor<K, Change<V>>
    {
        private readonly long maxRecords;
        private readonly long maxBytes;
        private readonly long suppressionDurationMs;
        private readonly BUFFER_FULL_STRATEGY bufferFullStrategy;
        private readonly bool safeToDropTombstone;
        private readonly string storeName;
        private readonly Func<ProcessorContext, K, long> timeDefinition;
        private readonly bool useWallClockTime;

        // wall-clock mode: one punctuation scheduled for when the oldest buffered record is due
        private TaskScheduled evictionPunctuation;
        private long evictionPunctuationDueTime = -1;
        
        private Sensor suppressionEmitSensor;
        private long observedStreamTime = -1;
        private ITimeOrderedKeyValueBuffer<K, V, Change<V>> buffer;
        private readonly ISerDes<K> keySerdes;
        private readonly ISerDes<V> valueSerdes;

        private bool OverCapacity => buffer.BufferSize > maxBytes || buffer.NumRecords > maxRecords;

        public KTableSuppressProcessor(Suppressed<K, V> suppressed, string storeName)
        {
            maxRecords = suppressed.BufferConfig.MaxRecords;
            maxBytes = suppressed.BufferConfig.MaxBytes;
            bufferFullStrategy = suppressed.BufferConfig.BufferFullStrategy;
            suppressionDurationMs = (long)suppressed.SuppressionTime.TotalMilliseconds;
            safeToDropTombstone = suppressed.SafeToDropTombstones;
            this.storeName = storeName;
            timeDefinition = suppressed.TimeDefinition;
            useWallClockTime = suppressed.UseWallClockTime;
            keySerdes = suppressed.KeySerdes;
            valueSerdes = suppressed.ValueSerdes;
        }

        public override void Init(ProcessorContext context)
        {
            base.Init(context);
            suppressionEmitSensor = ProcessorNodeMetrics.SuppressionEmitSensor(
                Thread.CurrentThread.Name,
                context.Id,
                Name,
                context.Metrics);
            
            buffer = (ITimeOrderedKeyValueBuffer<K, V, Change<V>>)context.GetStateStore(storeName);
            buffer.SetSerdesIfNull(keySerdes, valueSerdes);

            if (useWallClockTime)
            {
                // The buffer can be restored from its changelog after Init: check it once on the first punctuation.
                TaskScheduled startup = null;
                startup = Context.Task.RegisterScheduleTask(TimeSpan.FromMilliseconds(1),
                    PunctuationType.PROCESSING_TIME, now =>
                    {
                        startup.Cancel();
                        EvictExpiredWallClock(now);
                    });
            }
        }

        public override void Process(K key, Change<V> value)
        {
            observedStreamTime = Math.Max(observedStreamTime, Context.Timestamp);
            Buffer(key, value);
            EnforceConstraints();
            if (useWallClockTime)
                ScheduleNextWallClockEviction();
        }

        private void Buffer(K key, Change<V> value)
        {
            var bufferTime = timeDefinition(Context, key);
            buffer.Put(bufferTime, key, value, Context.RecordContext);
        }

        private void EvictExpired(long expiryTime)
            => buffer.EvictWhile(() => buffer.MinTimestamp <= expiryTime, Emit);

        private void EvictExpiredWallClock(long now)
        {
            EvictExpired(now - suppressionDurationMs);
            ScheduleNextWallClockEviction();
        }

        // Punctuate only when a record is due (and not periodically), so nothing runs while the buffer is empty.
        private void ScheduleNextWallClockEviction()
        {
            long dueTime = buffer.MinTimestamp == long.MaxValue
                ? -1
                : buffer.MinTimestamp + suppressionDurationMs;

            if (dueTime == evictionPunctuationDueTime)
                return;

            if (evictionPunctuation is {IsCancelled: false})
                evictionPunctuation.Cancel();
            evictionPunctuation = null;
            evictionPunctuationDueTime = dueTime;

            if (dueTime < 0)
                return;

            var delay = TimeSpan.FromMilliseconds(Math.Max(1, dueTime - Context.WallClockTime));
            evictionPunctuation = Context.Task.RegisterScheduleTask(delay, PunctuationType.PROCESSING_TIME,
                EvictExpiredWallClock);
        }

        private void EnforceConstraints()
        {
            // buffer times are wall-clock times in wall-clock mode, stream times otherwise
            long now = useWallClockTime ? Context.WallClockTime : observedStreamTime;
            EvictExpired(now - suppressionDurationMs);

            if (OverCapacity)
            {
                switch (bufferFullStrategy)
                {
                    case BUFFER_FULL_STRATEGY.EMIT:
                        buffer.EvictWhile(() => OverCapacity, Emit);
                        break;
                    case BUFFER_FULL_STRATEGY.SHUTDOWN:
                        throw new StreamsException(
                            $"{Name} buffer exceeded its max capacity. Currently [{buffer.NumRecords}/{maxRecords}] records and [{buffer.BufferSize}/{maxBytes}] bytes.");
                }
            }
        }

        private void Emit(K key, Change<V> value, IRecordContext context)
        {
            if (value.NewValue != null || !safeToDropTombstone)
            {
                var currentRecordContext = Context.RecordContext;
                try
                {
                    Context.SetRecordMetaData(context);
                    Forward(key, value);
                    suppressionEmitSensor.Record();
                }
                finally
                {
                    Context.SetRecordMetaData(currentRecordContext);
                }
            }
        }
    }
}