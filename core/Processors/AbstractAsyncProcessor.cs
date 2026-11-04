using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Metrics;
using Streamiz.Kafka.Net.Kafka.Internal;
using Streamiz.Kafka.Net.Metrics.Internal;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.Stream;

namespace Streamiz.Kafka.Net.Processors
{
    internal abstract class AbstractAsyncProcessor<K, V, K1, V1> :
        AbstractProcessor<K, V>, IAsyncProcessor<K, V, K1, V1>, IInTaskAsyncProcessor
    {
        private Sensor retrySensor;

        #region AsyncProcessingMode.V2 state

        private static readonly object NullKey = new();

        private bool inTask;
        private AsyncProcessingCoordinator coordinator;
        private CancellationTokenSource cancellation;
        private long timeoutMs;
        // keys with a call in progress -> records waiting for it (same key : in order)
        private readonly Dictionary<object, Queue<InTaskRecord>> keysInProgress = new();
        private readonly object keysLock = new();
        // only used by the stream thread
        private int pending;

        public int Pending => pending;

        public bool Saturated => pending >= Policy.MaxConcurrency;

        public int MaxConcurrency => Policy.MaxConcurrency;

        #endregion

        protected AbstractAsyncProcessor(RetryPolicy policy)
        {
            Policy = policy;
        }

        public RetryPolicy Policy { get; }

        public override void Process(K key, V value)
        {
            if (inTask)
            {
                ProcessInTask(key, value);
                return;
            }

            var context = new ExternalContext() {
                RetryNumber = 0
            };
            DateTime startProcessing = DateTime.Now;
            bool result = false, retry = true;
            Task<IEnumerable<KeyValuePair<K1, V1>>> task = null;
            Exception noneRetriableException = null;
            context.FirstCallEpoch = DateTime.Now.GetMilliseconds();
            
            while (retry)
            {
                if(context.RetryNumber > 0)
                    Thread.Sleep(TimeSpan.FromMilliseconds(Policy.RetryBackOffMs));

                context.CurrentCallEpoch = DateTime.Now.GetMilliseconds();
                
                if (context.RetryNumber == Policy.NumberOfRetry)
                    throw new NoneRetryableException($"Number of retry exceeded", context.RetryNumber,  context.CurrentCallEpoch - startProcessing.GetMilliseconds(), null); 
                
                ++context.RetryNumber;

                if (startProcessing.Add(TimeSpan.FromMilliseconds(Policy.TimeoutMs)).GetMilliseconds() <
                    context.CurrentCallEpoch)
                    throw new NotEnoughtTimeException("", context.CurrentCallEpoch - startProcessing.GetMilliseconds());
                
                task = ProcessAsync(key, value, Context.RecordContext.Headers, Context.Timestamp, context);
                retrySensor.Record(context.RetryNumber);
                
                try
                {
                    task.Wait();
                    result = true;
                    retry = false;
                }
                catch (AggregateException ae)
                {
                    LogProcessingKeyValueWithRetryNumber(key, value, context.RetryNumber, false);
                    if (ContainsRetryableExceptions(ae))
                    {
                        context.LastExceptions = ae.InnerExceptions;
                        log.LogDebug($"{logPrefix}An retryable exception is thrown during the processing : {ae.InnerExceptions.First().Message}");
                        log.LogInformation($"{logPrefix}An retryable exception is thrown during the processing : {ae.InnerExceptions.First().Message}");
                    }
                    else
                    {
                        retry = false;
                        noneRetriableException = ae.InnerExceptions.First();
                    }
                }
            }

            if (result && task.IsCompleted)
            {
                LogProcessingKeyValueWithRetryNumber(key, value, context.RetryNumber, true);
                if (task.Result != null && task.Result.Any())
                {
                    foreach (var kv in task.Result)
                    {
                        var cloneHeader = Context.RecordContext.Headers.Clone();
                        Forward(kv.Key, kv.Value);
                        Context.SetHeaders(cloneHeader);
                    }
                }
            }

            if (!retry && !result)
                throw new StreamsException(noneRetriableException);
        }
        
        public override void Init(ProcessorContext context)
        {
            retrySensor = ProcessorNodeMetrics.RetrySensor( 
                Thread.CurrentThread.Name,
                context.Id,
                Name,
                context.Metrics);
            
            base.Init(context);

            // V2 : the processor runs inside the stream task
            if (context.Configuration.AsyncProcessingMode == AsyncProcessingMode.V2 && context.Task is StreamTask streamTask)
            {
                inTask = true;
                coordinator = streamTask.AsyncCoordinator;
                coordinator.Register(this);
                cancellation = new CancellationTokenSource();
                timeoutMs = Policy.TimeoutMs > 0
                    ? Policy.TimeoutMs
                    : (long) (context.Configuration.MaxPollIntervalMs ?? 300000) / 3;
            }
        }

        #region AsyncProcessingMode.V2

        private class InTaskRecord : IAsyncCompletion
        {
            private readonly AbstractAsyncProcessor<K, V, K1, V1> processor;

            public InTaskRecord(AbstractAsyncProcessor<K, V, K1, V1> processor, K key, V value,
                IRecordContext recordContext, ConsumeResult<byte[], byte[]> sourceRecord)
            {
                this.processor = processor;
                Key = key;
                Value = value;
                RecordContext = recordContext;
                SourceRecord = sourceRecord;
            }

            public K Key { get; }
            public V Value { get; }
            public IRecordContext RecordContext { get; }
            public ConsumeResult<byte[], byte[]> SourceRecord { get; }
            public IEnumerable<KeyValuePair<K1, V1>> Result { get; set; }
            public Exception Error { get; set; }

            public void Complete() => processor.CompleteInTask(this);
        }

        // stream thread : accept the record, start its call now or after the call in progress for the same key
        private void ProcessInTask(K key, V value)
        {
            var recordContext = Context.RecordContext;
            var record = new InTaskRecord(this, key, value,
                new RecordContext(recordContext.Headers.Clone(), recordContext.Offset, recordContext.Timestamp,
                    recordContext.Partition, recordContext.Topic),
                ((StreamTask) Context.Task).CurrentSourceRecord);

            ++pending;
            coordinator.TrackSourceRecord(record.SourceRecord);

            bool start;
            object keyLock = (object) key ?? NullKey;
            lock (keysLock)
            {
                if (keysInProgress.TryGetValue(keyLock, out var waiting))
                {
                    waiting.Enqueue(record);
                    start = false;
                }
                else
                {
                    keysInProgress.Add(keyLock, new Queue<InTaskRecord>());
                    start = true;
                }
            }

            if (start)
                Start(record);
        }

        // the call runs on the thread pool; its completion is queued for the stream thread, then the next record
        // of the same key (if any) is started
        private void Start(InTaskRecord record)
        {
            var token = cancellation.Token;
            Task.Run(async () =>
            {
                try
                {
                    record.Result = await ProcessWithRetryAsync(record, token);
                }
                catch (Exception e)
                {
                    record.Error = e;
                }

                if (token.IsCancellationRequested)
                    return;

                coordinator.Enqueue(record);

                InTaskRecord next = null;
                object keyLock = (object) record.Key ?? NullKey;
                lock (keysLock)
                {
                    if (keysInProgress.TryGetValue(keyLock, out var waiting))
                    {
                        if (waiting.Count > 0)
                            next = waiting.Dequeue();
                        else
                            keysInProgress.Remove(keyLock);
                    }
                }

                if (next != null)
                    Start(next);
            }, token);
        }

        private async Task<IEnumerable<KeyValuePair<K1, V1>>> ProcessWithRetryAsync(InTaskRecord record, CancellationToken token)
        {
            var context = new ExternalContext { RetryNumber = 0 };
            long startProcessing = DateTime.Now.GetMilliseconds();
            context.FirstCallEpoch = startProcessing;
            // BUFFERED : the record is retried until it succeeds (the following records of the same key wait)
            bool unbounded = Policy.EndRetryBehavior == EndRetryBehavior.BUFFERED;

            while (true)
            {
                token.ThrowIfCancellationRequested();
                if (context.RetryNumber > 0)
                    await Task.Delay(TimeSpan.FromMilliseconds(Policy.RetryBackOffMs), token);

                context.CurrentCallEpoch = DateTime.Now.GetMilliseconds();

                if (!unbounded && context.RetryNumber >= Policy.NumberOfRetry)
                    throw new NoneRetryableException("Number of retry exceeded", context.RetryNumber,
                        context.CurrentCallEpoch - startProcessing, context.LastExceptions?.FirstOrDefault());

                long remainingMs = startProcessing + timeoutMs - context.CurrentCallEpoch;
                if (!unbounded && remainingMs < 0)
                    throw new NotEnoughtTimeException("", context.CurrentCallEpoch - startProcessing);

                ++context.RetryNumber;
                retrySensor.Record(context.RetryNumber);

                var call = ProcessAsync(record.Key, record.Value, record.RecordContext.Headers,
                    record.RecordContext.Timestamp, context);

                if (!unbounded &&
                    await Task.WhenAny(call, Task.Delay(TimeSpan.FromMilliseconds(remainingMs), token)) != call)
                {
                    // the call can't be cancelled, its result is ignored
                    _ = call.ContinueWith(t => t.Exception, TaskContinuationOptions.OnlyOnFaulted);
                    throw new NotEnoughtTimeException("", DateTime.Now.GetMilliseconds() - startProcessing);
                }

                try
                {
                    var result = await call;
                    LogProcessingKeyValueWithRetryNumber(record, context.RetryNumber, true);
                    return result;
                }
                catch (Exception e) when (IsRetriable(e))
                {
                    LogProcessingKeyValueWithRetryNumber(record, context.RetryNumber, false);
                    context.LastExceptions = new List<Exception> {e}.AsReadOnly();
                }
            }
        }

        // stream thread : forward the results with the record context of the source record, or raise the error
        private void CompleteInTask(InTaskRecord record)
        {
            --pending;

            if (record.Error != null)
            {
                if (record.Error is NoneRetryableException or NotEnoughtTimeException &&
                    Policy.EndRetryBehavior == EndRetryBehavior.SKIP)
                {
                    log.LogInformation(
                        "{LogPrefix}Record with this following metadata (Topic:{Topic}|Partition:{Partition}|Offset:{Offset}) exceed the retry policy. The retry policy behavior will be skipped this message and process next one",
                        logPrefix, record.RecordContext.Topic, record.RecordContext.Partition, record.RecordContext.Offset);
                    droppedRecordsSensor.Record();
                    DeadLetterQueue.SendIfConfigured(Context.Configuration, Context.RecordCollector,
                        record.SourceRecord, record.Error, log, logPrefix);
                    return;
                }

                // retry policy FAIL, or a none retriable exception : the error goes through the processing exception handler
                if (record.Error is NoneRetryableException or NotEnoughtTimeException)
                    throw record.Error;
                throw new StreamsException(record.Error);
            }

            if (record.Result == null)
                return;

            Context.SetRecordMetaData(record.RecordContext);
            foreach (var kv in record.Result)
            {
                Context.CurrentProcessor = this;
                var cloneHeader = Context.RecordContext.Headers.Clone();
                Forward(kv.Key, kv.Value);
                Context.SetHeaders(cloneHeader);
            }
        }

        public void Cancel()
        {
            cancellation?.Cancel();
            lock (keysLock)
                keysInProgress.Clear();
            pending = 0;
        }

        public override void Close()
        {
            if (inTask)
                Cancel();
            base.Close();
        }

        private bool IsRetriable(Exception exception)
            => Policy.RetriableExceptions.Any(type => type.IsInstanceOfType(exception));

        private void LogProcessingKeyValueWithRetryNumber(InTaskRecord record, int retryNumber, bool result)
        {
            if (log.IsEnabled(LogLevel.Debug))
            {
                log.LogDebug(
                    $"{logPrefix}Process<{typeof(K).Name},{typeof(V).Name}> message with key {record.Key} and {record.Value}" +
                    $" with record metadata [topic:{record.RecordContext.Topic}|" +
                    $"partition:{record.RecordContext.Partition}|offset:{record.RecordContext.Offset}] [retry.number={retryNumber}, result={(result ? "Success" : "Failure")}]");
            }
        }

        #endregion

        private bool ContainsRetryableExceptions(AggregateException ae)
            => (from innerException in ae.InnerExceptions 
                from policyException in Policy.RetriableExceptions 
                where policyException.IsInstanceOfType(innerException) 
                select innerException).Any();

        private void LogProcessingKeyValueWithRetryNumber(K key, V value, int retryNumber, bool result)
        {
            if (log.IsEnabled(LogLevel.Debug))
            {
                log.LogDebug(
                    $"{logPrefix}Process<{typeof(K).Name},{typeof(V).Name}> message with key {key} and {value}" +
                    $" with record metadata [topic:{Context.RecordContext.Topic}|" +
                    $"partition:{Context.RecordContext.Partition}|offset:{Context.RecordContext.Offset}] [retry.number={retryNumber}, result={(result ? "Success" : "Failure")}]");
            }
        }

        public abstract Task<IEnumerable<KeyValuePair<K1, V1>>> ProcessAsync(K key, V value, Headers headers, long timestamp,
            ExternalContext context);
    }
}