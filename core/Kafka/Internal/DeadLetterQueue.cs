using System;
using System.Text;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.SerDes;

namespace Streamiz.Kafka.Net.Kafka.Internal
{
    /// <summary>
    /// Sends source records skipped by an exception handler to the dead letter queue topic
    /// (<see cref="IStreamConfig.DeadLetterQueueTopicName"/>), with the error described in headers (KIP-1034).
    /// </summary>
    internal static class DeadLetterQueue
    {
        internal const string HEADER_PREFIX = "__streams.errors.";
        internal const string EXCEPTION_HEADER = HEADER_PREFIX + "exception";
        internal const string EXCEPTION_MESSAGE_HEADER = HEADER_PREFIX + "message";
        internal const string EXCEPTION_STACKTRACE_HEADER = HEADER_PREFIX + "stacktrace";
        internal const string TOPIC_HEADER = HEADER_PREFIX + "topic";
        internal const string PARTITION_HEADER = HEADER_PREFIX + "partition";
        internal const string OFFSET_HEADER = HEADER_PREFIX + "offset";

        private static readonly ByteArraySerDes serdes = new();

        /// <summary>
        /// Send <paramref name="record"/> (raw key and value, original headers and timestamp) to the dead letter
        /// queue, if one is configured.
        /// </summary>
        internal static void SendIfConfigured(
            IStreamConfig config,
            IRecordCollector collector,
            ConsumeResult<byte[], byte[]> record,
            Exception exception,
            ILogger log,
            string logPrefix)
        {
            var topic = config.DeadLetterQueueTopicName;
            if (string.IsNullOrEmpty(topic) || record == null)
                return;

            if (collector == null)
            {
                log.LogWarning(
                    "{LogPrefix}Record (Topic:{Topic}|Partition:{Partition}|Offset:{Offset}) can't be sent to the dead letter queue {DeadLetterQueue}: no producer is available here",
                    logPrefix, record.Topic, record.Partition.Value, record.Offset.Value, topic);
                return;
            }

            var headers = record.Message.Headers.Clone();
            headers.Add(EXCEPTION_HEADER, Encoding.UTF8.GetBytes(exception.GetType().FullName ?? exception.GetType().Name));
            headers.Add(EXCEPTION_MESSAGE_HEADER, Encoding.UTF8.GetBytes(exception.Message ?? string.Empty));
            headers.Add(EXCEPTION_STACKTRACE_HEADER, Encoding.UTF8.GetBytes(exception.ToString()));
            headers.Add(TOPIC_HEADER, Encoding.UTF8.GetBytes(record.Topic));
            headers.Add(PARTITION_HEADER, Encoding.UTF8.GetBytes(record.Partition.Value.ToString()));
            headers.Add(OFFSET_HEADER, Encoding.UTF8.GetBytes(record.Offset.Value.ToString()));

            collector.Send(topic, record.Message.Key, record.Message.Value, headers,
                record.Message.Timestamp.UnixTimestampMs, serdes, serdes);

            log.LogWarning(
                "{LogPrefix}Record (Topic:{Topic}|Partition:{Partition}|Offset:{Offset}) sent to the dead letter queue {DeadLetterQueue}",
                logPrefix, record.Topic, record.Partition.Value, record.Offset.Value, topic);
        }
    }
}
