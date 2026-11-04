using System;
using Confluent.Kafka;

namespace Streamiz.Kafka.Net.Errors
{
    /// <summary>
    /// Attaches the source record (topic, partition, offset) that caused an exception to the exception itself,
    /// without changing the exception type the user sees.
    /// Stream threads use it to skip that record when the InnerExceptionHandler returns CONTINUE;
    /// otherwise the record is consumed again after the thread rejoins the group and fails forever.
    /// </summary>
    internal static class FailedRecordInfo
    {
        private const string TopicKey = "streamiz.failed.record.topic";
        private const string PartitionKey = "streamiz.failed.record.partition";
        private const string OffsetKey = "streamiz.failed.record.offset";

        internal static void Attach(Exception exception, ConsumeResult<byte[], byte[]> record)
        {
            if (exception == null || record == null || exception.Data.Contains(TopicKey))
                return;

            try
            {
                exception.Data[TopicKey] = record.Topic;
                exception.Data[PartitionKey] = record.Partition.Value;
                exception.Data[OffsetKey] = record.Offset.Value;
            }
            catch (Exception)
            {
                // Data can be read-only for some custom exceptions: the record just won't be skipped.
            }
        }

        internal static bool TryGet(Exception exception, out (string Topic, int Partition, long Offset) record)
        {
            record = default;
            if (exception == null || !exception.Data.Contains(TopicKey))
                return false;

            record = ((string) exception.Data[TopicKey], (int) exception.Data[PartitionKey],
                (long) exception.Data[OffsetKey]);
            return true;
        }

        internal static (string Topic, int Partition, long Offset) Key(ConsumeResult<byte[], byte[]> record)
            => (record.Topic, record.Partition.Value, record.Offset.Value);
    }
}
