using Confluent.Kafka;
using Streamiz.Kafka.Net.Processors.Internal;

namespace Streamiz.Kafka.Net
{
    /// <summary>
    /// Information about the source record which raised an exception, given to
    /// <see cref="IStreamConfig.ProcessingExceptionHandler"/>.
    /// </summary>
    public class ErrorHandlerContext
    {
        internal ErrorHandlerContext(ConsumeResult<byte[], byte[]> record, string processorNodeId, TaskId taskId)
        {
            Topic = record.Topic;
            Partition = record.Partition.Value;
            Offset = record.Offset.Value;
            Timestamp = record.Message.Timestamp.UnixTimestampMs;
            Headers = record.Message.Headers;
            SourceRawKey = record.Message.Key;
            SourceRawValue = record.Message.Value;
            ProcessorNodeId = processorNodeId;
            TaskId = taskId;
        }

        /// <summary>
        /// Topic of the source record
        /// </summary>
        public string Topic { get; }

        /// <summary>
        /// Partition of the source record
        /// </summary>
        public int Partition { get; }

        /// <summary>
        /// Offset of the source record
        /// </summary>
        public long Offset { get; }

        /// <summary>
        /// Timestamp (unix milliseconds) of the source record
        /// </summary>
        public long Timestamp { get; }

        /// <summary>
        /// Headers of the source record
        /// </summary>
        public Headers Headers { get; }

        /// <summary>
        /// Key of the source record, as consumed (not deserialized)
        /// </summary>
        public byte[] SourceRawKey { get; }

        /// <summary>
        /// Value of the source record, as consumed (not deserialized)
        /// </summary>
        public byte[] SourceRawValue { get; }

        /// <summary>
        /// Name of the processor which raised the exception, null if it is unknown
        /// </summary>
        public string ProcessorNodeId { get; }

        /// <summary>
        /// Task which processed the record
        /// </summary>
        public TaskId TaskId { get; }
    }
}
