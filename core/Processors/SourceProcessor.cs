
using System.Threading;
using Confluent.Kafka;
using Streamiz.Kafka.Net.Metrics;
using Streamiz.Kafka.Net.Metrics.Internal;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.SerDes;

namespace Streamiz.Kafka.Net.Processors
{
    internal interface ISourceProcessor : IProcessor
    {
        string TopicName { get; set; }
        ITimestampExtractor Extractor { get; }
        // sendToDeadLetterQueue: false when the record is only deserialized for the timestamp extractor,
        // so a skipped record is sent once, when it is processed
        ObjectDeserialized DeserializeKey(ConsumeResult<byte[], byte[]> record, bool sendToDeadLetterQueue = true);
        ObjectDeserialized DeserializeValue(ConsumeResult<byte[], byte[]> record, bool sendToDeadLetterQueue = true);
    }

    internal class SourceProcessor<K, V> : AbstractProcessor<K, V>, ISourceProcessor
    {
        internal Sensor ProcessSensor { get; private set; }
        
        internal SourceProcessor(string name, string topicName, ISerDes<K> keySerdes, ISerDes<V> valueSerdes, ITimestampExtractor extractor)
            : base(name, keySerdes, valueSerdes)
        {
            TopicName = topicName;
            Extractor = extractor;
        }

        public string TopicName { get; set; }

        public ITimestampExtractor Extractor { get; }

        public override void Init(ProcessorContext context)
        {
            if (Key == null)
            {
                Key = context.Configuration.DefaultKeySerDes;
            }

            if (Value == null)
            {
                Value = context.Configuration.DefaultValueSerDes;
            }

            Key?.Initialize(context.SerDesContext);
            Value?.Initialize(context.SerDesContext);

            ProcessSensor = ProcessorNodeMetrics.ProcessNodeSensor(
                Thread.CurrentThread.Name,
                context.Id,
                Name,
                context.Metrics);
                
            base.Init(context);
        }

        public override void Process(K key, V value)
        {
            MaybeRecordEndToEndLatency();
            Forward(key, value);
            ProcessSensor.Record(1.0, Context.Timestamp);
        }
    }
}
