using Confluent.Kafka;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Mock.Sync;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Streamiz.Kafka.Net.Kafka.Internal;

namespace Streamiz.Kafka.Net.Mock.Pipes
{
    internal class SyncPipeOutput : IPipeOutput
    {
        private readonly string topicName;
        private readonly TimeSpan timeout;
        private readonly CancellationToken token;
        private readonly IConsumer<byte[], byte[]> consumer;
        private readonly SyncProducer producer;


        public SyncPipeOutput(string topic, TimeSpan consumeTimeout, IStreamConfig configuration, SyncProducer producer, CancellationToken token)
        {
            this.token = token;
            this.producer = producer;
            topicName = topic;
            timeout = consumeTimeout;
            consumer = new SyncConsumer(producer);
            (consumer as SyncConsumer).UseConfig(configuration.ToConsumerConfig($"pipe-output-{configuration.ApplicationId}-{topicName}"));
            consumer.Subscribe(topicName);
        }

        public string TopicName => topicName;

        // records written to the topic and not read yet by this output topic
        public int Size
        {
            get
            {
                long position = consumer.Position(new TopicPartition(topicName, 0)).Value;
                return (int) Math.Max(0, producer.GetHistory(topicName).Count() - Math.Max(0, position));
            }
        }

        public bool IsEmpty => Size == 0;

        public List<PipeOutputInfo> GetInfos()
        {
            long high = producer.GetHistory(topicName).Count();
            long position = Math.Max(0, consumer.Position(new TopicPartition(topicName, 0)).Value);
            return new List<PipeOutputInfo>
            {
                new() {Topic = topicName, Partition = 0, Offset = position, Low = 0, High = high}
            };
        }

        public void Dispose()
        {
            consumer.Unsubscribe();
            consumer.Dispose();
        }

        public ConsumeResult<byte[], byte[]> Read()
        {
            int count = 0;
            while (count <= 10 && !token.IsCancellationRequested)
            {
                var record = consumer.Consume(timeout);
                if (record != null)
                {
                    consumer.Commit(record);
                    return record;
                }
                else
                {
                    Thread.Sleep((int)timeout.TotalMilliseconds / 10);
                    ++count;
                }
            }

            throw new StreamsException($"No record found in topic {topicName} after {timeout.TotalSeconds}s !");
        }

        public IEnumerable<ConsumeResult<byte[], byte[]>> ReadList()
        {
            List<ConsumeResult<byte[], byte[]>> records = new List<ConsumeResult<byte[], byte[]>>();
            ConsumeResult<byte[], byte[]> record = null;
            do
            {
                record = consumer.Consume(timeout);
                if (record != null)
                {
                    consumer.Commit(record);
                    records.Add(record);
                }
            } while (!token.IsCancellationRequested && record != null);
            return records;
        }
    }
}
