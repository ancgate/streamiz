using System;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;

namespace Streamiz.Kafka.Net.Tests.Processors
{
    /// <summary>
    /// RetryPolicy.NumberOfRetry is the number of retries after the first attempt.
    /// </summary>
    public class AsyncRetryCountTests
    {
        [TestCase(AsyncProcessingMode.V1, 0, 1)]
        [TestCase(AsyncProcessingMode.V1, 2, 3)]
        [TestCase(AsyncProcessingMode.V2, 0, 1)]
        [TestCase(AsyncProcessingMode.V2, 2, 3)]
        public void NumberOfRetryIsTheNumberOfRetriesAfterTheFirstAttempt(AsyncProcessingMode mode, int retries, int expectedCalls)
        {
            int calls = 0;
            var builder = new StreamBuilder();
            builder.Stream<string, string>("input")
                .MapValuesAsync<string>(async (record, _) =>
                    {
                        Interlocked.Increment(ref calls);
                        await Task.Yield();
                        throw new StreamsException("external service error");
                    },
                    RetryPolicy.NewBuilder()
                        .RetriableException<StreamsException>()
                        .NumberOfRetry(retries)
                        .RetryBackOffMs(1)
                        .RetryBehavior(EndRetryBehavior.SKIP)
                        .Build())
                .To("output");

            var config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = $"test-async-retry-count-{Guid.NewGuid()}",
                AsyncProcessingMode = mode
            };

            using var driver = new TopologyTestDriver(builder.Build(), config);
            var input = driver.CreateInputTopic<string, string>("input");
            input.PipeInput("k1", "value");

            Assert.AreEqual(expectedCalls, calls);
        }
    }
}
