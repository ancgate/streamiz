using Streamiz.Examples.Common;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.Stream;

namespace Streamiz.Examples.ErrorHandling.Examples;

/// <summary>
/// Calls an external service for each record with a retry policy. When the retries are exhausted,
/// EndRetryBehavior.SKIP drops the record, and the dead letter queue keeps it.
/// See the 10-AsyncProcessing group for the async processing engines.
/// </summary>
public class AsyncRetryDeadLetterQueueExample : IExample
{
    public const string DeadLetterQueueTopic = "payments-dlq";

    private readonly Func<string, Task<string>> fraudCheck;

    /// <param name="fraudCheck">The external call; by default a fake service which fails for "unreachable" values.</param>
    public AsyncRetryDeadLetterQueueExample(Func<string, Task<string>> fraudCheck = null)
    {
        this.fraudCheck = fraudCheck ?? FakeFraudCheckService;
    }

    public string Name => "async-retry-dlq";
    public string Description => "Retries an external call, then sends the record to the dead letter queue.";
    public IEnumerable<string> InputTopics => new[] { "payments", DeadLetterQueueTopic };

    public void BuildTopology(StreamBuilder builder)
    {
        builder
            .Stream<string, string>("payments")
            .MapValuesAsync(
                async (record, context) => await fraudCheck(record.Value),
                RetryPolicy.NewBuilder()
                    // only these exceptions are retried; the others fail the record right away
                    .RetriableException<HttpRequestException>()
                    .NumberOfRetry(2)
                    .RetryBackOffMs(10)
                    // after the last retry: drop the record (and send it to the dead letter queue)
                    .RetryBehavior(EndRetryBehavior.SKIP)
                    .Build())
            .To("payments-checked");
    }

    public void Configure(StreamConfig config)
    {
        config.DeadLetterQueueTopicName = DeadLetterQueueTopic;
        // the in-task engine: the calls run concurrently inside the stream task
        config.AsyncProcessingMode = AsyncProcessingMode.V2;
    }

    private static Task<string> FakeFraudCheckService(string payment)
    {
        if (payment.Contains("unreachable"))
            throw new HttpRequestException("fraud-check service unavailable");
        return Task.FromResult($"{payment}:ok");
    }
}
