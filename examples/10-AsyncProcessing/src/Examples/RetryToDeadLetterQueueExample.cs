using Streamiz.Examples.AsyncProcessing.Services;
using Streamiz.Examples.Common;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.Stream;

namespace Streamiz.Examples.AsyncProcessing.Examples;

/// <summary>
/// Records whose call keeps failing after the retries go to a dead letter queue (EndRetryBehavior.SKIP + DeadLetterQueueTopicName).
/// </summary>
public class RetryToDeadLetterQueueExample : IExample
{
    private readonly ICustomerDirectory directory;

    public RetryToDeadLetterQueueExample() : this(SimulatedCustomerDirectory.FromEnvironment())
    {
    }

    public RetryToDeadLetterQueueExample(ICustomerDirectory directory) => this.directory = directory;

    public string Name => "retry-to-dead-letter-queue";

    public string Description => "Tries a failing lookup twice, then sends the order to a dead letter queue topic instead of losing it.";

    public IEnumerable<string> InputTopics => new[] { "orders-to-enrich" };

    public void BuildTopology(StreamBuilder builder)
    {
        builder.Stream<string, string>("orders-to-enrich")
            .MapValuesAsync(
                async (record, _) => $"{record.Value}:{await directory.GetCustomerNameAsync(record.Value)}",
                RetryPolicy.NewBuilder()
                    .RetriableException<HttpRequestException>()
                    .NumberOfRetry(1) // retries after the first call: 2 calls in total
                    .RetryBackOffMs(10)
                    .RetryBehavior(EndRetryBehavior.SKIP) // skipped records go to the dead letter queue below
                    .Build())
            .To("orders-enriched-or-dlq");
    }

    public void Configure(StreamConfig config)
    {
        // the original record (raw key, value and headers) + __streams.errors.* headers describing the error
        config.DeadLetterQueueTopicName = "orders-dead-letter";
        // the in-task engine (V2) also sends skipped records to the dead letter queue
        config.AsyncProcessingMode = AsyncProcessingMode.V2;
    }
}
