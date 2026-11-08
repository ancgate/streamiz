using Streamiz.Examples.AsyncProcessing.Services;
using Streamiz.Examples.Common;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.Stream;

namespace Streamiz.Examples.AsyncProcessing.Examples;

/// <summary>
/// <c>MapValuesAsync</c>: enrich each record by calling an external service, with a retry policy.
/// </summary>
public class EnrichWithHttpExample : IExample
{
    private readonly ICustomerDirectory directory;

    public EnrichWithHttpExample() : this(SimulatedCustomerDirectory.FromEnvironment())
    {
    }

    public EnrichWithHttpExample(ICustomerDirectory directory) => this.directory = directory;

    public string Name => "enrich-with-http";

    public string Description => "Adds the customer name to each order by calling a customer API, retrying transient failures.";

    public IEnumerable<string> InputTopics => new[] { "orders" };

    public void BuildTopology(StreamBuilder builder)
    {
        // key: order id, value: customer id
        builder.Stream<string, string>("orders")
            .MapValuesAsync(
                async (record, context) =>
                {
                    // record.Key, record.Value, record.Headers and record.Timestamp are the source record's;
                    // context.RetryNumber says how many times this record was already retried
                    var name = await directory.GetCustomerNameAsync(record.Value);
                    return $"{record.Value}:{name}";
                },
                RetryPolicy.NewBuilder()
                    // only these exceptions are retried; any other one fails the record at once
                    .RetriableException<HttpRequestException>()
                    .RetriableException<TaskCanceledException>() // the HttpClient timeout
                    .NumberOfRetry(3) // attempts in total, the first call included (0 would never call the function)
                    .RetryBackOffMs(200)
                    // when the retries are exhausted: SKIP drops the record (FAIL stops processing, BUFFERED keeps retrying)
                    .RetryBehavior(EndRetryBehavior.SKIP)
                    .Build())
            .To("orders-enriched");
    }

    public void Configure(StreamConfig config)
    {
    }
}
