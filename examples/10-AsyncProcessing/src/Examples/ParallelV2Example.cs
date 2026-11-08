using Streamiz.Examples.AsyncProcessing.Services;
using Streamiz.Examples.Common;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.Stream;

namespace Streamiz.Examples.AsyncProcessing.Examples;

/// <summary>
/// The V2 engine: async calls run concurrently inside the stream task, up to MaxConcurrency at a time,
/// while records with the same key are still processed one after the other.
/// </summary>
public class ParallelV2Example : IExample
{
    private readonly IFraudScoring scoring;

    public ParallelV2Example() : this(new SimulatedFraudScoring())
    {
    }

    public ParallelV2Example(IFraudScoring scoring) => this.scoring = scoring;

    public string Name => "parallel-v2";

    public string Description => "Scores payments with a slow service, 16 calls at a time, keeping the order of each account's payments.";

    public IEnumerable<string> InputTopics => new[] { "payments-to-score" };

    public void BuildTopology(StreamBuilder builder)
    {
        // key: account id -> the payments of one account are scored in order, different accounts in parallel
        builder.Stream<string, string>("payments-to-score")
            .MapValuesAsync(
                async (record, _) => $"{record.Value}:score={await scoring.ScoreAsync(record.Key, record.Value)}",
                RetryPolicy.NewBuilder()
                    .MaxConcurrency(16) // calls in flight per async processor and per task (V2 only)
                    .Build())
            .To("payments-scored");
    }

    public void Configure(StreamConfig config)
    {
        // V1 (default): one external thread runs the calls one by one through internal request/response topics.
        // V2: the calls run inside the stream task, concurrently, without the request topic.
        config.AsyncProcessingMode = AsyncProcessingMode.V2;
    }
}
