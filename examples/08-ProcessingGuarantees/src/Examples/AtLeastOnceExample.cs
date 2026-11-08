using Streamiz.Examples.Common;
using Streamiz.Kafka.Net;

namespace Streamiz.Examples.ProcessingGuarantees.Examples;

/// <summary>
/// At-least-once (the default): the offsets are committed after the records are processed and their outputs
/// written. After a crash, the records processed since the last commit are processed again: nothing is lost,
/// but an output can be written twice. Make the consumers of the output idempotent (e.g. keyed by an id).
/// </summary>
public class AtLeastOnceExample : IExample
{
    public string Name => "at-least-once";
    public string Description => "Default guarantee: no record lost, an output can be duplicated after a crash.";
    public IEnumerable<string> InputTopics => new[] { "payments" };

    public void BuildTopology(StreamBuilder builder)
    {
        builder
            // key: payment id, value: "<account>;<amount in cents>"
            .Stream<string, string>("payments")
            // the ledger entry keeps the payment id as key: a duplicate after a crash is easy to detect downstream
            .MapValues((paymentId, payment, _) => $"ledger-entry|{paymentId}|{payment}")
            .To("ledger");
    }

    public void Configure(StreamConfig config)
    {
        config.Guarantee = ProcessingGuarantee.AT_LEAST_ONCE;
        // the window of possible duplicates: the records processed since the last commit
        config.CommitIntervalMs = 1000;
    }
}
