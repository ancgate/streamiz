using Streamiz.Examples.AsyncProcessing.Services;
using Streamiz.Examples.Common;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.Stream;

namespace Streamiz.Examples.AsyncProcessing.Examples;

/// <summary>
/// <c>ForeachAsync</c>: a terminal operation writing each record to an external system.
/// </summary>
public class AuditSinkExample : IExample
{
    private readonly IAuditLog auditLog;

    public AuditSinkExample() : this(new ConsoleAuditLog())
    {
    }

    public AuditSinkExample(IAuditLog auditLog) => this.auditLog = auditLog;

    public string Name => "audit-sink";

    public string Description => "Writes every payment to an external audit log, retrying a busy log a few times.";

    public IEnumerable<string> InputTopics => new[] { "payments" };

    public void BuildTopology(StreamBuilder builder)
    {
        builder.Stream<string, string>("payments")
            .ForeachAsync(
                (record, _) => auditLog.WriteAsync(record.Key, record.Value),
                RetryPolicy.NewBuilder()
                    .RetriableException<TimeoutException>()
                    .NumberOfRetry(5) // attempts in total
                    .RetryBackOffMs(100)
                    .Build());
    }

    public void Configure(StreamConfig config)
    {
    }
}
