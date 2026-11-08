using Confluent.Kafka;
using Streamiz.Examples.Common;
using Streamiz.Kafka.Net;

namespace Streamiz.Examples.ErrorHandling.Examples;

/// <summary>
/// The ProductionExceptionHandler decides what to do when Kafka refuses an output record
/// (after the producer's own retries): RETRY it on the next iteration, CONTINUE without it, or FAIL.
/// </summary>
public class ProductionErrorsExample : IExample
{
    public string Name => "production-errors";
    public string Description => "Retries, drops or fails on output records Kafka refuses with a ProductionExceptionHandler.";
    public IEnumerable<string> InputTopics => new[] { "orders" };

    public void BuildTopology(StreamBuilder builder)
    {
        builder
            .Stream<string, string>("orders")
            .MapValues((id, order, _) => order.ToUpperInvariant())
            .To("orders-uppercase");
    }

    public void Configure(StreamConfig config)
    {
        config.ProductionExceptionHandler = Handle;
        // the producer retries transient errors itself before the handler is called
        config.MessageSendMaxRetries = 5;
    }

    /// <summary>The decision for a failed delivery.</summary>
    public static ProductionExceptionHandlerResponse Handle(DeliveryReport<byte[], byte[]> report)
    {
        switch (report.Error.Code)
        {
            // too big for the topic: retrying won't help, drop the record (and log it)
            case ErrorCode.MsgSizeTooLarge:
                Console.WriteLine($"Dropping a record too large for {report.Topic}");
                return ProductionExceptionHandlerResponse.CONTINUE;

            // the broker or the network was unavailable longer than the producer timeout: try again
            case ErrorCode.Local_MsgTimedOut:
            case ErrorCode.RequestTimedOut:
            case ErrorCode.NotEnoughReplicas:
            case ErrorCode.NotEnoughReplicasAfterAppend:
                return ProductionExceptionHandlerResponse.RETRY;

            // authorization, unknown topic, ...: stop and fix the configuration
            default:
                return ProductionExceptionHandlerResponse.FAIL;
        }
    }
}
