using Streamiz.Examples.Common;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;

namespace Streamiz.Examples.ErrorHandling.Examples;

/// <summary>
/// With a dead letter queue, the records skipped by a handler are not lost: they are written, unchanged,
/// to the DeadLetterQueueTopicName topic, with headers describing the error
/// (__streams.errors.exception, .message, .stacktrace, .topic, .partition, .offset).
/// You can inspect them, fix the cause and replay them.
/// </summary>
public class DeadLetterQueueExample : IExample
{
    public const string DeadLetterQueueTopic = "orders-dlq";

    public string Name => "dead-letter-queue";
    public string Description => "Sends the skipped records to a dead letter queue topic with the error in headers.";
    // the dead letter queue topic must be created too: Streamiz doesn't create it
    public IEnumerable<string> InputTopics => new[] { "orders", "payments", DeadLetterQueueTopic };

    public void BuildTopology(StreamBuilder builder)
    {
        builder
            // value: "<item>;<quantity>"; a bad quantity throws a FormatException
            .Stream<string, string>("orders")
            .MapValues((id, order, _) =>
            {
                var parts = order.Split(';');
                return $"{int.Parse(parts[1])} x {parts[0]}";
            }, "parse-order")
            .To("parsed-orders");

        // a second input, read with a JSON serdes: bad JSON goes to the dead letter queue too
        builder
            .Stream("payments", new StringSerDes(), new JsonSerDes<Payment>())
            .MapValues((id, payment, _) => $"{payment.Amount} {payment.Currency}")
            .To("payment-amounts");
    }

    public void Configure(StreamConfig config)
    {
        // the topic must exist: Streamiz doesn't create it
        config.DeadLetterQueueTopicName = DeadLetterQueueTopic;

        // the records skipped by these handlers (CONTINUE) are sent to the dead letter queue
        config.ProcessingExceptionHandler = (context, exception) =>
            exception is FormatException ? ExceptionHandlerResponse.CONTINUE : ExceptionHandlerResponse.FAIL;
        config.DeserializationExceptionHandler = (context, record, exception) => ExceptionHandlerResponse.CONTINUE;
    }
}
