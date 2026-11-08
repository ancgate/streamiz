using Streamiz.Examples.Common;
using Streamiz.Kafka.Net;

namespace Streamiz.Examples.ErrorHandling.Examples;

/// <summary>
/// When your code throws while processing one record, the ProcessingExceptionHandler decides:
/// CONTINUE skips the record and processes the next ones (without stopping or rebalancing),
/// FAIL (the default) raises the exception to the stream thread.
/// The handler receives the context of the record: topic, partition, offset, raw key and value, processor.
/// </summary>
public class ProcessingErrorsExample : IExample
{
    public string Name => "processing-errors";
    public string Description => "Skips the records your code can't process with a ProcessingExceptionHandler.";
    public IEnumerable<string> InputTopics => new[] { "orders" };

    /// <summary>The records skipped by the handler (for the tests; log them in a real application).</summary>
    public List<ErrorHandlerContext> Skipped { get; } = new();

    public void BuildTopology(StreamBuilder builder)
    {
        builder
            // value: "<item>;<quantity>", e.g. "keyboard;2"
            .Stream<string, string>("orders")
            // int.Parse throws a FormatException on "keyboard;two"; "parse-order" names the processor
            .MapValues((id, order, _) =>
            {
                var parts = order.Split(';');
                return $"{int.Parse(parts[1])} x {parts[0]}";
            }, "parse-order")
            .To("parsed-orders");
    }

    public void Configure(StreamConfig config)
    {
        config.ProcessingExceptionHandler = (context, exception) =>
        {
            // a bad record: skip it
            if (exception is FormatException or IndexOutOfRangeException)
            {
                Skipped.Add(context);
                Console.WriteLine($"Skipping {context.Topic}[{context.Partition}]@{context.Offset} " +
                                  $"in processor {context.ProcessorNodeId}: {exception.Message}");
                return ExceptionHandlerResponse.CONTINUE;
            }

            // anything else is a bug or an outage: stop, the record is processed again after a restart
            return ExceptionHandlerResponse.FAIL;
        };
    }
}
