using Streamiz.Examples.Common;
using Streamiz.Kafka.Net;

namespace Streamiz.Examples.ErrorHandling.Examples;

/// <summary>
/// The InnerExceptionHandler is the last line of defence: it's called for the exceptions which reach the
/// stream thread (including those a ProcessingExceptionHandler returned FAIL for).
/// CONTINUE closes the tasks, rejoins the consumer group and skips the record which failed;
/// FAIL (the default) stops the thread. Prefer the ProcessingExceptionHandler to skip bad records:
/// it doesn't close the tasks nor trigger a rebalance.
/// </summary>
public class InnerExceptionContinueExample : IExample
{
    public string Name => "inner-exception-continue";
    public string Description => "Keeps the stream thread alive when an exception reaches it, skipping the failing record.";
    public IEnumerable<string> InputTopics => new[] { "orders" };

    /// <summary>Number of exceptions which reached the handler (for the tests).</summary>
    public int HandledExceptions;

    public void BuildTopology(StreamBuilder builder)
    {
        builder
            .Stream<string, string>("orders")
            .Peek((id, order, _) =>
            {
                if (order == "poison")
                    throw new InvalidOperationException($"Can't process order {id}");
            })
            .To("orders-checked");
    }

    public void Configure(StreamConfig config)
    {
        config.InnerExceptionHandler = exception =>
        {
            Interlocked.Increment(ref HandledExceptions);
            Console.WriteLine($"The stream thread caught: {exception.Message}");
            return ExceptionHandlerResponse.CONTINUE;
        };
    }
}
