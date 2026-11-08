using Streamiz.Examples.Common;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;

namespace Streamiz.Examples.ErrorHandling.Examples;

public class Payment
{
    public string Id { get; set; }
    public decimal Amount { get; set; }
    public string Currency { get; set; }
}

/// <summary>
/// A record which can't be deserialized (here: invalid JSON) fails the stream thread by default.
/// The DeserializationExceptionHandler can skip it instead (CONTINUE): the record is logged and dropped.
/// </summary>
public class SkipBadJsonExample : IExample
{
    public string Name => "skip-bad-json";
    public string Description => "Skips the records which can't be deserialized with a DeserializationExceptionHandler.";
    public IEnumerable<string> InputTopics => new[] { "payments" };

    /// <summary>Records skipped by the handler (for the tests; log them in a real application).</summary>
    public List<string> Skipped { get; } = new();

    public void BuildTopology(StreamBuilder builder)
    {
        builder
            // the values are JSON payments: JsonSerDes deserializes them, and throws on invalid JSON
            .Stream("payments", new StringSerDes(), new JsonSerDes<Payment>())
            .MapValues((id, payment, _) => $"{payment.Amount} {payment.Currency}")
            .To("payment-amounts");
    }

    public void Configure(StreamConfig config)
    {
        // called for each record that can't be deserialized
        config.DeserializationExceptionHandler = (context, record, exception) =>
        {
            Skipped.Add($"{record.Topic}[{record.Partition.Value}]@{record.Offset.Value}: {exception.GetType().Name}");
            Console.WriteLine($"Skipping a bad record at offset {record.Offset}: {exception.Message}");
            // CONTINUE drops the record and processes the next one; FAIL (the default) stops the thread
            return ExceptionHandlerResponse.CONTINUE;
        };
    }
}
