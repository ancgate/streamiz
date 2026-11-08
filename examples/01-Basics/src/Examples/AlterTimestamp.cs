using Streamiz.Examples.Common;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;

namespace Streamiz.Examples.Basics;

/// <summary>An invoice, due at a given time.</summary>
public class Invoice
{
    public string Customer { get; set; }
    public double Amount { get; set; }

    /// <summary>Due date (unix milliseconds).</summary>
    public long DueAt { get; set; }
}

/// <summary>
/// AlterRecordTimestamp changes the timestamp of records in the middle of a topology. Here invoices are
/// re-timestamped with their due date, so that a downstream consumer (or a windowed aggregation) groups them
/// by due date instead of the time they were issued.
/// </summary>
public class AlterTimestamp : IExample
{
    public const string InputTopic = "invoices";
    public const string OutputTopic = "invoices-by-due-date";

    public string Name => "alter-record-timestamp";
    public string Description => "Re-timestamp invoices with their due date using AlterRecordTimestamp.";
    public IEnumerable<string> InputTopics => new[] { InputTopic };

    public void BuildTopology(StreamBuilder builder)
    {
        builder.Stream(InputTopic, new StringSerDes(), new JsonSerDes<Invoice>())
            .Filter((id, invoice, _) => invoice.Amount > 0)
            // The function returns the new timestamp (unix milliseconds). A negative value keeps the current one.
            .AlterRecordTimestamp((id, invoice, context) => invoice.DueAt)
            .To(OutputTopic, new StringSerDes(), new JsonSerDes<Invoice>());
    }

    public void Configure(StreamConfig config)
    {
    }
}
