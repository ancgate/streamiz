using System.Globalization;
using System.Text;
using Confluent.Kafka;
using Streamiz.Examples.Common;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.Processors.Public;

namespace Streamiz.Examples.ProcessorApi.Examples;

/// <summary>
/// Read and add record headers in a transformer: propagate a trace id and mark the record as processed.
/// </summary>
public class TraceHeadersExample : IExample
{
    public const string OrdersTopic = "orders";
    public const string OutputTopic = "orders-traced";
    public const string TraceIdHeader = "trace-id";
    public const string ProcessedByHeader = "processed-by";

    public string Name => "trace-headers";

    public string Description => "Headers in a transformer: keep the trace-id header (or create one) and add a processed-by header.";

    public IEnumerable<string> InputTopics => new[] { OrdersTopic };

    public void BuildTopology(StreamBuilder builder)
    {
        builder.Stream<string, string>(OrdersTopic)
            .TransformValues(TransformerBuilder.New<string, string, string, string>()
                .Transformer<TraceHeadersTransformer>()
                .Build())
            .To(OutputTopic);
    }

    public void Configure(StreamConfig config)
    {
    }
}

public class TraceHeadersTransformer : ITransformer<string, string, string, string>
{
    public void Init(ProcessorContext<string, string> context)
    {
    }

    public Record<string, string> Process(Record<string, string> record)
    {
        // Copy the incoming headers, so the input record isn't modified.
        var headers = new Headers();
        foreach (var header in record.Headers)
            headers.Add(header.Key, header.GetValueBytes());

        // Records without a trace id get one, derived from their position in the input topic
        // (a real application would typically use a new Guid or its tracing library).
        if (!headers.TryGetLastBytes(TraceHeadersExample.TraceIdHeader, out _))
        {
            var position = record.TopicPartitionOffset;
            var traceId = $"{position.Topic}-{position.Partition.Value}-{position.Offset.Value}";
            headers.Add(TraceHeadersExample.TraceIdHeader, Encoding.UTF8.GetBytes(traceId));
        }

        headers.Add(TraceHeadersExample.ProcessedByHeader, Encoding.UTF8.GetBytes("order-enricher"));

        // The headers of the returned record become the headers of the output record.
        return Record<string, string>.Create(record.Key, record.Value, headers);
    }

    public void Close()
    {
    }
}

/// <summary>
/// Forward to a specific downstream processor by name: route large orders and standard orders to different topics.
/// </summary>
public class RouteToNamedSinksExample : IExample
{
    public const string OrdersTopic = "orders";
    public const string HighValueTopic = "orders-high-value";
    public const string StandardTopic = "orders-standard";
    public const string HighValueSink = "high-value-sink";
    public const string StandardSink = "standard-sink";

    public string Name => "route-to-named-sinks";

    public string Description => "Forward to named child processors: send orders over 1000 to one topic and the others to another.";

    public IEnumerable<string> InputTopics => new[] { OrdersTopic };

    public void BuildTopology(StreamBuilder builder)
    {
        // The router forwards each order itself, to one of its two children, chosen by name.
        var routed = builder.Stream<string, string>(OrdersTopic)
            .Transform(TransformerBuilder.New<string, string, string, string>()
                .Transformer<OrderRouter>()
                .Build());

        // The children of the transformer: two sinks, named so the router can address them.
        routed.To(HighValueTopic, HighValueSink);
        routed.To(StandardTopic, StandardSink);
    }

    public void Configure(StreamConfig config)
    {
    }
}

/// <summary>
/// Orders are keyed by order id, value = amount.
/// </summary>
public class OrderRouter : ITransformer<string, string, string, string>
{
    private ProcessorContext<string, string> context;

    public void Init(ProcessorContext<string, string> context)
    {
        this.context = context;
    }

    public Record<string, string> Process(Record<string, string> record)
    {
        var amount = decimal.Parse(record.Value, CultureInfo.InvariantCulture);
        // Forward(key, value, childName) sends the record to that child only.
        context.Forward(record.Key, record.Value, amount > 1000 ? RouteToNamedSinksExample.HighValueSink : RouteToNamedSinksExample.StandardSink);
        // Returning null: nothing else is forwarded (returning the record would send it to every child).
        return null;
    }

    public void Close()
    {
    }
}
