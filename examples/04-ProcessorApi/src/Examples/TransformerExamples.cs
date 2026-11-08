using Streamiz.Examples.Common;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.Processors.Public;

namespace Streamiz.Examples.ProcessorApi.Examples;

/// <summary>
/// TransformValues with an inline transformer: mask the card number of each payment.
/// </summary>
public class MaskCardNumbersExample : IExample
{
    public const string PaymentsTopic = "payments";
    public const string OutputTopic = "payments-masked";

    public string Name => "mask-card-numbers";

    public string Description => "TransformValues with an inline transformer: mask the card number of each payment, keep the key and headers.";

    public IEnumerable<string> InputTopics => new[] { PaymentsTopic };

    public void BuildTopology(StreamBuilder builder)
    {
        builder.Stream<string, string>(PaymentsTopic)
            // TransformValues: the key can't change, so no repartitioning is needed downstream.
            // TransformerBuilder.New<K, V, K1, V1>() + .Transformer(func) wraps a lambda in a transformer.
            .TransformValues(TransformerBuilder.New<string, string, string, string>()
                // The returned record replaces the current one. Pass record.Headers along to keep the headers:
                // a record created without headers clears them.
                .Transformer(record => Record<string, string>.Create(Mask(record.Value), record.Headers))
                .Build())
            .To(OutputTopic);
    }

    // "card=4111111111111111;amount=10.00" -> "card=************1111;amount=10.00"
    internal static string Mask(string payment)
    {
        var fields = payment.Split(';').Select(field =>
        {
            if (!field.StartsWith("card="))
                return field;
            var number = field.Substring("card=".Length);
            return "card=" + new string('*', Math.Max(0, number.Length - 4)) + number[^Math.Min(4, number.Length)..];
        });
        return string.Join(';', fields);
    }

    public void Configure(StreamConfig config)
    {
    }
}

/// <summary>
/// Transform with a transformer class: change the key of each event (user id -> country).
/// </summary>
public class RekeyByCountryExample : IExample
{
    public const string EventsTopic = "user-events";
    public const string OutputTopic = "events-by-country";

    public string Name => "rekey-by-country";

    public string Description => "Transform with a transformer class: re-key user events by country and keep the user id in the value.";

    public IEnumerable<string> InputTopics => new[] { EventsTopic };

    public void BuildTopology(StreamBuilder builder)
    {
        builder.Stream<string, string>(EventsTopic)
            // Transform can change the key (and its type): a later key-based operation (groupBy, join...)
            // would repartition the stream. The transformer class is instantiated once per task.
            .Transform(TransformerBuilder.New<string, string, string, string>()
                .Transformer<CountryKeyTransformer>()
                .Build())
            .To(OutputTopic);
    }

    public void Configure(StreamConfig config)
    {
    }
}

/// <summary>
/// Input: key = user id, value = "country;action". Output: key = country, value = "userId:action".
/// </summary>
public class CountryKeyTransformer : ITransformer<string, string, string, string>
{
    public void Init(ProcessorContext<string, string> context)
    {
        // Keep the context to read the task id, schedule punctuations, or forward extra records.
    }

    public Record<string, string> Process(Record<string, string> record)
    {
        var parts = record.Value.Split(';');
        return Record<string, string>.Create(parts[0], $"{record.Key}:{parts[1]}", record.Headers);
    }

    public void Close()
    {
    }
}
