using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Examples.Observability.Examples;

/// <summary>
/// The topology the metrics examples measure: a word count with a state store,
/// so there are processor, task, thread and store metrics to export.
/// </summary>
public static class WordCountTopology
{
    public static void Build(StreamBuilder builder)
    {
        builder.Stream<string, string>("sentences")
            .FlatMapValues((sentence, _) => sentence.ToLowerInvariant().Split(' ', StringSplitOptions.RemoveEmptyEntries))
            .GroupBy((_, word, _) => word)
            .Count(InMemory.As<string, long, StringSerDes, Int64SerDes>("word-counts"))
            .ToStream()
            .To<StringSerDes, Int64SerDes>("word-counts");
    }
}
