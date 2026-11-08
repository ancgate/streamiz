using Streamiz.Examples.Common;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Examples.Basics;

/// <summary>
/// The classic word count: split sentences into words, group by word and count.
/// The count is a table (KTable): each new occurrence of a word updates its count.
/// </summary>
public class WordCount : IExample
{
    public const string InputTopic = "sentences";
    public const string OutputTopic = "word-counts";
    public const string StoreName = "word-count-store";

    public string Name => "word-count";
    public string Description => "Count the words of a stream of sentences (FlatMapValues, GroupBy, Count, ToStream).";
    public IEnumerable<string> InputTopics => new[] { InputTopic };

    public void BuildTopology(StreamBuilder builder)
    {
        builder.Stream<string, string>(InputTopic)
            // FlatMapValues: one sentence becomes one record per word, with the same key.
            .FlatMapValues((sentence, _) => sentence.ToLowerInvariant()
                .Split(' ', StringSplitOptions.RemoveEmptyEntries))
            // GroupBy changes the key to the word: the records are repartitioned so that all the
            // occurrences of a word are counted by the same task.
            .GroupBy((key, word, _) => word)
            // Count keeps the count of each word in a state store, named so that it can be queried.
            .Count(InMemory.As<string, long>(StoreName))
            // ToStream turns the table into the stream of its updates: one record per new count.
            .ToStream()
            .To(OutputTopic, new StringSerDes(), new Int64SerDes());
    }

    public void Configure(StreamConfig config)
    {
    }
}
