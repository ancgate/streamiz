using Streamiz.Examples.Common;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Examples.Windowing;

/// <summary>
/// Suppress UntilWindowClose on session windows: emit each visit once, when it's over, with its final count.
/// The intermediate sessions and their tombstones are never emitted.
/// </summary>
public class SessionFinalResults : IExample
{
    public const string InputTopic = "page-views-final";
    public const string OutputTopic = "visits-final";
    public static readonly TimeSpan InactivityGap = TimeSpan.FromMinutes(5);

    public string Name => "session-final-results";
    public string Description => "Emit each user visit once, when it's over (session windows + Suppress UntilWindowClose).";
    public IEnumerable<string> InputTopics => new[] { InputTopic };

    public void BuildTopology(StreamBuilder builder)
    {
        var sessionKeySerdes = new SessionWindowedSerDes<string>(new StringSerDes());

        builder.Stream<string, string>(InputTopic)
            .GroupByKey()
            .WindowedBy(SessionWindowOptions.Of(InactivityGap))
            .Count(InMemorySessions.As<string, long?>("visits-final-store"))
            // A session can still be extended by a record up to the inactivity gap after its end, so it is
            // closed at end + gap + grace: pass gap + grace (here no grace) as the suppress grace period.
            .Suppress(SuppressedBuilder.UntilWindowClose<Windowed<string>, long?>(InactivityGap, StrictBufferConfig.Unbounded())
                .WithKeySerdes(sessionKeySerdes)
                .WithValueSerdes(new NullableInt64SerDes()))
            .ToStream()
            .To(OutputTopic, sessionKeySerdes, new NullableInt64SerDes());
    }

    public void Configure(StreamConfig config)
    {
    }
}
