using Streamiz.Examples.Common;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;

namespace Streamiz.Examples.Basics;

/// <summary>
/// To with a topic extractor chooses the output topic of each record at runtime.
/// </summary>
public class DynamicRouting : IExample
{
    public const string InputTopic = "site-events";

    public string Name => "dynamic-routing";
    public string Description => "Route each event to a topic named after its type (events-click, events-purchase...).";
    public IEnumerable<string> InputTopics => new[] { InputTopic };

    public void BuildTopology(StreamBuilder builder)
    {
        builder.Stream(InputTopic, new StringSerDes(), new JsonSerDes<SiteEvent>())
            // The topic extractor gets the key, the value and the record context (source topic, headers...).
            // The output topics must exist: Streamiz doesn't create them.
            .To((key, siteEvent, context) => $"events-{siteEvent.Type}",
                new StringSerDes(), new JsonSerDes<SiteEvent>());
    }

    public void Configure(StreamConfig config)
    {
    }
}
