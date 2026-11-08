using Streamiz.Examples.Common;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Examples.Joins.Examples;

/// <summary>
/// Stream-table inner join: enrich each click with the profile of its user.
/// </summary>
public class ClicksProfileJoinExample : IExample
{
    public const string ProfilesTopic = "user-profiles";
    public const string ClicksTopic = "page-clicks";
    public const string OutputTopic = "enriched-clicks";

    public string Name => "clicks-profile-join";

    public string Description => "Stream-table join: enrich each page click with the latest profile of its user.";

    public IEnumerable<string> InputTopics => new[] { ProfilesTopic, ClicksTopic };

    public void BuildTopology(StreamBuilder builder)
    {
        // The table holds the latest profile of each user ("name;country"), keyed by user id.
        // It is materialized in a state store so the join can look the profile up.
        var profiles = builder.Table(ProfilesTopic, InMemory.As<string, string>("user-profiles-store"));

        // Clicks are keyed by user id too (value: the page). Both topics must be co-partitioned.
        builder.Stream<string, string>(ClicksTopic)
            // For each click, the current profile of the user is looked up in the table.
            // Clicks of unknown users are dropped by an inner join. Table updates don't produce output.
            .Join(profiles, (page, profile) =>
            {
                var parts = profile.Split(';');
                return $"{parts[0]} ({parts[1]}) visited {page}";
            })
            .To(OutputTopic);
    }

    public void Configure(StreamConfig config)
    {
    }
}

/// <summary>
/// Stream-table left join: clicks of unknown users are kept, as anonymous clicks.
/// </summary>
public class ClicksProfileLeftJoinExample : IExample
{
    public const string ProfilesTopic = "user-profiles";
    public const string ClicksTopic = "page-clicks";
    public const string OutputTopic = "enriched-clicks";

    public string Name => "clicks-profile-left-join";

    public string Description => "Stream-table left join: keep the clicks of users without a profile, as anonymous clicks.";

    public IEnumerable<string> InputTopics => new[] { ProfilesTopic, ClicksTopic };

    public void BuildTopology(StreamBuilder builder)
    {
        var profiles = builder.Table(ProfilesTopic, InMemory.As<string, string>("user-profiles-store"));

        builder.Stream<string, string>(ClicksTopic)
            // LeftJoin: the joiner is called with a null profile when the user isn't in the table.
            .LeftJoin(profiles, (page, profile) =>
                profile == null
                    ? $"anonymous visited {page}"
                    : $"{profile.Split(';')[0]} visited {page}")
            .To(OutputTopic);
    }

    public void Configure(StreamConfig config)
    {
    }
}
