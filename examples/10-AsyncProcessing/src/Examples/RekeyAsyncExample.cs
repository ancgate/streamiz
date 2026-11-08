using Streamiz.Examples.AsyncProcessing.Services;
using Streamiz.Examples.Common;
using Streamiz.Kafka.Net;

namespace Streamiz.Examples.AsyncProcessing.Examples;

/// <summary>
/// <c>MapAsync</c>: compute a new key (and value) with an async call.
/// </summary>
public class RekeyAsyncExample : IExample
{
    private readonly IAccountLookup accounts;

    public RekeyAsyncExample() : this(new SimulatedAccountLookup())
    {
    }

    public RekeyAsyncExample(IAccountLookup accounts) => this.accounts = accounts;

    public string Name => "rekey-async";

    public string Description => "Re-keys sign-up events from the e-mail address to the account id returned by a lookup service.";

    public IEnumerable<string> InputTopics => new[] { "signups" };

    public void BuildTopology(StreamBuilder builder)
    {
        // key: e-mail address, value: plan
        builder.Stream<string, string>("signups")
            .MapAsync(async (record, _) =>
            {
                var accountId = await accounts.FindAccountIdAsync(record.Key);
                return KeyValuePair.Create(accountId, record.Value);
            })
            // with the default retry policy: up to 5 attempts for the retriable exceptions (none by default), then SKIP
            .To("signups-by-account");
    }

    public void Configure(StreamConfig config)
    {
    }
}
