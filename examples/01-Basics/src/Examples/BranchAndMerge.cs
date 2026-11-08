using Streamiz.Examples.Common;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;

namespace Streamiz.Examples.Basics;

/// <summary>
/// Branch splits a stream in several streams with predicates, and Merge combines streams back into one.
/// </summary>
public class BranchAndMerge : IExample
{
    public const string InputTopic = "payments";
    public const string CardTopic = "payments-card";
    public const string WalletTopic = "payments-wallet";
    public const string OtherTopic = "payments-other";

    public string Name => "branch-and-merge";
    public string Description => "Branch payments by method, then Merge the digital wallets back together.";
    public IEnumerable<string> InputTopics => new[] { InputTopic };

    public void BuildTopology(StreamBuilder builder)
    {
        var payments = builder.Stream(InputTopic, new StringSerDes(), new JsonSerDes<Payment>());

        // Branch evaluates the predicates in order and sends each record to the first branch that matches.
        // The last predicate catches everything else: without it, unmatched records are dropped.
        var branches = payments.Branch(
            (id, p, _) => p.Method == "card",
            (id, p, _) => p.Method == "paypal",
            (id, p, _) => p.Method == "apple-pay",
            (id, p, _) => true);

        var card = branches[0];
        var paypal = branches[1];
        var applePay = branches[2];
        var other = branches[3];

        card.To(CardTopic, new StringSerDes(), new JsonSerDes<Payment>());
        other.To(OtherTopic, new StringSerDes(), new JsonSerDes<Payment>());

        // Merge combines two streams of the same types into one (records keep their order within each stream).
        paypal.Merge(applePay)
            .To(WalletTopic, new StringSerDes(), new JsonSerDes<Payment>());
    }

    public void Configure(StreamConfig config)
    {
    }
}
