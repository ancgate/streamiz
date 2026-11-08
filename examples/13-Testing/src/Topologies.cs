using System.Text;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Examples.Testing;

// The topologies tested in the tests/ project. Exposing a Build(StreamBuilder) method (and the topic and store
// names as constants) is what makes a topology easy to test: the test builds it and runs it in a TopologyTestDriver.

/// <summary>Stateless: routes orders to a topic chosen from the "priority" header, and upper-cases them.</summary>
public static class OrderRouting
{
    public const string Input = "orders";
    public const string Express = "orders-express";
    public const string Standard = "orders-standard";

    public static void Build(StreamBuilder builder)
    {
        builder.Stream<string, string>(Input)
            .MapValues((order, _) => order.ToUpperInvariant())
            .To((_, _, context) =>
            {
                var priority = context.Headers.TryGetLastBytes("priority", out var bytes)
                    ? Encoding.UTF8.GetString(bytes)
                    : "normal";
                return priority == "high" ? Express : Standard;
            });
    }
}

/// <summary>Stateful: the running total of the amounts of each account, in a key-value store.</summary>
public static class RunningTotals
{
    public const string Input = "deposits";
    public const string Output = "balances";
    public const string Store = "balances-store";

    public static void Build(StreamBuilder builder)
    {
        builder.Stream<string, long, StringSerDes, Int64SerDes>(Input)
            .GroupByKey()
            .Aggregate(() => 0L, (_, amount, total) => total + amount,
                InMemory.As<string, long, StringSerDes, Int64SerDes>(Store))
            .ToStream()
            .To<StringSerDes, Int64SerDes>(Output);
    }
}

/// <summary>Windows and time: clicks per page and per minute, emitted once the minute is over (suppress).</summary>
public static class ClicksPerMinute
{
    public const string Input = "clicks";
    public const string Output = "clicks-per-minute";
    public const string Store = "clicks-store";
    public static readonly TimeSpan Window = TimeSpan.FromMinutes(1);

    public static TimeWindowedSerDes<string> WindowedKeySerDes =>
        new(new StringSerDes(), (long) Window.TotalMilliseconds);

    public static void Build(StreamBuilder builder)
    {
        builder.Stream<string, string>(Input) // key: page
            .GroupByKey()
            .WindowedBy(TumblingWindowOptions.Of(Window, TimeSpan.Zero))
            .Count(InMemoryWindows.As<string, long>(Store)
                .WithKeySerdes(new StringSerDes())
                .WithValueSerdes(new Int64SerDes()))
            // one final result per window, when stream time passes the end of the window
            .Suppress(SuppressedBuilder.UntilWindowClose<Windowed<string>, long>(TimeSpan.Zero, StrictBufferConfig.Unbounded())
                .WithKeySerdes(WindowedKeySerDes)
                .WithValueSerdes(new Int64SerDes()))
            .ToStream()
            .To(Output, WindowedKeySerDes, new Int64SerDes());
    }
}

/// <summary>Session windows: user activity sessions (5 minutes of inactivity close a session).</summary>
public static class UserSessions
{
    public const string Input = "user-activity";
    public const string Store = "sessions-store";

    public static void Build(StreamBuilder builder)
    {
        builder.Stream<string, string>(Input) // key: user, value: action
            .GroupByKey()
            .WindowedBy(SessionWindowOptions.Of(TimeSpan.FromMinutes(5)))
            .Count(InMemorySessions.As<string, long?>(Store)
                .WithKeySerdes(new StringSerDes())
                .WithValueSerdes(new NullableInt64SerDes()));
    }
}

/// <summary>Versioned table: an order is priced with the price that was valid at the order's timestamp.</summary>
public static class PriceAtOrderTime
{
    public const string Prices = "prices";
    public const string Orders = "orders-to-price";
    public const string Output = "priced-orders";
    public const string Store = "prices-store";

    public static void Build(StreamBuilder builder)
    {
        // key: product, value: price; the store keeps 1 hour of price history
        var prices = builder.Table(Prices, InMemoryVersioned.As<string, string>(Store, TimeSpan.FromHours(1)));

        // key: product, value: quantity; joined with the price as of the order's timestamp
        builder.Stream<string, string>(Orders)
            .Join(prices, (quantity, price) => $"{quantity} x {price}")
            .To(Output);
    }
}

/// <summary>Wall-clock time: an alert is emitted 30 s (of real time) after its first occurrence, with its latest value.</summary>
public static class AlertDebouncer
{
    public const string Input = "raw-alerts";
    public const string Output = "debounced-alerts";
    public static readonly TimeSpan Delay = TimeSpan.FromSeconds(30);

    public static void Build(StreamBuilder builder)
    {
        builder.Table(Input, InMemory.As<string, string>("raw-alerts-store"))
            .Suppress(SuppressedBuilder.UntilWallClockTimeLimit<string, string>(Delay, StrictBufferConfig.Unbounded())
                .WithKeySerdes(new StringSerDes())
                .WithValueSerdes(new StringSerDes()))
            .ToStream()
            .To(Output);
    }
}

/// <summary>Error handling: unparsable amounts are skipped and sent to a dead letter queue.</summary>
public static class SafeAmountParser
{
    public const string Input = "raw-amounts";
    public const string Output = "amounts";
    public const string DeadLetterQueue = "raw-amounts-dlq";

    public static void Build(StreamBuilder builder)
    {
        builder.Stream<string, string>(Input)
            .MapValues((raw, _) => decimal.Parse(raw, System.Globalization.CultureInfo.InvariantCulture) * 100)
            .MapValues((cents, _) => ((long) cents).ToString())
            .To(Output);
    }

    public static void Configure(StreamConfig config)
    {
        // a record whose processing throws is skipped (CONTINUE) instead of stopping the stream thread...
        config.ProcessingExceptionHandler = (_, _) => ExceptionHandlerResponse.CONTINUE;
        // ...and written to this topic, with the error in __streams.errors.* headers
        config.DeadLetterQueueTopicName = DeadLetterQueue;
    }
}
