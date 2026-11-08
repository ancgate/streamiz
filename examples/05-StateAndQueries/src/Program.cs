using Streamiz.Examples.Common;
using Streamiz.Examples.StateAndQueries;
using Streamiz.Examples.StateAndQueries.Examples;

IExample[] examples =
{
    new InMemoryStoreExample(),
    new RocksDbStoreExample(),
    new CachingExample(),
    new ChangelogExample(),
    new LocalQueriesExample(),
    new InteractiveQueriesHttpExample(),
    new StandbyReplicasExample()
};

// the examples which query their stores start the stream themselves
var hosted = args.Length > 0
    ? examples.OfType<IHostedExample>().FirstOrDefault(e => e.Name.Equals(args[0], StringComparison.OrdinalIgnoreCase))
    : null;

if (hosted != null)
{
    using var stop = new CancellationTokenSource();
    Console.CancelKeyPress += (_, e) =>
    {
        e.Cancel = true;
        stop.Cancel();
    };
    return await hosted.RunAsync(args, stop.Token);
}

return await ExampleRunner.RunAsync(examples, args);
