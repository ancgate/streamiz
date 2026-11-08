using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;

namespace Streamiz.Examples.Common;

/// <summary>
/// Lists the examples of a group, or runs one of them against Kafka until Ctrl+C.
/// The cluster address comes from the KAFKA_BOOTSTRAP environment variable (default localhost:9092,
/// the address of environment/docker-compose.yml).
/// </summary>
public static class ExampleRunner
{
    public static string BootstrapServers =>
        Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP") ?? "localhost:9092";

    /// <summary>
    /// Default configuration of an example: string keys and values, an application id derived from the
    /// example name, and the cluster address. The example's <see cref="IExample.Configure"/> then adjusts it.
    /// </summary>
    public static StreamConfig CreateConfig(IExample example)
    {
        var config = new StreamConfig<StringSerDes, StringSerDes>
        {
            ApplicationId = $"streamiz-example-{example.Name}",
            BootstrapServers = BootstrapServers,
            AutoOffsetReset = Confluent.Kafka.AutoOffsetReset.Earliest
        };
        example.Configure(config);
        return config;
    }

    public static async Task<int> RunAsync(IReadOnlyCollection<IExample> examples, string[] args)
    {
        var example = args.Length > 0
            ? examples.FirstOrDefault(e => e.Name.Equals(args[0], StringComparison.OrdinalIgnoreCase))
            : null;

        if (example == null)
        {
            Console.WriteLine("Usage: dotnet run -- <example>");
            Console.WriteLine();
            foreach (var e in examples)
                Console.WriteLine($"  {e.Name,-32} {e.Description}");
            return args.Length == 0 ? 0 : 1;
        }

        var builder = new StreamBuilder();
        example.BuildTopology(builder);
        var topology = builder.Build();
        var config = CreateConfig(example);

        Console.WriteLine($"Running '{example.Name}' against {config.BootstrapServers}");
        Console.WriteLine($"Input topics (create them first): {string.Join(", ", example.InputTopics)}");
        Console.WriteLine(topology.Describe());

        using var stream = new KafkaStream(topology, config);
        using var stop = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true;
            stop.Cancel();
        };

        await stream.StartAsync(stop.Token);
        try
        {
            await Task.Delay(Timeout.Infinite, stop.Token);
        }
        catch (TaskCanceledException)
        {
        }

        return 0;
    }
}
