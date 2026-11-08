using Streamiz.Examples.Common;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Examples.Observability.Examples;

/// <summary>
/// Lifecycle and restoration events of a <see cref="KafkaStream"/>: StateChanged (CREATED, REBALANCING, RUNNING,
/// PENDING_SHUTDOWN, NOT_RUNNING, ERROR) and the OnRestore* events fired while state stores are restored from their changelogs.
/// </summary>
public class StreamListenersExample : IExample
{
    public string Name => "stream-listeners";

    public string Description => "Prints the state changes of the stream and the restoration progress of its store.";

    public IEnumerable<string> InputTopics => new[] { "user-profiles" };

    public void BuildTopology(StreamBuilder builder)
    {
        // a table with a logged store: restarting the example restores it from its changelog topic
        builder.Table("user-profiles", RocksDb.As<string, string>("user-profiles-store"));
    }

    public void Configure(StreamConfig config)
    {
    }

    /// <summary>Subscribes to the events of <paramref name="stream"/> and writes them with <paramref name="log"/>.</summary>
    public static void Attach(KafkaStream stream, Action<string> log)
    {
        stream.StateChanged += (oldState, newState) => log(DescribeStateChange(oldState, newState));
        stream.OnRestoreStartEvent += (partition, store, start, end) =>
            log($"restoring {store} from {partition}: offsets {start} to {end}");
        stream.OnRestoreBatchEvent += (partition, store, batchEnd, restored) =>
            log($"restoring {store} from {partition}: {restored} records, up to offset {batchEnd}");
        stream.OnRestoreEndEvent += (partition, store, total) =>
            log($"restored {store} from {partition}: {total} records");
    }

    public static string DescribeStateChange(KafkaStream.State oldState, KafkaStream.State newState)
    {
        var meaning = newState.ToString() switch
        {
            "REBALANCING" => "partitions are being (re)assigned, stores may be restoring",
            "RUNNING" => "processing records",
            "PENDING_SHUTDOWN" => "closing",
            "NOT_RUNNING" => "stopped",
            "ERROR" => "a stream thread died: check the logs",
            _ => ""
        };
        return $"state {oldState} -> {newState} {meaning}".TrimEnd();
    }

    /// <summary>
    /// The runner of the other examples creates the KafkaStream itself; this one needs it to subscribe to the events.
    /// </summary>
    public async Task RunAsync()
    {
        var builder = new StreamBuilder();
        BuildTopology(builder);
        var config = ExampleRunner.CreateConfig(this);

        using var stream = new KafkaStream(builder.Build(), config);
        Attach(stream, Console.WriteLine);

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
    }
}
