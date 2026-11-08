using Streamiz.Kafka.Net;

namespace Streamiz.Examples.Hosting;

/// <summary>
/// Tracks the state of the KafkaStream (from its StateChanged event) for health checks.
/// </summary>
public class StreamHealth
{
    public record Status(string State, bool Healthy, int HttpStatusCode, string Description);

    private volatile KafkaStream.State state = KafkaStream.State.CREATED;

    public KafkaStream.State State => state;

    public void OnStateChanged(KafkaStream.State oldState, KafkaStream.State newState) => state = newState;

    public Status Current => Evaluate(state);

    /// <summary>
    /// RUNNING and REBALANCING are healthy (a rebalance is normal and short); anything else is not ready (503),
    /// so an orchestrator stops routing queries to this instance, or restarts it after ERROR.
    /// </summary>
    public static Status Evaluate(KafkaStream.State state) => state.ToString() switch
    {
        "RUNNING" => new Status("RUNNING", true, 200, "processing records"),
        "REBALANCING" => new Status("REBALANCING", true, 200, "partitions are being assigned"),
        "CREATED" => new Status("CREATED", false, 503, "starting"),
        "PENDING_SHUTDOWN" => new Status("PENDING_SHUTDOWN", false, 503, "stopping"),
        "NOT_RUNNING" => new Status("NOT_RUNNING", false, 503, "stopped"),
        _ => new Status(state.ToString(), false, 503, "a stream thread failed")
    };
}
