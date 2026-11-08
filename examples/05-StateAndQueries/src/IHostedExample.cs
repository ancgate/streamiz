using Streamiz.Examples.Common;

namespace Streamiz.Examples.StateAndQueries;

/// <summary>
/// An example which needs more than running the topology: it queries the stores of the running
/// application (and may serve them over HTTP), so it starts the <c>KafkaStream</c> itself.
/// </summary>
public interface IHostedExample : IExample
{
    Task<int> RunAsync(string[] args, CancellationToken token);
}
