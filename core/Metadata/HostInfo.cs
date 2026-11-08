using System;
using Streamiz.Kafka.Net.Errors;

namespace Streamiz.Kafka.Net
{
    /// <summary>
    /// Host and port of a Streamiz instance, as configured with <see cref="IStreamConfig.ApplicationServer"/>.
    /// </summary>
    public sealed class HostInfo : IEquatable<HostInfo>
    {
        /// <summary>
        /// Returned when the host is not known : <see cref="IStreamConfig.ApplicationServer"/> is not set, the metadata is not
        /// available yet (rebalancing, starting), or no instance owns the key or store.
        /// </summary>
        public static readonly HostInfo Unavailable = new("unavailable", -1);

        /// <summary>
        /// Host
        /// </summary>
        public string Host { get; }

        /// <summary>
        /// Port
        /// </summary>
        public int Port { get; }

        /// <summary>
        /// Create a host info
        /// </summary>
        /// <param name="host">host</param>
        /// <param name="port">port</param>
        public HostInfo(string host, int port)
        {
            Host = host;
            Port = port;
        }

        /// <summary>
        /// Parse a "host:port" string (the format of <see cref="IStreamConfig.ApplicationServer"/>).
        /// </summary>
        /// <param name="endpoint">host:port</param>
        /// <returns>the host info, or <see cref="Unavailable"/> if <paramref name="endpoint"/> is null or empty</returns>
        /// <exception cref="StreamConfigException">if <paramref name="endpoint"/> is not a valid host:port pair</exception>
        public static HostInfo Parse(string endpoint)
        {
            if (string.IsNullOrWhiteSpace(endpoint))
                return Unavailable;

            int index = endpoint.LastIndexOf(':');
            if (index <= 0 || index == endpoint.Length - 1 ||
                !int.TryParse(endpoint.Substring(index + 1), out int port) || port < 0 || port > 65535)
                throw new StreamConfigException(
                    $"Invalid application.server '{endpoint}': it must be a host:port pair, e.g. localhost:8080");

            return new HostInfo(endpoint.Substring(0, index), port);
        }

        /// <inheritdoc/>
        public bool Equals(HostInfo other)
            => other != null && Host == other.Host && Port == other.Port;

        /// <inheritdoc/>
        public override bool Equals(object obj) => obj is HostInfo other && Equals(other);

        /// <inheritdoc/>
        public override int GetHashCode() => ((Host?.GetHashCode() ?? 0) * 397) ^ Port;

        /// <inheritdoc/>
        public override string ToString() => $"{Host}:{Port}";
    }
}
