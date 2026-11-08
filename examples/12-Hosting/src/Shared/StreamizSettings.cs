using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;

namespace Streamiz.Examples.Hosting;

/// <summary>
/// The "Streamiz" section of appsettings.json (or environment variables such as Streamiz__BootstrapServers),
/// bound by the configuration system and turned into a <see cref="StreamConfig"/>.
/// </summary>
public class StreamizSettings
{
    public const string Section = "Streamiz";

    public string ApplicationId { get; set; } = "streamiz-hosting-example";
    public string BootstrapServers { get; set; } = "localhost:9092";
    public int NumStreamThreads { get; set; } = 1;
    public ProcessingGuarantee Guarantee { get; set; } = ProcessingGuarantee.AT_LEAST_ONCE;
    public string StateDir { get; set; }

    /// <summary>host:port of this instance, to find which instance holds a key (interactive queries across instances).</summary>
    public string ApplicationServer { get; set; }

    public StreamConfig ToStreamConfig()
    {
        var config = new StreamConfig<StringSerDes, StringSerDes>
        {
            ApplicationId = ApplicationId,
            BootstrapServers = BootstrapServers,
            NumStreamThreads = NumStreamThreads,
            Guarantee = Guarantee,
            AutoOffsetReset = Confluent.Kafka.AutoOffsetReset.Earliest
        };
        if (!string.IsNullOrEmpty(StateDir))
            config.StateDir = StateDir;
        if (!string.IsNullOrEmpty(ApplicationServer))
            config.ApplicationServer = ApplicationServer;
        return config;
    }
}
