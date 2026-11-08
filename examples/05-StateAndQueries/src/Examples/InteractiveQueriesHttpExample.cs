using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Streamiz.Examples.Common;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Examples.StateAndQueries.Examples;

/// <summary>
/// Interactive queries across instances: each instance serves <c>GET /count/{page}</c>.
/// When the page is hosted by another instance, it answers with a redirect to that instance.
/// <c>ApplicationServer</c> tells the other instances where this one answers: Streamiz publishes it,
/// with the partitions of the instance, in the internal topic <c>&lt;application.id&gt;-streamiz-metadata</c>.
/// </summary>
public class InteractiveQueriesHttpExample : IHostedExample
{
    public const string StoreName = "views-per-page";

    public string Name => "interactive-queries-http";
    public string Description => "Serves the page counts of every instance over HTTP with ApplicationServer and QueryMetadataForKey.";
    public IEnumerable<string> InputTopics => new[] { "page-views" };

    /// <summary>host:port of this instance (APPLICATION_SERVER, default localhost:7070).</summary>
    public static string ApplicationServer =>
        Environment.GetEnvironmentVariable("APPLICATION_SERVER") ?? "localhost:7070";

    public void BuildTopology(StreamBuilder builder)
    {
        builder
            .Stream<string, string>("page-views")
            .GroupBy((user, page, _) => page)
            .Count(InMemory.As<string, long>(StoreName).WithValueSerdes(new Int64SerDes()));
    }

    public void Configure(StreamConfig config)
    {
        // the endpoint of this instance, published to the other instances
        config.ApplicationServer = ApplicationServer;
        // several instances run on the same machine in this example: one state directory each
        config.StateDir = Path.Combine(Path.GetTempPath(), "streamiz-examples", Name, ApplicationServer.Replace(':', '-'));
    }

    public async Task<int> RunAsync(string[] args, CancellationToken token)
    {
        var builder = new StreamBuilder();
        BuildTopology(builder);
        var config = ExampleRunner.CreateConfig(this);
        var self = HostInfo.Parse(config.ApplicationServer);

        using var stream = new KafkaStream(builder.Build(), config);
        var router = new CountQueryRouter(new KafkaStreamCountBackend(stream, StoreName), self);

        var web = WebApplication.CreateBuilder();
        var app = web.Build();
        app.Urls.Add($"http://{self.Host}:{self.Port}");

        // GET /count/home -> 12, or 307 to the instance which hosts "home"
        app.MapGet("/count/{page}", (string page) =>
        {
            var result = router.Query(page);
            if (result.Unavailable)
                return Results.StatusCode(StatusCodes.Status503ServiceUnavailable);
            if (result.RedirectUrl != null)
                return Results.Redirect(result.RedirectUrl, permanent: false, preserveMethod: true);
            return Results.Ok(new { page, views = result.Count });
        });

        // GET /instances -> the instances of the application and their partitions
        app.MapGet("/instances", () => stream.AllMetadataForStore(StoreName)
            .Select(m => new
            {
                host = m.HostInfo.ToString(),
                partitions = m.TopicPartitions.Select(tp => tp.ToString())
            }));

        await stream.StartAsync(token);
        Console.WriteLine($"Try: curl -L http://{self}/count/home");

        // serve until Ctrl+C
        await app.StartAsync(token);
        try
        {
            await Task.Delay(Timeout.Infinite, token);
        }
        catch (TaskCanceledException)
        {
        }

        await app.StopAsync();
        return 0;
    }
}
