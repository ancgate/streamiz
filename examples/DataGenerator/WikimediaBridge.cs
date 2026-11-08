using System.Text.Json;
using Confluent.Kafka;

namespace Streamiz.Examples.DataGenerator;

/// <summary>
/// Bridges a public real-time stream into Kafka: Wikimedia's "recentchange" event stream (every edit on every
/// Wikimedia wiki, a few dozen events per second), read as Server-Sent Events over HTTP.
/// Each event is produced as-is (JSON), keyed by wiki (e.g. "enwiki"), with the event time (meta.dt) as timestamp.
/// See https://wikitech.wikimedia.org/wiki/Event_Platform/EventStreams for the data and its terms of use.
/// </summary>
public static class WikimediaBridge
{
    public const string Url = "https://stream.wikimedia.org/v2/stream/recentchange";

    public static async Task RunAsync(string topic, long? max, CancellationToken token)
    {
        using var http = new HttpClient { Timeout = Timeout.InfiniteTimeSpan };
        // Wikimedia asks clients to identify themselves with a User-Agent
        http.DefaultRequestHeaders.UserAgent.ParseAdd("StreamizExamples/1.0 (https://github.com/LGouellec/streamiz)");
        http.DefaultRequestHeaders.Accept.ParseAdd("text/event-stream");

        using var producer = Kafka.CreateProducer();
        using var response = await http.GetAsync(Url, HttpCompletionOption.ResponseHeadersRead, token);
        response.EnsureSuccessStatusCode();
        using var reader = new StreamReader(await response.Content.ReadAsStreamAsync(token));

        Console.WriteLine($"Bridging {Url} to '{topic}'. Ctrl+C to stop.");
        long count = 0;
        while (!token.IsCancellationRequested && (max == null || count < max))
        {
            var line = await reader.ReadLineAsync(token);
            if (line == null)
                break;
            // SSE: "event: message", "id: [...]", "data: {json}", and blank lines between events
            if (!line.StartsWith("data: ", StringComparison.Ordinal))
                continue;

            var json = line["data: ".Length..];
            string key;
            DateTime timestamp;
            try
            {
                using var doc = JsonDocument.Parse(json);
                key = doc.RootElement.TryGetProperty("wiki", out var wiki) ? wiki.GetString() : null;
                timestamp = doc.RootElement.GetProperty("meta").GetProperty("dt").GetDateTime().ToUniversalTime();
            }
            catch (Exception)
            {
                continue; // keep-alive or unexpected payload
            }

            producer.Produce(topic, new Message<string, string>
            {
                Key = key,
                Value = json,
                Timestamp = new Timestamp(timestamp, TimestampType.CreateTime)
            });

            if (++count % 500 == 0)
                Console.WriteLine($"{count} events");
        }

        producer.Flush(TimeSpan.FromSeconds(10));
        Console.WriteLine($"{count} events produced to '{topic}'.");
    }
}
