using System.Diagnostics;
using System.Text.Json;
using Confluent.Kafka;

namespace Streamiz.Examples.DataGenerator;

/// <summary>A dataset record ready to produce: topic, key, raw JSON value and event time.</summary>
public record DataRecord(string Topic, string Key, string Value, DateTime Timestamp);

/// <summary>
/// Reads a dataset's files and produces them to Kafka. The record timestamp is the event time of the record.
/// Files are in arrival order (the iot readings are deliberately out of order), so the records of a topic are
/// produced in file order, and the topics of a dataset are interleaved by time.
/// </summary>
public static class Loader
{
    public static List<DataRecord> Read(string dataDir, DatasetSpec dataset)
    {
        var sequences = new List<List<(DateTime ArrivalKey, DataRecord Record)>>();
        foreach (var spec in dataset.Files)
        {
            var files = Directory.GetFiles(Path.Combine(dataDir, dataset.Name), spec.Pattern)
                .OrderBy(f => f, StringComparer.Ordinal);
            var sequence = new List<(DateTime, DataRecord)>();
            var runningMax = DateTime.MinValue;
            foreach (var line in files.SelectMany(File.ReadLines).Where(l => l.Length > 0))
            {
                var record = Parse(line, spec);
                // the "arrival time" of a record is the latest event time seen so far in its topic:
                // a late record keeps its place in the file instead of moving back in time
                runningMax = record.Timestamp > runningMax ? record.Timestamp : runningMax;
                sequence.Add((runningMax, record));
            }

            sequences.Add(sequence);
        }

        // merge the topics by arrival time (stable: same-time records keep their file order)
        return sequences
            .SelectMany((seq, fileIndex) => seq.Select((r, i) => (r.ArrivalKey, fileIndex, i, r.Record)))
            .OrderBy(r => r.ArrivalKey).ThenBy(r => r.fileIndex).ThenBy(r => r.i)
            .Select(r => r.Record)
            .ToList();
    }

    public static DataRecord Parse(string line, FileSpec spec)
    {
        using var doc = JsonDocument.Parse(line);
        var root = doc.RootElement;
        var key = root.GetProperty(spec.KeyField).GetString();
        var timestamp = root.GetProperty(spec.TimestampField).GetDateTime().ToUniversalTime();
        var value = spec.Wrapped ? root.GetProperty("value").GetString() : line;
        return new DataRecord(spec.Topic, key, value, timestamp);
    }

    /// <param name="speedup">null: as fast as possible; otherwise replays the original time gaps divided by this factor</param>
    public static async Task LoadAsync(string dataDir, DatasetSpec dataset, double? speedup)
    {
        var records = Read(dataDir, dataset);
        Console.WriteLine($"Producing {records.Count} records of '{dataset.Name}' to {string.Join(", ", records.Select(r => r.Topic).Distinct())}" +
                          (speedup.HasValue ? $" (replay x{speedup})" : " (as fast as possible)"));

        using var producer = Kafka.CreateProducer();
        var errors = 0;
        var clock = Stopwatch.StartNew();
        var first = records.Count > 0 ? records[0].Timestamp : DateTime.MinValue;
        var latest = first;

        foreach (var record in records)
        {
            if (speedup.HasValue && record.Timestamp > latest)
            {
                latest = record.Timestamp;
                var due = TimeSpan.FromTicks((long)((latest - first).Ticks / speedup.Value));
                var wait = due - clock.Elapsed;
                if (wait > TimeSpan.Zero)
                    await Task.Delay(wait);
            }

            producer.Produce(record.Topic, new Message<string, string>
            {
                Key = record.Key,
                Value = record.Value,
                Timestamp = new Timestamp(record.Timestamp, TimestampType.CreateTime)
            }, report =>
            {
                if (report.Error.IsError)
                    Interlocked.Increment(ref errors);
            });
        }

        producer.Flush(TimeSpan.FromSeconds(30));
        Console.WriteLine($"Done in {clock.Elapsed.TotalSeconds:F1}s, {errors} errors.");
    }
}
