using System.Globalization;
using Streamiz.Examples.DataGenerator;

// datagen: generates the sample datasets of examples/data, loads them into Kafka, produces live events,
// creates the topics, and bridges a public real-time stream. Run without arguments for the usage.

using var stop = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    stop.Cancel();
};

string Option(string name) => args.SkipWhile(a => a != name).Skip(1).FirstOrDefault();
bool Flag(string name) => args.Contains(name);
double DoubleOption(string name, double defaultValue)
    => Option(name) is { } v ? double.Parse(v, CultureInfo.InvariantCulture) : defaultValue;

var dataDir = Option("--data") ?? (args.Length > 0 && args[0] != "bridge" && args[0] != "live" ? Catalog.FindDataDir() : null);

switch (args.FirstOrDefault())
{
    case "generate-files":
        Catalog.GenerateAll(dataDir);
        Console.WriteLine($"Generated the datasets in {dataDir} (seed {Catalog.Seed})");
        foreach (var file in Directory.GetFiles(dataDir, "*.jsonl", SearchOption.AllDirectories).OrderBy(f => f, StringComparer.Ordinal))
            Console.WriteLine($"  {Path.GetRelativePath(dataDir, file),-45} {File.ReadLines(file).Count(),7} records {new FileInfo(file).Length / 1024,6} KB");
        return 0;

    case "load" when args.Length > 1:
        var dataset = Catalog.Find(args[1]);
        double? speedup = Flag("--as-fast-as-possible") || Option("--speedup") == null ? null : DoubleOption("--speedup", 1);
        await Loader.LoadAsync(dataDir, dataset, speedup);
        return 0;

    case "live" when args.Length > 1:
        var duration = Option("--duration") is { } d ? TimeSpan.FromSeconds(double.Parse(d, CultureInfo.InvariantCulture)) : (TimeSpan?)null;
        await Live.RunAsync(args[1], new LiveOptions(
            DoubleOption("--rate", 10), DoubleOption("--late", 0), DoubleOption("--out-of-order", 0), DoubleOption("--bad", 0), duration), stop.Token);
        return 0;

    case "create-topics":
        var topics = Catalog.Datasets.SelectMany(ds => ds.Files.Select(f => f.Topic)).ToList();
        if (Flag("--with-outputs"))
            topics.AddRange(Catalog.OutputTopics);
        await Kafka.CreateTopicsAsync(topics, (int)DoubleOption("--partitions", 3));
        return 0;

    case "bridge" when args.Length > 1 && args[1] == "wikimedia":
        await WikimediaBridge.RunAsync(Option("--topic") ?? "wikimedia.recentchange",
            Option("--max") is { } max ? long.Parse(max, CultureInfo.InvariantCulture) : null, stop.Token);
        return 0;

    default:
        Console.WriteLine("""
            datagen: sample data for the Streamiz examples (Kafka at KAFKA_BOOTSTRAP, default localhost:9092)

              generate-files                         regenerate examples/data (deterministic)
              create-topics [--partitions N] [--with-outputs]
                                                     create the dataset topics (and the 14-RealWorld output topics)
              load <dataset> [--speedup N | --as-fast-as-possible]
                                                     produce a dataset with its event times as record timestamps;
                                                     --speedup replays the original time gaps N times faster
              live <dataset> [--rate N] [--late P] [--out-of-order P] [--bad P] [--duration S]
                                                     produce new events continuously (P: ratio between 0 and 1)
              bridge wikimedia [--topic T] [--max N] produce Wikimedia's live recent changes (internet access)

            datasets:
            """);
        foreach (var ds in Catalog.Datasets)
            Console.WriteLine($"  {ds.Name,-12} {ds.Description}");
        return args.Length == 0 ? 0 : 1;
}
