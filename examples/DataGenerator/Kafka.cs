using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Streamiz.Examples.Common;

namespace Streamiz.Examples.DataGenerator;

/// <summary>Kafka clients used by the commands (KAFKA_BOOTSTRAP, default localhost:9092).</summary>
public static class Kafka
{
    public static IProducer<string, string> CreateProducer()
        => new ProducerBuilder<string, string>(new ProducerConfig
        {
            BootstrapServers = ExampleRunner.BootstrapServers,
            // the same partitioner as Streamiz (and the Java clients): a key always goes to the same partition,
            // in the topics produced here and in the topics written by Streamiz, so joins find their matches
            Partitioner = Partitioner.Murmur2Random,
            LingerMs = 20,
            EnableIdempotence = true
        }).Build();

    public static async Task CreateTopicsAsync(IEnumerable<string> topics, int partitions)
    {
        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = ExampleRunner.BootstrapServers }).Build();
        var existing = admin.GetMetadata(TimeSpan.FromSeconds(10)).Topics.Select(t => t.Topic).ToHashSet();
        var specs = topics.Distinct()
            .Where(t => !existing.Contains(t))
            .Select(t => new TopicSpecification { Name = t, NumPartitions = partitions, ReplicationFactor = 1 })
            .ToList();

        if (specs.Count == 0)
        {
            Console.WriteLine("All the topics already exist.");
            return;
        }

        try
        {
            await admin.CreateTopicsAsync(specs);
        }
        catch (CreateTopicsException e)
        {
            foreach (var result in e.Results.Where(r => r.Error.Code != ErrorCode.NoError && r.Error.Code != ErrorCode.TopicAlreadyExists))
                Console.Error.WriteLine($"Failed to create {result.Topic}: {result.Error.Reason}");
        }

        foreach (var spec in specs)
            Console.WriteLine($"Created {spec.Name} ({partitions} partitions)");
    }
}
