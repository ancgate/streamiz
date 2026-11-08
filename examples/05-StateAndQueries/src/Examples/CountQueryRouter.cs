using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;

namespace Streamiz.Examples.StateAndQueries.Examples;

/// <summary>Where a key is hosted: the instance of its active task and its partition.</summary>
public record KeyLocation(HostInfo Host, int Partition);

/// <summary>
/// What the router needs from the running application. <see cref="KafkaStreamCountBackend"/> implements it
/// with <c>KafkaStream</c>; the tests use a fake.
/// </summary>
public interface ICountBackend
{
    /// <summary>The instance and the partition of a key (<c>KafkaStream.QueryMetadataForKey</c>).</summary>
    KeyLocation Locate(string key);

    /// <summary>The count of a key in the local store of its partition.</summary>
    long GetLocal(string key, int partition);
}

/// <summary>Result of a query: the count, or the instance to ask, or "not available yet".</summary>
public record CountQueryResult(long? Count, string RedirectUrl, bool Unavailable)
{
    public static CountQueryResult Found(long count) => new(count, null, false);
    public static CountQueryResult Redirect(string url) => new(null, url, false);
    public static readonly CountQueryResult NotAvailable = new(null, null, true);
}

/// <summary>
/// Answers "how many views for this page?" on any instance: from the local store when this instance hosts
/// the page, otherwise by sending the caller to the instance which hosts it.
/// </summary>
public class CountQueryRouter
{
    private readonly ICountBackend backend;
    private readonly HostInfo self;

    public CountQueryRouter(ICountBackend backend, HostInfo self)
    {
        this.backend = backend;
        this.self = self;
    }

    public CountQueryResult Query(string page)
    {
        var location = backend.Locate(page);

        // during a rebalance, or before the metadata of the instances is known
        if (location.Host.Equals(HostInfo.Unavailable))
            return CountQueryResult.NotAvailable;

        // hosted here: read only the store of the key's partition
        if (location.Host.Equals(self))
            return CountQueryResult.Found(backend.GetLocal(page, location.Partition));

        // hosted by another instance: the caller asks it (it could also be proxied with an HttpClient)
        return CountQueryResult.Redirect($"http://{location.Host.Host}:{location.Host.Port}/count/{Uri.EscapeDataString(page)}");
    }
}

/// <summary><see cref="ICountBackend"/> on a running <c>KafkaStream</c>.</summary>
public class KafkaStreamCountBackend : ICountBackend
{
    private readonly KafkaStream stream;
    private readonly string storeName;

    public KafkaStreamCountBackend(KafkaStream stream, string storeName)
    {
        this.stream = stream;
        this.storeName = storeName;
    }

    public KeyLocation Locate(string key)
    {
        // the partition of the key is computed like the producer does (murmur2 by default)
        var metadata = stream.QueryMetadataForKey(storeName, key, new StringSerDes());
        return new KeyLocation(metadata.ActiveHost, metadata.Partition);
    }

    public long GetLocal(string key, int partition)
    {
        var store = stream.Store(StoreQueryParameters
            .FromNameAndType(storeName, QueryableStoreTypes.KeyValueStore<string, long>())
            // only the store of this partition, instead of every local store
            .WithPartition(partition));
        return store.Get(key);
    }
}
