using System.Collections.Concurrent;
using Streamiz.Examples.AsyncProcessing.Services;

namespace Streamiz.Examples.AsyncProcessing.Tests;

/// <summary>Fails the first call for each customer in <see cref="FlakyOnce"/>, and every call for <see cref="AlwaysFailing"/>.</summary>
internal class FakeCustomerDirectory : ICustomerDirectory
{
    private readonly ConcurrentDictionary<string, int> calls = new();

    public ISet<string> FlakyOnce { get; } = new HashSet<string>();
    public ISet<string> AlwaysFailing { get; } = new HashSet<string>();

    public int CallsFor(string customerId) => calls.TryGetValue(customerId, out var n) ? n : 0;

    public Task<string> GetCustomerNameAsync(string customerId)
    {
        var n = calls.AddOrUpdate(customerId, 1, (_, c) => c + 1);
        if (AlwaysFailing.Contains(customerId) || (FlakyOnce.Contains(customerId) && n == 1))
            throw new HttpRequestException($"503 Service Unavailable for {customerId}");
        return Task.FromResult(customerId switch
        {
            "c-1" => "Alice",
            "c-2" => "Bob",
            _ => "Unknown"
        });
    }
}

internal class FakeAccountLookup : IAccountLookup
{
    public Task<string> FindAccountIdAsync(string email) => Task.FromResult(email switch
    {
        "alice@example.com" => "acc-42",
        "bob@example.com" => "acc-7",
        _ => "acc-unknown"
    });
}

internal class FakeAuditLog : IAuditLog
{
    private int failuresLeft;

    public FakeAuditLog(int failuresBeforeSuccess = 0) => failuresLeft = failuresBeforeSuccess;

    public ConcurrentQueue<(string Key, string Value)> Entries { get; } = new();

    public Task WriteAsync(string key, string value)
    {
        if (Interlocked.Decrement(ref failuresLeft) >= 0)
            throw new TimeoutException("audit log is busy");
        Entries.Enqueue((key, value));
        return Task.CompletedTask;
    }
}

/// <summary>Slow scoring which records how many calls run at the same time, and the order of the calls per account.</summary>
internal class FakeFraudScoring : IFraudScoring
{
    private int inFlight;
    private int maxInFlight;

    public int MaxInFlight => maxInFlight;

    public ConcurrentQueue<(string Account, string Payment)> Started { get; } = new();

    public async Task<int> ScoreAsync(string paymentId, string payment)
    {
        Started.Enqueue((paymentId, payment));
        var now = Interlocked.Increment(ref inFlight);
        InterlockedMax(ref maxInFlight, now);
        // the first payment of an account is the slowest: a later one must still wait for it (per-key order)
        await Task.Delay(payment.EndsWith("-1") ? 150 : 30);
        Interlocked.Decrement(ref inFlight);
        return payment.Length;
    }

    private static void InterlockedMax(ref int target, int value)
    {
        int current;
        while ((current = Volatile.Read(ref target)) < value &&
               Interlocked.CompareExchange(ref target, value, current) != current)
        {
        }
    }
}
