using System.Net.Http.Json;

namespace Streamiz.Examples.AsyncProcessing.Services;

// The external systems called by the examples. Each has a simulated implementation (used by default, so the
// examples run without any service), an HTTP implementation (used when the matching environment variable is set),
// and the tests use their own fakes.

/// <summary>Looks up customer names (e.g. a CRM REST API).</summary>
public interface ICustomerDirectory
{
    Task<string> GetCustomerNameAsync(string customerId);
}

/// <summary>Finds the account id of an e-mail address.</summary>
public interface IAccountLookup
{
    Task<string> FindAccountIdAsync(string email);
}

/// <summary>An external audit log (a database, a SaaS API...).</summary>
public interface IAuditLog
{
    Task WriteAsync(string key, string value);
}

/// <summary>A slow fraud-scoring service.</summary>
public interface IFraudScoring
{
    Task<int> ScoreAsync(string paymentId, string payment);
}

/// <summary>GET {baseUrl}/customers/{id} returning { "name": "..." }.</summary>
public class HttpCustomerDirectory : ICustomerDirectory
{
    private record CustomerDto(string Name);

    private readonly HttpClient client;

    public HttpCustomerDirectory(string baseUrl)
        => client = new HttpClient { BaseAddress = new Uri(baseUrl), Timeout = TimeSpan.FromSeconds(2) };

    public async Task<string> GetCustomerNameAsync(string customerId)
    {
        // a non-success status throws HttpRequestException, which the retry policy of the example retries
        var customer = await client.GetFromJsonAsync<CustomerDto>($"customers/{Uri.EscapeDataString(customerId)}");
        return customer?.Name;
    }
}

public class SimulatedCustomerDirectory : ICustomerDirectory
{
    public async Task<string> GetCustomerNameAsync(string customerId)
    {
        await Task.Delay(20); // network latency
        return $"Customer {customerId.ToUpperInvariant()}";
    }

    /// <summary>CUSTOMER_API_URL set: the HTTP directory, otherwise the simulated one.</summary>
    public static ICustomerDirectory FromEnvironment()
    {
        var url = Environment.GetEnvironmentVariable("CUSTOMER_API_URL");
        return string.IsNullOrEmpty(url) ? new SimulatedCustomerDirectory() : new HttpCustomerDirectory(url);
    }
}

public class SimulatedAccountLookup : IAccountLookup
{
    public async Task<string> FindAccountIdAsync(string email)
    {
        await Task.Delay(20);
        return "acc-" + email.Split('@')[0];
    }
}

public class ConsoleAuditLog : IAuditLog
{
    public Task WriteAsync(string key, string value)
    {
        Console.WriteLine($"[audit] {key} -> {value}");
        return Task.CompletedTask;
    }
}

public class SimulatedFraudScoring : IFraudScoring
{
    public async Task<int> ScoreAsync(string paymentId, string payment)
    {
        await Task.Delay(50);
        return Math.Abs(payment.GetHashCode()) % 100;
    }
}
