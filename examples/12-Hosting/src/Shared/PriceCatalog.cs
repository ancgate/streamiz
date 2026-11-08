using Microsoft.Extensions.Configuration;

namespace Streamiz.Examples.Hosting;

/// <summary>A service resolved from dependency injection and used inside the topology.</summary>
public interface IPriceCatalog
{
    double PriceOf(string sku);
}

/// <summary>Prices read from the "Prices" section of the configuration (sku -> price).</summary>
public class ConfigurationPriceCatalog : IPriceCatalog
{
    private readonly IReadOnlyDictionary<string, double> prices;

    public ConfigurationPriceCatalog(IConfiguration configuration)
        => prices = configuration.GetSection("Prices").Get<Dictionary<string, double>>() ?? new Dictionary<string, double>();

    public double PriceOf(string sku) => prices.TryGetValue(sku, out var price) ? price : 0;
}
