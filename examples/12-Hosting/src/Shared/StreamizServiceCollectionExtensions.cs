using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Streamiz.Examples.Hosting;

public static class StreamizServiceCollectionExtensions
{
    /// <summary>
    /// Registers the settings, the topology and its dependencies, the health tracker and the hosted service.
    /// </summary>
    public static IServiceCollection AddOrderTotalsStream(this IServiceCollection services, IConfiguration configuration)
    {
        var settings = configuration.GetSection(StreamizSettings.Section).Get<StreamizSettings>() ?? new StreamizSettings();

        services.AddSingleton(settings);
        services.AddSingleton<IPriceCatalog, ConfigurationPriceCatalog>();
        services.AddSingleton<OrderTotalsTopology>();
        services.AddSingleton<StreamHealth>();
        // registered once, as itself (to reach the stream) and as a hosted service (to run it)
        services.AddSingleton<StreamizHostedService>();
        services.AddHostedService(provider => provider.GetRequiredService<StreamizHostedService>());
        return services;
    }
}
