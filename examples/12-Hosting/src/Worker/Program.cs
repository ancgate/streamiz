using Streamiz.Examples.Hosting;

// A .NET Generic Host (worker service): configuration (appsettings.json, environment variables, command line),
// logging and dependency injection come from the host, and the stream runs as a hosted service.
var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddOrderTotalsStream(builder.Configuration);

await builder.Build().RunAsync();
