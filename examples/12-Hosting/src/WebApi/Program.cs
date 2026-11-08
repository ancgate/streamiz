using Streamiz.Examples.Hosting;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.State;

// ASP.NET Core minimal API hosting the stream: a health endpoint for the orchestrator
// and an endpoint serving the stream's state store (interactive query).
var builder = WebApplication.CreateBuilder(args);

builder.Services.AddOrderTotalsStream(builder.Configuration);

var app = builder.Build();

// 200 while the stream is RUNNING or REBALANCING, 503 otherwise (use it as the readiness probe)
app.MapGet("/health", (StreamHealth health) =>
{
    var status = health.Current;
    return Results.Json(status, statusCode: status.HttpStatusCode);
});

// total spent by a customer, read from the local state store
app.MapGet("/totals/{customer}", (string customer, StreamizHostedService service) =>
{
    try
    {
        var store = service.Stream?.Store(StoreQueryParameters.FromNameAndType(
            OrderTotalsTopology.StoreName, QueryableStoreTypes.KeyValueStore<string, double>()));
        if (store == null)
            return Results.StatusCode(StatusCodes.Status503ServiceUnavailable);

        // with several instances, a key can live on another instance: see the interactive queries docs
        // (ApplicationServer + KafkaStream.QueryMetadataForKey) to forward the request there
        var total = store.Get(customer);
        return Results.Ok(new { customer, total });
    }
    catch (Exception e) when (e is InvalidStateStoreException or IllegalStateException)
    {
        // the stream is not running yet, or is rebalancing
        return Results.StatusCode(StatusCodes.Status503ServiceUnavailable);
    }
});

app.Run();
