using Streamiz.Examples.Common;
using Streamiz.Examples.RealWorld.Examples;

// End-to-end scenarios on the datasets of examples/data.
// Load a dataset first: dotnet run --project ../../DataGenerator -- load <dataset>
return await ExampleRunner.RunAsync(new IExample[]
{
    new EcommerceRevenue(),
    new OrderPaymentMatching(),
    new FraudDetection(),
    new IotAverages(),
    new IotAlerts(),
    new ClickstreamAnalytics(),
    new StockOhlc(),
}, args);
