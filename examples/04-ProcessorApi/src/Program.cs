using Streamiz.Examples.Common;
using Streamiz.Examples.ProcessorApi.Examples;

return await ExampleRunner.RunAsync(new IExample[]
{
    new MaskCardNumbersExample(),
    new RekeyByCountryExample(),
    new DeduplicateEventsExample(),
    new CountPerMinuteExample(),
    new SilentSensorAlertExample(),
    new CustomStateStoreExample(),
    new CurrencyRatesGlobalStoreExample(),
    new GlobalTableLookupExample(),
    new TraceHeadersExample(),
    new RouteToNamedSinksExample(),
}, args);
