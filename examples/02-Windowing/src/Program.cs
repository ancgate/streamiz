using Streamiz.Examples.Common;
using Streamiz.Examples.Windowing;

return await ExampleRunner.RunAsync(new IExample[]
{
    new TumblingClicksPerMinute(),
    new HoppingAverageTemperature(),
    new SlidingFailedLogins(),
    new SessionCount(),
    new SessionCartTotal(),
    new SessionLastPage(),
    new TumblingFinalResults(),
    new SessionFinalResults(),
    new RateLimitStreamTime(),
    new RateLimitWallClock(),
    new LateRecordsAndGrace(),
    new ReadWindowedResults(),
}, args);
