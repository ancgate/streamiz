using Streamiz.Examples.AsyncProcessing.Examples;
using Streamiz.Examples.Common;

return await ExampleRunner.RunAsync(new IExample[]
{
    new EnrichWithHttpExample(),
    new RekeyAsyncExample(),
    new AuditSinkExample(),
    new RetryToDeadLetterQueueExample(),
    new ParallelV2Example()
}, args);
