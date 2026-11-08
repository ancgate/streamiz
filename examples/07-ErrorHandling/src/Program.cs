using Streamiz.Examples.Common;
using Streamiz.Examples.ErrorHandling.Examples;

return await ExampleRunner.RunAsync(new IExample[]
{
    new SkipBadJsonExample(),
    new ProcessingErrorsExample(),
    new DeadLetterQueueExample(),
    new ProductionErrorsExample(),
    new InnerExceptionContinueExample(),
    new AsyncRetryDeadLetterQueueExample()
}, args);
