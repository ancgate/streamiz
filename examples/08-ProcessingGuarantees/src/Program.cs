using Streamiz.Examples.Common;
using Streamiz.Examples.ProcessingGuarantees.Examples;

return await ExampleRunner.RunAsync(new IExample[]
{
    new AtLeastOnceExample(),
    new ExactlyOnceExample(),
    new AtMostOnceExample()
}, args);
