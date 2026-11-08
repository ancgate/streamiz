using Streamiz.Examples.Basics;
using Streamiz.Examples.Common;

return await ExampleRunner.RunAsync(new IExample[]
{
    new FilterAndTransform(),
    new FlatMapOrderLines(),
    new BranchAndMerge(),
    new DynamicRouting(),
    new WordCount(),
    new TotalPerCustomer(),
    new TableBasics(),
    new TableAggregate(),
    new EventTimeExtractor(),
    new AlterTimestamp(),
    new ExplicitRepartition(),
    new DropDuplicates(),
}, args);
