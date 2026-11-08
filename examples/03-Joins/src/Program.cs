using Streamiz.Examples.Common;
using Streamiz.Examples.Joins.Examples;

return await ExampleRunner.RunAsync(new IExample[]
{
    new OrdersPaymentsJoinExample(),
    new OrdersPaymentsLeftJoinExample(),
    new OrdersPaymentsOuterJoinExample(),
    new ClicksProfileJoinExample(),
    new ClicksProfileLeftJoinExample(),
    new OrdersCatalogGlobalJoinExample(),
    new CustomerAccountTableJoinsExample(),
    new OrdersProductsForeignKeyJoinExample(),
    new OrdersProductsForeignKeyLeftJoinExample(),
    new PriceAsOfOrderTimeExample(),
    new PriceJoinWithGraceExample(),
    new VersionedTableOutOfOrderExample(),
}, args);
