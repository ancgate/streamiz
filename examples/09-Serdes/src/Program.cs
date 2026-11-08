using Streamiz.Examples.Common;
using Streamiz.Examples.Serdes.Examples;

return await ExampleRunner.RunAsync(new IExample[]
{
    new BuiltInSerdesExample(),
    new NullableSerdesExample(),
    new MaterializedSerdesExample(),
    new CustomJsonSerdesExample(),
    new SchemaRegistryJsonExample(),
    new SchemaRegistryAvroExample(),
    new SchemaRegistryProtobufExample(),
    new CloudEventsExample()
}, args);
