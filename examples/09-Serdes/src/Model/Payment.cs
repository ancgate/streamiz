using Avro;
using Avro.Specific;

namespace Streamiz.Examples.Serdes.Model;

/// <summary>
/// An Avro record. In a real project this class is generated from the .avsc schema
/// (for example with the <c>avrogen</c> tool of Apache.Avro); it's written by hand here to keep the example self-contained.
/// </summary>
public class Payment : ISpecificRecord
{
    public static readonly Schema _SCHEMA = Schema.Parse(@"{
        ""type"": ""record"",
        ""name"": ""Payment"",
        ""namespace"": ""Streamiz.Examples.Serdes.Model"",
        ""fields"": [
            { ""name"": ""id"", ""type"": ""string"" },
            { ""name"": ""account"", ""type"": ""string"" },
            { ""name"": ""amount"", ""type"": ""double"" },
            { ""name"": ""status"", ""type"": ""string"" }
        ]
    }");

    public string id { get; set; }
    public string account { get; set; }
    public double amount { get; set; }
    public string status { get; set; }

    public virtual Schema Schema => _SCHEMA;

    public virtual object Get(int fieldPos) => fieldPos switch
    {
        0 => id,
        1 => account,
        2 => amount,
        3 => status,
        _ => throw new AvroRuntimeException($"Bad index {fieldPos} in Get()")
    };

    public virtual void Put(int fieldPos, object fieldValue)
    {
        switch (fieldPos)
        {
            case 0: id = (string) fieldValue; break;
            case 1: account = (string) fieldValue; break;
            case 2: amount = (double) fieldValue; break;
            case 3: status = (string) fieldValue; break;
            default: throw new AvroRuntimeException($"Bad index {fieldPos} in Put()");
        }
    }
}
