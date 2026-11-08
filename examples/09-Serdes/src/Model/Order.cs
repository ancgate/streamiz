namespace Streamiz.Examples.Serdes.Model;

/// <summary>
/// A plain C# object used by the JSON examples (custom serdes, Schema Registry JSON, CloudEvents).
/// </summary>
public class Order
{
    public string Id { get; set; }
    public string Customer { get; set; }
    public double Amount { get; set; }
    public string Currency { get; set; }
}
