namespace Streamiz.Examples.Basics;

// Values of the examples, serialized as JSON with Streamiz's JsonSerDes<T>.

/// <summary>An order placed by a customer.</summary>
public class Order
{
    public string Customer { get; set; }
    public string Product { get; set; }
    public int Quantity { get; set; }
    public double UnitPrice { get; set; }

    public double Total() => Quantity * UnitPrice;
}

/// <summary>One line of a shopping cart.</summary>
public class OrderLine
{
    public string Product { get; set; }
    public int Quantity { get; set; }
}

/// <summary>An order made of several lines.</summary>
public class MultiLineOrder
{
    public string Customer { get; set; }
    public List<OrderLine> Lines { get; set; } = new();
}

/// <summary>A payment, with the method used to pay.</summary>
public class Payment
{
    public string Method { get; set; }
    public double Amount { get; set; }
}

/// <summary>An event of the web site, routed by its type.</summary>
public class SiteEvent
{
    public string Type { get; set; }
    public string User { get; set; }
}

/// <summary>A measure sent by a sensor, with the time it was taken.</summary>
public class SensorReading
{
    public string Sensor { get; set; }
    public double Value { get; set; }

    /// <summary>When the measure was taken (unix milliseconds), which can be long before Kafka receives it.</summary>
    public long MeasuredAt { get; set; }
}

/// <summary>A bank account, kept in a table: its branch and balance change over time.</summary>
public class Account
{
    public string Branch { get; set; }
    public double Balance { get; set; }
}
