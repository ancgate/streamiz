namespace Streamiz.Examples.Windowing;

/// <summary>Running sum and count, to compute an average in a windowed aggregation.</summary>
public class SumCount
{
    public double Sum { get; set; }
    public long Count { get; set; }

    public double Average() => Count == 0 ? 0 : Sum / Count;
}

/// <summary>What a user put in their cart during one session.</summary>
public class CartSummary
{
    public List<string> Items { get; set; } = new();
    public double Total { get; set; }
}

/// <summary>An item added to a cart.</summary>
public class CartItem
{
    public string Product { get; set; }
    public double Price { get; set; }
}
