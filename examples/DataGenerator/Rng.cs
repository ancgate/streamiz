namespace Streamiz.Examples.DataGenerator;

/// <summary>
/// Seeded random helpers. System.Random with a seed returns the same sequence on every run, so the generated
/// files are reproducible.
/// </summary>
public class Rng
{
    private readonly Random random;

    public Rng(int seed) => random = new Random(seed);

    public int Int(int minInclusive, int maxExclusive) => random.Next(minInclusive, maxExclusive);

    public double Double() => random.NextDouble();

    public double Double(double min, double max) => min + random.NextDouble() * (max - min);

    public bool Chance(double probability) => random.NextDouble() < probability;

    public T Pick<T>(IReadOnlyList<T> items) => items[random.Next(items.Count)];

    /// <summary>Picks an index with the given weights.</summary>
    public int Weighted(IReadOnlyList<double> weights)
    {
        var total = weights.Sum();
        var r = random.NextDouble() * total;
        for (var i = 0; i < weights.Count; ++i)
        {
            r -= weights[i];
            if (r < 0)
                return i;
        }

        return weights.Count - 1;
    }

    /// <summary>Standard normal value (Box-Muller).</summary>
    public double Gaussian()
    {
        var u1 = 1.0 - random.NextDouble();
        var u2 = random.NextDouble();
        return Math.Sqrt(-2.0 * Math.Log(u1)) * Math.Cos(2.0 * Math.PI * u2);
    }

    /// <summary>Skewed index in [0, count): low indexes are picked more often (popular products, frequent customers).</summary>
    public int Skewed(int count, double power = 2.0) => Math.Min(count - 1, (int)(Math.Pow(random.NextDouble(), power) * count));

    public TimeSpan Seconds(int min, int maxExclusive) => TimeSpan.FromSeconds(random.Next(min, maxExclusive));
}
