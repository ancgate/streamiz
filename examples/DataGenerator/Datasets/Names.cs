namespace Streamiz.Examples.DataGenerator.Datasets;

/// <summary>Small built-in name lists (no external dependency).</summary>
public static class Names
{
    private static readonly string[] First =
    {
        "Alice", "Bob", "Chloe", "David", "Emma", "Felix", "Grace", "Hugo", "Ines", "Jules", "Karim", "Lea",
        "Malik", "Nina", "Oscar", "Paula", "Quentin", "Rosa", "Samir", "Tara", "Ugo", "Vera", "Willem", "Xenia",
        "Yanis", "Zoe", "Amir", "Brigitte", "Carlos", "Dalia", "Elias", "Fatou", "Gustav", "Hana", "Ivan", "Julia"
    };

    private static readonly string[] Last =
    {
        "Martin", "Bernard", "Dubois", "Schmidt", "Muller", "Smith", "Johnson", "Garcia", "Rossi", "Lopez",
        "Brown", "Wilson", "Moreau", "Fischer", "Weber", "Taylor", "Clark", "Silva", "Costa", "Novak",
        "Jensen", "Kowalski", "Nguyen", "Tanaka", "Ivanova", "Haddad", "Okafor", "Larsen", "Romano", "Petit"
    };

    public static (string First, string Last) Person(Rng rng) => (rng.Pick(First), rng.Pick(Last));
}
