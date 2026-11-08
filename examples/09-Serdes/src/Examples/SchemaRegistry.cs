namespace Streamiz.Examples.Serdes.Examples;

/// <summary>
/// Schema Registry settings shared by the Schema Registry examples.
/// </summary>
public static class SchemaRegistry
{
    /// <summary>
    /// SCHEMA_REGISTRY_URL, default http://localhost:8081 (the registry of environment/docker-compose.yml).
    /// Tests use a "mock://scope" url: the serdes then use an in-memory registry, no server needed.
    /// </summary>
    public static string Url => Environment.GetEnvironmentVariable("SCHEMA_REGISTRY_URL") ?? "http://localhost:8081";
}
