using System.Text.Json;
using Confluent.Kafka;
using Streamiz.Kafka.Net.SerDes;

namespace Streamiz.Examples.Serdes.Examples;

/// <summary>
/// A custom serdes: JSON with System.Text.Json, without Schema Registry.
/// Derive from <see cref="AbstractSerDes{T}"/> and implement the two methods; a null object is a Kafka null.
/// </summary>
public class SystemTextJsonSerDes<T> : AbstractSerDes<T> where T : class
{
    private static readonly JsonSerializerOptions options = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase
    };

    public override byte[] Serialize(T data, SerializationContext context)
        => data == null ? null : JsonSerializer.SerializeToUtf8Bytes(data, options);

    public override T Deserialize(byte[] data, SerializationContext context)
        => data == null ? null : JsonSerializer.Deserialize<T>(data, options);
}
