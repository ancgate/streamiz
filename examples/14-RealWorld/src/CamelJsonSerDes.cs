using System.Text.Json;
using Confluent.Kafka;
using Streamiz.Kafka.Net.SerDes;

namespace Streamiz.Examples.RealWorld;

/// <summary>
/// JSON serdes with System.Text.Json and camelCase names: the format of the files in examples/data.
/// A value that isn't valid JSON for <typeparamref name="T"/> throws, so the deserialization exception handler
/// (and the dead letter queue) can deal with it.
/// </summary>
public class CamelJsonSerDes<T> : AbstractSerDes<T> where T : class
{
    public static readonly JsonSerializerOptions Options = new() { PropertyNamingPolicy = JsonNamingPolicy.CamelCase };

    public override byte[] Serialize(T data, SerializationContext context)
        => data == null ? null : JsonSerializer.SerializeToUtf8Bytes(data, Options);

    public override T Deserialize(byte[] data, SerializationContext context)
        => data == null ? null : JsonSerializer.Deserialize<T>(data, Options) ?? throw new JsonException("null JSON value");
}
