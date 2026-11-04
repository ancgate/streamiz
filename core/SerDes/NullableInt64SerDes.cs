using System;
using Confluent.Kafka;

namespace Streamiz.Kafka.Net.SerDes
{
    /// <summary>
    /// SerDes for nullable <see cref="long"/>: null is serialized as null bytes (a Kafka tombstone),
    /// other values like <see cref="Int64SerDes"/>.
    /// </summary>
    public class NullableInt64SerDes : AbstractSerDes<long?>
    {
        /// <summary>
        /// Deserialize a record value from a byte array into a nullable <see cref="long"/> value
        /// </summary>
        /// <param name="data">serialized bytes, null for a tombstone</param>
        /// <param name="context">serialization context</param>
        /// <returns>deserialized value, or null if <paramref name="data"/> is null</returns>
        public override long? Deserialize(byte[] data, SerializationContext context)
            => data == null ? null : BitConverter.ToInt64(data, 0);

        /// <summary>
        /// Convert a nullable long <code>data</code> into a byte array.
        /// </summary>
        /// <param name="data">value to serialize</param>
        /// <param name="context">serialization context</param>
        /// <returns>serialized bytes, or null if <paramref name="data"/> is null</returns>
        public override byte[] Serialize(long? data, SerializationContext context)
            => data.HasValue ? BitConverter.GetBytes(data.Value) : null;
    }
}
