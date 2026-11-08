using Confluent.Kafka;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.State.Internal;
using Streamiz.Kafka.Net.Stream;

namespace Streamiz.Kafka.Net.SerDes
{
    /// <summary>
    /// Serdes of session windowed keys (<see cref="Windowed{K}"/> with a <see cref="SessionWindow"/>).
    /// The binary format is the same as Kafka Streams: <c>key | endTime (8 bytes) | startTime (8 bytes)</c>, big-endian.
    /// </summary>
    /// <typeparam name="T">type of the inner key</typeparam>
    public class SessionWindowedSerDes<T> : AbstractSerDes<Windowed<T>>
    {
        private readonly ISerDes<T> innerSerdes;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="innerSerdes">serdes of the inner key</param>
        public SessionWindowedSerDes(ISerDes<T> innerSerdes)
        {
            this.innerSerdes = innerSerdes;
        }

        /// <summary>
        /// Deserialize a session windowed key
        /// </summary>
        /// <param name="data">serialized bytes</param>
        /// <param name="context">serialization context</param>
        /// <returns>the session windowed key, null if <paramref name="data"/> is null or empty</returns>
        public override Windowed<T> Deserialize(byte[] data, SerializationContext context)
        {
            if (data == null || data.Length == 0)
                return null;

            return new Windowed<T>(
                innerSerdes.Deserialize(SessionKeySchema.ExtractKeyBytes(data), context),
                SessionKeySchema.ExtractWindow(data));
        }

        /// <summary>
        /// Serialize a session windowed key
        /// </summary>
        /// <param name="data">session windowed key</param>
        /// <param name="context">serialization context</param>
        /// <returns>the serialized bytes, null if <paramref name="data"/> is null</returns>
        public override byte[] Serialize(Windowed<T> data, SerializationContext context)
        {
            if (data == null)
                return null;

            return SessionKeySchema.ToBinary(innerSerdes.Serialize(data.Key, context), data.Window.StartMs, data.Window.EndMs);
        }

        /// <summary>
        /// Initialize the inner serdes
        /// </summary>
        /// <param name="context">serdes context</param>
        public override void Initialize(SerDesContext context)
        {
            base.Initialize(context);
            innerSerdes.Initialize(context);
        }
    }
}
