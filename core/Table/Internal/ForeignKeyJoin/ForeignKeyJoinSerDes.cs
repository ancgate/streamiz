using System;
using Confluent.Kafka;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.SerDes;

namespace Streamiz.Kafka.Net.Table.Internal.ForeignKeyJoin
{
    internal static class ForeignKeyJoinSerDesHelper
    {
        internal const byte NULL_HASH_FLAG = 0x80;

        // the inner serdes default to the ones of the configuration when they are not set
        internal static ISerDes<T> ResolveKeySerDes<T>(ISerDes<T> serdes, SerDesContext context, string description)
            => Resolve(serdes, context?.Config?.DefaultKeySerDes, context, description);

        internal static ISerDes<T> ResolveValueSerDes<T>(ISerDes<T> serdes, SerDesContext context, string description)
            => Resolve(serdes, context?.Config?.DefaultValueSerDes, context, description);

        private static ISerDes<T> Resolve<T>(ISerDes<T> serdes, ISerDes defaultSerDes, SerDesContext context, string description)
        {
            if (serdes == null && defaultSerDes is ISerDes<T> des)
                serdes = des;

            if (serdes == null)
                throw new StreamsException(
                    $"The {description} serdes of the foreign-key join is not set and the default serdes of the configuration is not a ISerDes<{typeof(T).Name}>. Please set it explicitly.");

            if (context != null)
                serdes.Initialize(context);
            return serdes;
        }

        internal static void PutHash(ByteBuffer buffer, long[] hash)
        {
            if (hash != null)
                buffer.PutLong(hash[0]).PutLong(hash[1]);
        }
    }

    /// <summary>
    /// Wire format, big-endian:
    /// <c>version (1 byte, high bit set if the hash is null) | instruction (1 byte) | hash (16 bytes, if not null) |
    /// primary partition (4 bytes) | serialized primary key</c>.
    /// </summary>
    internal class SubscriptionWrapperSerDes<K> : AbstractSerDes<SubscriptionWrapper<K>>
    {
        private readonly string primaryKeySerdesTopic;
        private ISerDes<K> primaryKeySerdes;

        public SubscriptionWrapperSerDes(string primaryKeySerdesTopic, ISerDes<K> primaryKeySerdes)
        {
            this.primaryKeySerdesTopic = primaryKeySerdesTopic;
            this.primaryKeySerdes = primaryKeySerdes;
        }

        public override void Initialize(SerDesContext context)
        {
            primaryKeySerdes = ForeignKeyJoinSerDesHelper.ResolveKeySerDes(primaryKeySerdes, context, "primary key");
            base.Initialize(context);
        }

        public override byte[] Serialize(SubscriptionWrapper<K> data, SerializationContext context)
        {
            if (data == null)
                return null;

            var primaryKey = primaryKeySerdes.Serialize(data.PrimaryKey,
                new SerializationContext(MessageComponentType.Key, primaryKeySerdesTopic, context.Headers));

            using var buffer = ByteBuffer.Build(2 + (data.Hash == null ? 0 : 16) + 4 + (primaryKey?.Length ?? 0), true);
            buffer.Put(data.Hash == null
                    ? (byte) (data.Version | ForeignKeyJoinSerDesHelper.NULL_HASH_FLAG)
                    : data.Version)
                .Put((byte) data.Instruction);
            ForeignKeyJoinSerDesHelper.PutHash(buffer, data.Hash);
            return buffer.PutInt(data.PrimaryPartition)
                .Put(primaryKey)
                .ToArray();
        }

        public override SubscriptionWrapper<K> Deserialize(byte[] data, SerializationContext context)
        {
            if (data == null)
                return null;

            using var buffer = ByteBuffer.Build(data, true);
            byte versionAndFlag = buffer.GetBytes(1)[0];
            byte version = (byte) (versionAndFlag & ~ForeignKeyJoinSerDesHelper.NULL_HASH_FLAG);
            if (version > SubscriptionWrapper<K>.CURRENT_VERSION)
                throw new StreamsException(
                    $"Unknown subscription version {version}, the newest supported version is {SubscriptionWrapper<K>.CURRENT_VERSION}");

            var instruction = (SubscriptionInstruction) buffer.GetBytes(1)[0];
            long[] hash = null;
            int position = 2;
            if ((versionAndFlag & ForeignKeyJoinSerDesHelper.NULL_HASH_FLAG) == 0)
            {
                hash = new[] {buffer.GetLong(), buffer.GetLong()};
                position += 16;
            }

            int primaryPartition = buffer.GetInt();
            position += 4;
            var primaryKeyBytes = buffer.GetBytes(data.Length - position);
            var primaryKey = primaryKeySerdes.Deserialize(primaryKeyBytes,
                new SerializationContext(MessageComponentType.Key, primaryKeySerdesTopic, context.Headers));

            return new SubscriptionWrapper<K>(hash, instruction, primaryKey, primaryPartition, version);
        }
    }

    /// <summary>
    /// Wire format, big-endian:
    /// <c>version (1 byte, high bit set if the hash is null) | hash (16 bytes, if not null) | primary partition (4 bytes) |
    /// foreign value present (1 byte, 0 or 1) | serialized foreign value</c>.
    /// </summary>
    internal class SubscriptionResponseWrapperSerDes<VO> : AbstractSerDes<SubscriptionResponseWrapper<VO>>
    {
        private readonly string foreignValueSerdesTopic;
        private ISerDes<VO> foreignValueSerdes;

        public SubscriptionResponseWrapperSerDes(string foreignValueSerdesTopic, ISerDes<VO> foreignValueSerdes)
        {
            this.foreignValueSerdesTopic = foreignValueSerdesTopic;
            this.foreignValueSerdes = foreignValueSerdes;
        }

        public override void Initialize(SerDesContext context)
        {
            foreignValueSerdes = ForeignKeyJoinSerDesHelper.ResolveValueSerDes(foreignValueSerdes, context, "foreign value");
            base.Initialize(context);
        }

        public override byte[] Serialize(SubscriptionResponseWrapper<VO> data, SerializationContext context)
        {
            if (data == null)
                return null;

            byte[] foreignValue = data.ForeignValue == null
                ? null
                : foreignValueSerdes.Serialize(data.ForeignValue,
                    new SerializationContext(MessageComponentType.Value, foreignValueSerdesTopic, context.Headers));

            using var buffer = ByteBuffer.Build(1 + (data.OriginalValueHash == null ? 0 : 16) + 4 + 1 + (foreignValue?.Length ?? 0), true);
            buffer.Put(data.OriginalValueHash == null
                ? (byte) (data.Version | ForeignKeyJoinSerDesHelper.NULL_HASH_FLAG)
                : data.Version);
            ForeignKeyJoinSerDesHelper.PutHash(buffer, data.OriginalValueHash);
            return buffer.PutInt(data.PrimaryPartition)
                .Put((byte) (foreignValue == null ? 0 : 1))
                .Put(foreignValue)
                .ToArray();
        }

        public override SubscriptionResponseWrapper<VO> Deserialize(byte[] data, SerializationContext context)
        {
            if (data == null)
                return null;

            using var buffer = ByteBuffer.Build(data, true);
            byte versionAndFlag = buffer.GetBytes(1)[0];
            byte version = (byte) (versionAndFlag & ~ForeignKeyJoinSerDesHelper.NULL_HASH_FLAG);
            if (version > SubscriptionResponseWrapper<VO>.CURRENT_VERSION)
                throw new StreamsException(
                    $"Unknown subscription response version {version}, the newest supported version is {SubscriptionResponseWrapper<VO>.CURRENT_VERSION}");

            long[] hash = null;
            int position = 1;
            if ((versionAndFlag & ForeignKeyJoinSerDesHelper.NULL_HASH_FLAG) == 0)
            {
                hash = new[] {buffer.GetLong(), buffer.GetLong()};
                position += 16;
            }

            int primaryPartition = buffer.GetInt();
            bool hasForeignValue = buffer.GetBytes(1)[0] == 1;
            position += 5;

            VO foreignValue = default;
            if (hasForeignValue)
                foreignValue = foreignValueSerdes.Deserialize(buffer.GetBytes(data.Length - position),
                    new SerializationContext(MessageComponentType.Value, foreignValueSerdesTopic, context.Headers));

            return new SubscriptionResponseWrapper<VO>(hash, foreignValue, primaryPartition, version);
        }
    }

    /// <summary>
    /// Key of the subscription store: <c>length of the serialized foreign key (4 bytes, big-endian) | serialized foreign key |
    /// serialized primary key</c>, so all the subscriptions of a foreign key share a prefix (same layout as Kafka Streams).
    /// </summary>
    internal class CombinedKeySchema<KO, K>
    {
        private readonly string foreignKeySerdesTopic;
        private readonly string primaryKeySerdesTopic;
        private ISerDes<KO> foreignKeySerdes;
        private ISerDes<K> primaryKeySerdes;

        public CombinedKeySchema(string foreignKeySerdesTopic, ISerDes<KO> foreignKeySerdes,
            string primaryKeySerdesTopic, ISerDes<K> primaryKeySerdes)
        {
            this.foreignKeySerdesTopic = foreignKeySerdesTopic;
            this.foreignKeySerdes = foreignKeySerdes;
            this.primaryKeySerdesTopic = primaryKeySerdesTopic;
            this.primaryKeySerdes = primaryKeySerdes;
        }

        public void Initialize(SerDesContext context)
        {
            foreignKeySerdes = ForeignKeyJoinSerDesHelper.ResolveKeySerDes(foreignKeySerdes, context, "foreign key");
            primaryKeySerdes = ForeignKeyJoinSerDesHelper.ResolveKeySerDes(primaryKeySerdes, context, "primary key");
        }

        public byte[] SerializeForeignKey(KO foreignKey)
            => foreignKeySerdes.Serialize(foreignKey,
                new SerializationContext(MessageComponentType.Key, foreignKeySerdesTopic));

        public Bytes ToBytes(KO foreignKey, K primaryKey)
        {
            var fk = SerializeForeignKey(foreignKey);
            var pk = primaryKeySerdes.Serialize(primaryKey,
                new SerializationContext(MessageComponentType.Key, primaryKeySerdesTopic));
            using var buffer = ByteBuffer.Build(4 + fk.Length + pk.Length, true);
            return Bytes.Wrap(buffer.PutInt(fk.Length).Put(fk).Put(pk).ToArray());
        }

        public Bytes PrefixBytes(KO foreignKey)
        {
            var fk = SerializeForeignKey(foreignKey);
            using var buffer = ByteBuffer.Build(4 + fk.Length, true);
            return Bytes.Wrap(buffer.PutInt(fk.Length).Put(fk).ToArray());
        }

        public (KO ForeignKey, K PrimaryKey) FromBytes(Bytes bytes)
        {
            var data = bytes.Get;
            using var buffer = ByteBuffer.Build(data, true);
            int foreignKeyLength = buffer.GetInt();
            var fk = buffer.GetBytes(foreignKeyLength);
            var pk = buffer.GetBytes(data.Length - 4 - foreignKeyLength);
            return (foreignKeySerdes.Deserialize(fk, new SerializationContext(MessageComponentType.Key, foreignKeySerdesTopic)),
                primaryKeySerdes.Deserialize(pk, new SerializationContext(MessageComponentType.Key, primaryKeySerdesTopic)));
        }

        /// <summary>
        /// Smallest byte array greater than all the arrays starting with <paramref name="prefix"/> (Kafka's Bytes.increment),
        /// null if there is none (all the bytes are 0xFF).
        /// </summary>
        internal static Bytes PrefixUpperBound(Bytes prefix)
        {
            var bytes = (byte[]) prefix.Get.Clone();
            for (int i = bytes.Length - 1; i >= 0; i--)
            {
                if (bytes[i] != 0xFF)
                {
                    bytes[i]++;
                    var result = new byte[i + 1];
                    Array.Copy(bytes, result, i + 1);
                    return Bytes.Wrap(result);
                }
            }

            return null;
        }

        internal static bool StartsWith(byte[] bytes, byte[] prefix)
        {
            if (bytes.Length < prefix.Length)
                return false;
            for (int i = 0; i < prefix.Length; i++)
                if (bytes[i] != prefix[i])
                    return false;
            return true;
        }
    }
}
