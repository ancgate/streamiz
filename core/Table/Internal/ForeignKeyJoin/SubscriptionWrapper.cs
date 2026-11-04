using System.Linq;

namespace Streamiz.Kafka.Net.Table.Internal.ForeignKeyJoin
{
    /// <summary>
    /// What the right (foreign) side must do with a subscription (same values as Kafka Streams).
    /// </summary>
    internal enum SubscriptionInstruction : byte
    {
        /// <summary>
        /// Remove the subscription, don't send a response (the left record changed its foreign key)
        /// </summary>
        DELETE_KEY_NO_PROPAGATE = 0,

        /// <summary>
        /// Remove the subscription and send a response with a null foreign value (the left record was deleted)
        /// </summary>
        DELETE_KEY_AND_PROPAGATE = 1,

        /// <summary>
        /// Store the subscription and always send a response, with a null foreign value if there is none
        /// (left join, or a foreign key change which must "unset" the previous result)
        /// </summary>
        PROPAGATE_NULL_IF_NO_FK_VAL_AVAILABLE = 2,

        /// <summary>
        /// Store the subscription and send a response only if a foreign value exists (inner join of a new left record)
        /// </summary>
        PROPAGATE_ONLY_IF_FK_VAL_AVAILABLE = 3
    }

    /// <summary>
    /// Subscription sent by the left table to the right table, keyed by the foreign key.
    /// </summary>
    internal class SubscriptionWrapper<K>
    {
        internal const byte CURRENT_VERSION = 1;

        public SubscriptionWrapper(long[] hash, SubscriptionInstruction instruction, K primaryKey, int primaryPartition)
            : this(hash, instruction, primaryKey, primaryPartition, CURRENT_VERSION)
        {
        }

        internal SubscriptionWrapper(long[] hash, SubscriptionInstruction instruction, K primaryKey, int primaryPartition, byte version)
        {
            Hash = hash;
            Instruction = instruction;
            PrimaryKey = primaryKey;
            PrimaryPartition = primaryPartition;
            Version = version;
        }

        /// <summary>
        /// Murmur3 128-bit hash of the serialized left value, null if the left value is null
        /// </summary>
        public long[] Hash { get; }

        public SubscriptionInstruction Instruction { get; }

        public K PrimaryKey { get; }

        /// <summary>
        /// Partition of the left record, where the response is sent back
        /// </summary>
        public int PrimaryPartition { get; }

        public byte Version { get; }

        internal static bool HashEquals(long[] hash1, long[] hash2)
            => hash1 == null ? hash2 == null : hash2 != null && hash1.SequenceEqual(hash2);

        public override string ToString()
            => $"SubscriptionWrapper{{version={Version}, primaryKey={PrimaryKey}, instruction={Instruction}, primaryPartition={PrimaryPartition}}}";
    }

    /// <summary>
    /// Response sent by the right table to the left table, keyed by the primary key.
    /// </summary>
    internal class SubscriptionResponseWrapper<VO>
    {
        internal const byte CURRENT_VERSION = 1;

        public SubscriptionResponseWrapper(long[] originalValueHash, VO foreignValue, int primaryPartition)
            : this(originalValueHash, foreignValue, primaryPartition, CURRENT_VERSION)
        {
        }

        internal SubscriptionResponseWrapper(long[] originalValueHash, VO foreignValue, int primaryPartition, byte version)
        {
            OriginalValueHash = originalValueHash;
            ForeignValue = foreignValue;
            PrimaryPartition = primaryPartition;
            Version = version;
        }

        /// <summary>
        /// Hash of the left value which sent the subscription, to drop stale responses
        /// </summary>
        public long[] OriginalValueHash { get; }

        public VO ForeignValue { get; }

        public int PrimaryPartition { get; }

        public byte Version { get; }

        public override string ToString()
            => $"SubscriptionResponseWrapper{{version={Version}, foreignValue={ForeignValue}, primaryPartition={PrimaryPartition}}}";
    }
}
