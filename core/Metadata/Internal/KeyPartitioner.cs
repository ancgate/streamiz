using Confluent.Kafka;

namespace Streamiz.Kafka.Net.Processors.Internal
{
    /// <summary>
    /// Partition of a serialized key, computed like the producer's partitioner (librdkafka), so the partition of a key in a
    /// store can be found without producing it.
    /// </summary>
    internal static class KeyPartitioner
    {
        /// <summary>
        /// Partition of <paramref name="keyBytes"/> for the configured producer partitioner, or null if it can't be computed
        /// (null key, random partitioner).
        /// </summary>
        internal static int? Partition(Partitioner? partitioner, byte[] keyBytes, int numPartitions)
        {
            if (keyBytes == null || numPartitions <= 0)
                return null;

            switch (partitioner ?? Partitioner.Murmur2Random)
            {
                case Partitioner.Murmur2:
                case Partitioner.Murmur2Random:
                    return (Murmur2(keyBytes) & 0x7fffffff) % numPartitions;
                case Partitioner.Consistent:
                case Partitioner.ConsistentRandom:
                    return (int) (Crc32(keyBytes) % (uint) numPartitions);
                default:
                    return null; // random
            }
        }

        /// <summary>
        /// Murmur2 hash of the Java producer (org.apache.kafka.common.utils.Utils.murmur2), also used by librdkafka's murmur2 partitioners
        /// </summary>
        internal static int Murmur2(byte[] data)
        {
            int length = data.Length;
            const uint seed = 0x9747b28c;
            const uint m = 0x5bd1e995;
            const int r = 24;

            uint h = seed ^ (uint) length;
            int length4 = length / 4;

            for (int i = 0; i < length4; i++)
            {
                int i4 = i * 4;
                uint k = (uint) (data[i4] & 0xff) + ((uint) (data[i4 + 1] & 0xff) << 8) +
                         ((uint) (data[i4 + 2] & 0xff) << 16) + ((uint) (data[i4 + 3] & 0xff) << 24);
                k *= m;
                k ^= k >> r;
                k *= m;
                h *= m;
                h ^= k;
            }

            switch (length % 4)
            {
                case 3:
                    h ^= (uint) (data[(length & ~3) + 2] & 0xff) << 16;
                    goto case 2;
                case 2:
                    h ^= (uint) (data[(length & ~3) + 1] & 0xff) << 8;
                    goto case 1;
                case 1:
                    h ^= (uint) (data[length & ~3] & 0xff);
                    h *= m;
                    break;
            }

            h ^= h >> 13;
            h *= m;
            h ^= h >> 15;

            return unchecked((int) h);
        }

        private static readonly uint[] crcTable = BuildCrcTable();

        private static uint[] BuildCrcTable()
        {
            var table = new uint[256];
            for (uint i = 0; i < 256; i++)
            {
                uint c = i;
                for (int j = 0; j < 8; j++)
                    c = (c & 1) != 0 ? 0xEDB88320 ^ (c >> 1) : c >> 1;
                table[i] = c;
            }

            return table;
        }

        /// <summary>
        /// CRC-32 (IEEE), used by librdkafka's consistent partitioners
        /// </summary>
        internal static uint Crc32(byte[] data)
        {
            uint crc = 0xFFFFFFFF;
            foreach (var b in data)
                crc = crcTable[(crc ^ b) & 0xFF] ^ (crc >> 8);
            return crc ^ 0xFFFFFFFF;
        }
    }
}
