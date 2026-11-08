namespace Streamiz.Kafka.Net.Crosscutting
{
    /// <summary>
    /// MurmurHash3 x64 128-bit, ported from Kafka Streams' <c>org.apache.kafka.streams.state.internals.Murmur3</c>
    /// (same algorithm and seed). Used by the foreign-key join to hash the left value of a subscription.
    /// </summary>
    internal static class Murmur3
    {
        // from Kafka Streams
        internal const int DEFAULT_SEED = 104729;

        private const ulong C1 = 0x87c37b91114253d5UL;
        private const ulong C2 = 0x4cf5ad432745937fUL;
        private const int R1 = 31;
        private const int R2 = 27;
        private const int R3 = 33;
        private const ulong M = 5;
        private const ulong N1 = 0x52dce729;
        private const ulong N2 = 0x38495ab5;

        /// <summary>
        /// 128-bit hash of <paramref name="data"/> with the default seed, as two longs (h1, h2).
        /// </summary>
        internal static long[] Hash128(byte[] data)
            => Hash128(data, 0, data?.Length ?? 0, DEFAULT_SEED);

        internal static long[] Hash128(byte[] data, int offset, int length, int seed)
        {
            ulong h1 = (ulong) (long) seed;
            ulong h2 = (ulong) (long) seed;
            int nblocks = length >> 4;

            // body
            for (int i = 0; i < nblocks; i++)
            {
                int i16 = offset + (i << 4);
                ulong k1 = GetLittleEndianLong(data, i16);
                ulong k2 = GetLittleEndianLong(data, i16 + 8);

                // mix functions for k1
                k1 *= C1;
                k1 = RotateLeft(k1, R1);
                k1 *= C2;
                h1 ^= k1;
                h1 = RotateLeft(h1, R2);
                h1 += h2;
                h1 = h1 * M + N1;

                // mix functions for k2
                k2 *= C2;
                k2 = RotateLeft(k2, R3);
                k2 *= C1;
                h2 ^= k2;
                h2 = RotateLeft(h2, R1);
                h2 += h1;
                h2 = h2 * M + N2;
            }

            // tail
            ulong t1 = 0;
            ulong t2 = 0;
            int tailStart = offset + (nblocks << 4);
            int tailLength = length - (nblocks << 4);

            // bytes 8..14 of the tail go in k2, bytes 0..7 in k1 (little endian)
            for (int i = tailLength - 1; i >= 8; i--)
                t2 ^= (ulong) data[tailStart + i] << ((i - 8) * 8);
            if (tailLength > 8)
            {
                t2 *= C2;
                t2 = RotateLeft(t2, R3);
                t2 *= C1;
                h2 ^= t2;
            }

            for (int i = System.Math.Min(tailLength, 8) - 1; i >= 0; i--)
                t1 ^= (ulong) data[tailStart + i] << (i * 8);
            if (tailLength > 0)
            {
                t1 *= C1;
                t1 = RotateLeft(t1, R1);
                t1 *= C2;
                h1 ^= t1;
            }

            // finalization
            h1 ^= (ulong) length;
            h2 ^= (ulong) length;

            h1 += h2;
            h2 += h1;

            h1 = Fmix64(h1);
            h2 = Fmix64(h2);

            h1 += h2;
            h2 += h1;

            return new[] {(long) h1, (long) h2};
        }

        private static ulong GetLittleEndianLong(byte[] data, int index)
        {
            ulong result = 0;
            for (int i = 7; i >= 0; i--)
                result = (result << 8) | data[index + i];
            return result;
        }

        private static ulong RotateLeft(ulong value, int distance)
            => (value << distance) | (value >> (64 - distance));

        private static ulong Fmix64(ulong h)
        {
            h ^= h >> 33;
            h *= 0xff51afd7ed558ccdUL;
            h ^= h >> 33;
            h *= 0xc4ceb9fe1a85ec53UL;
            h ^= h >> 33;
            return h;
        }
    }
}
