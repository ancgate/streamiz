using System;
using System.Text;

namespace Streamiz.Kafka.Net.MongoDb.RemoteStorage.Internal
{
    /// <summary>
    /// Encoding of the keys in the documents. The key bytes are stored as a lowercase hex string: hex strings compare
    /// (with the simple binary collation) like the bytes they encode, prefixes included, whereas BSON binary values
    /// compare by length first.
    /// </summary>
    internal static class MongoDbKeys
    {
        internal const string ID_FIELD = "_id";
        internal const string TASK_FIELD = "t";
        internal const string KEY_FIELD = "k";
        internal const string VALUE_FIELD = "v";
        internal const string INDEX_NAME = "t_1_k_1";

        private const string HEX_DIGITS = "0123456789abcdef";

        internal static string ToHex(byte[] bytes)
        {
            var chars = new char[bytes.Length * 2];
            for (int i = 0; i < bytes.Length; ++i)
            {
                chars[2 * i] = HEX_DIGITS[bytes[i] >> 4];
                chars[2 * i + 1] = HEX_DIGITS[bytes[i] & 0x0F];
            }

            return new string(chars);
        }

        internal static byte[] FromHex(string hex)
        {
            if (hex.Length % 2 != 0)
                throw new FormatException($"Invalid hex key '{hex}'");

            var bytes = new byte[hex.Length / 2];
            for (int i = 0; i < bytes.Length; ++i)
                bytes[i] = (byte) ((HexValue(hex[2 * i]) << 4) | HexValue(hex[2 * i + 1]));
            return bytes;
        }

        // the task id contains no '|', so ids of different tasks can't collide
        internal static string Id(string task, string hexKey) => task + "|" + hexKey;

        /// <summary>
        /// Collection name of a store: prefix + store name, without the characters MongoDB forbids
        /// ('$' and the null character), and not in the reserved "system." namespace.
        /// </summary>
        internal static string CollectionName(string prefix, string storeName)
        {
            var name = new StringBuilder();
            foreach (var c in (prefix ?? string.Empty) + (storeName ?? string.Empty))
                name.Append(c == '$' || c == '\0' ? '_' : c);

            if (name.Length == 0)
                return "streamiz";

            var result = name.ToString();
            return result.StartsWith("system.", StringComparison.Ordinal) ? "_" + result : result;
        }

        private static int HexValue(char c)
        {
            if (c >= '0' && c <= '9') return c - '0';
            if (c >= 'a' && c <= 'f') return c - 'a' + 10;
            if (c >= 'A' && c <= 'F') return c - 'A' + 10;
            throw new FormatException($"Invalid hex character '{c}'");
        }
    }
}
