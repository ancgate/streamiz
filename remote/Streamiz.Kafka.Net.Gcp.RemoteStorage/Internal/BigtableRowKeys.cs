using System;
using System.Text;

namespace Streamiz.Kafka.Net.Gcp.RemoteStorage.Internal
{
    /// <summary>
    /// Row keys of the Bigtable store: <c>len(applicationId) | applicationId | len(storeName) | storeName | len(taskId) | taskId | key</c>,
    /// with 2-byte big-endian lengths and UTF-8 strings. The length prefixes make the prefix of a task unambiguous
    /// (one prefix can't be the start of another one), and Bigtable sorts row keys as unsigned bytes,
    /// so the rows of a task are contiguous and sorted like the keys of the local stores.
    /// </summary>
    internal static class BigtableRowKeys
    {
        internal static byte[] Prefix(string applicationId, string storeName, string taskId)
        {
            var parts = new[]
            {
                Encoding.UTF8.GetBytes(applicationId ?? string.Empty),
                Encoding.UTF8.GetBytes(storeName ?? string.Empty),
                Encoding.UTF8.GetBytes(taskId ?? string.Empty)
            };

            int size = 0;
            foreach (var part in parts)
            {
                if (part.Length > ushort.MaxValue)
                    throw new ArgumentException("Application id, store name and task id must be shorter than 65536 bytes");
                size += 2 + part.Length;
            }

            var prefix = new byte[size];
            int offset = 0;
            foreach (var part in parts)
            {
                prefix[offset++] = (byte) (part.Length >> 8);
                prefix[offset++] = (byte) part.Length;
                Buffer.BlockCopy(part, 0, prefix, offset, part.Length);
                offset += part.Length;
            }

            return prefix;
        }

        internal static byte[] RowKey(byte[] prefix, byte[] key)
        {
            var rowKey = new byte[prefix.Length + key.Length];
            Buffer.BlockCopy(prefix, 0, rowKey, 0, prefix.Length);
            Buffer.BlockCopy(key, 0, rowKey, prefix.Length, key.Length);
            return rowKey;
        }

        internal static bool HasPrefix(byte[] rowKey, byte[] prefix)
        {
            if (rowKey.Length < prefix.Length)
                return false;
            for (int i = 0; i < prefix.Length; ++i)
                if (rowKey[i] != prefix[i])
                    return false;
            return true;
        }

        internal static byte[] Key(byte[] rowKey, byte[] prefix)
        {
            var key = new byte[rowKey.Length - prefix.Length];
            Buffer.BlockCopy(rowKey, prefix.Length, key, 0, key.Length);
            return key;
        }

        /// <summary>
        /// Smallest row key greater than all the row keys starting with <paramref name="prefix"/>, null if there is none.
        /// </summary>
        internal static byte[] PrefixEnd(byte[] prefix)
        {
            for (int i = prefix.Length - 1; i >= 0; --i)
            {
                if (prefix[i] != 0xFF)
                {
                    var end = new byte[i + 1];
                    Buffer.BlockCopy(prefix, 0, end, 0, i + 1);
                    end[i]++;
                    return end;
                }
            }

            return null;
        }

        /// <summary>
        /// Smallest row key greater than <paramref name="rowKey"/>: an inclusive end bound becomes an exclusive one.
        /// </summary>
        internal static byte[] Successor(byte[] rowKey)
        {
            var successor = new byte[rowKey.Length + 1];
            Buffer.BlockCopy(rowKey, 0, successor, 0, rowKey.Length);
            return successor;
        }
    }
}
