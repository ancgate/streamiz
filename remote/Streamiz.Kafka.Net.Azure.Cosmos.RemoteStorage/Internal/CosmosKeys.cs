using System;
using System.Security.Cryptography;
using System.Text;

namespace Streamiz.Kafka.Net.Azure.Cosmos.RemoteStorage.Internal
{
    /// <summary>
    /// How the keys of a store are written in its items.
    /// </summary>
    internal static class CosmosKeys
    {
        /// <summary>
        /// Longest hexadecimal key used as the item id. Cosmos DB ids are limited to 1023 bytes,
        /// longer keys get a hashed id (see <see cref="Id"/>).
        /// </summary>
        internal const int MaxHexIdLength = 1000;

        internal const string HashedIdPrefix = "h-";

        // an id can't be empty: id of the empty key (a hexadecimal id has no '-')
        internal const string EmptyKeyId = "e-";

        /// <summary>
        /// Lowercase hexadecimal of the bytes. These strings compare (ordinal) like the bytes compare (unsigned),
        /// including keys which are a prefix of others, so the ranges of the store are sorted like the local stores.
        /// </summary>
        internal static string Hex(byte[] bytes)
        {
            var sb = new StringBuilder(bytes.Length * 2);
            foreach (var b in bytes)
                sb.Append(b.ToString("x2"));
            return sb.ToString();
        }

        internal static byte[] FromHex(string hex)
        {
            var bytes = new byte[hex.Length / 2];
            for (int i = 0; i < bytes.Length; ++i)
                bytes[i] = Convert.ToByte(hex.Substring(i * 2, 2), 16);
            return bytes;
        }

        /// <summary>
        /// Id of the item of a key: its hexadecimal (only 0-9 and a-f, which are valid in an id), "e-" for the empty key,
        /// or for a key longer than 500 bytes, "h-" followed by the hexadecimal of its SHA-256. The id of a key is always the
        /// same, so items can be read and deleted by id; the full key is always in <see cref="CosmosItem.K"/>.
        /// </summary>
        internal static string Id(string hexKey, byte[] key)
        {
            if (hexKey.Length == 0)
                return EmptyKeyId;
            if (hexKey.Length <= MaxHexIdLength)
                return hexKey;

            using var sha256 = SHA256.Create();
            return HashedIdPrefix + Hex(sha256.ComputeHash(key));
        }

        /// <summary>
        /// Name of the container of a store: prefix + store name, without the characters Cosmos DB doesn't accept in an id
        /// ('/', '\', '#', '?'), at most 255 characters.
        /// </summary>
        internal static string ContainerName(string prefix, string storeName)
        {
            var name = (prefix ?? string.Empty) + (storeName ?? string.Empty);
            var sb = new StringBuilder(name.Length);
            foreach (var c in name)
                sb.Append(c is '/' or '\\' or '#' or '?' || char.IsControl(c) ? '_' : c);
            var sanitized = sb.ToString().Trim();
            if (sanitized.Length == 0)
                throw new ArgumentException("The container name of a Cosmos DB remote store can't be empty");
            return sanitized.Length > 255 ? sanitized.Substring(0, 255) : sanitized;
        }
    }
}
