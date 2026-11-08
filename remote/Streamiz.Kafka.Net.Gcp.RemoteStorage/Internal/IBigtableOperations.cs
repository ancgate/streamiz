using System;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Gcp.RemoteStorage.Internal
{
    /// <summary>
    /// The Bigtable calls used by <see cref="BigtableStore"/>, on one table and one column. Rows hold one value;
    /// a null value deletes the row.
    /// </summary>
    internal interface IBigtableOperations : IDisposable
    {
        /// <summary>
        /// Create the table and the column family if they don't exist
        /// </summary>
        void EnsureTable();

        /// <summary>
        /// Value of a row, null if the row doesn't exist
        /// </summary>
        byte[] Read(byte[] rowKey);

        /// <summary>
        /// Write the value of a row, or delete the row if the value is null
        /// </summary>
        void Write(byte[] rowKey, byte[] value);

        /// <summary>
        /// Write (or delete) several rows in one request. Throws if a row couldn't be written.
        /// </summary>
        void WriteAll(IReadOnlyList<KeyValuePair<byte[], byte[]>> rows);

        /// <summary>
        /// Rows from <paramref name="startClosed"/> (included) to <paramref name="endOpen"/> (excluded, null : no end),
        /// sorted by row key, in reverse order if <paramref name="reversed"/>.
        /// </summary>
        IEnumerable<KeyValuePair<byte[], byte[]>> Scan(byte[] startClosed, byte[] endOpen, bool reversed);
    }
}
