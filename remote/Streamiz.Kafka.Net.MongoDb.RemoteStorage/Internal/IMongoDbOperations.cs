using System;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.MongoDb.RemoteStorage.Internal
{
    /// <summary>
    /// One write of <see cref="IMongoDbOperations.BulkWrite"/>: the document of a key, deleted if <see cref="Value"/> is null
    /// </summary>
    internal readonly struct MongoDbWrite
    {
        public MongoDbWrite(string task, string hexKey, byte[] value)
        {
            Task = task;
            HexKey = hexKey;
            Value = value;
        }

        public string Task { get; }
        public string HexKey { get; }
        public byte[] Value { get; }
    }

    /// <summary>
    /// The MongoDB calls used by <see cref="MongoDbStore"/>, on the collection of one store.
    /// Documents are { _id: "task|hexKey", t: task, k: hexKey, v: value }.
    /// </summary>
    internal interface IMongoDbOperations : IDisposable
    {
        /// <summary>
        /// Create the unique index { t: 1, k: 1 } (simple collation) if it doesn't exist
        /// </summary>
        void EnsureIndex();

        /// <summary>
        /// Value of a key, null if there is no document
        /// </summary>
        byte[] Get(string task, string hexKey);

        /// <summary>
        /// Write the document of a key (upsert), or delete it if the value is null
        /// </summary>
        void Put(MongoDbWrite write);

        /// <summary>
        /// Delete the document of a key and return its value, null if there was no document
        /// </summary>
        byte[] Delete(string task, string hexKey);

        /// <summary>
        /// Apply the writes in order (a later write of a key wins)
        /// </summary>
        void BulkWrite(IReadOnlyList<MongoDbWrite> writes);

        /// <summary>
        /// Documents of a task with <paramref name="fromHex"/> &lt;= k &lt;= <paramref name="toHex"/> (null : no bound),
        /// sorted by k (descending if <paramref name="descending"/>), as (hexKey, value). Enumerated lazily.
        /// </summary>
        IEnumerable<KeyValuePair<string, byte[]>> Find(string task, string fromHex, string toHex, bool descending);

        /// <summary>
        /// Number of documents of a task
        /// </summary>
        long Count(string task);
    }
}
