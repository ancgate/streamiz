using System.Collections.Generic;
using Streamiz.Kafka.Net.Crosscutting;

namespace Streamiz.Kafka.Net.Processors.Internal
{
    /// <summary>
    /// Restoration logic for log-backed state stores upon restart, it takes all the records of one polled batch
    /// (in changelog order, tombstones included as null values) so the store can apply them in a single write.
    /// </summary>
    /// <param name="records">Records' key, value and timestamp (Unix milliseconds)</param>
    internal delegate void BatchingStateRestoreCallback(IReadOnlyList<(Bytes Key, byte[] Value, long Timestamp)> records);

    /// <summary>
    /// Restoration logic for log-backed state stores upon restart, it takes one record at a time from the logs to apply to the restoring state.
    /// </summary>
    /// <param name="key">Record's key</param>
    /// <param name="value">Record's value</param>
    /// /// <param name="timestamp">Record's timestamp in Unix milliseconds long format</param>
    public delegate void StateRestoreCallback(Bytes key, byte[] value, long timestamp);
}
