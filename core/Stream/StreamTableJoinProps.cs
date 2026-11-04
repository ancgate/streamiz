using System;
using Streamiz.Kafka.Net.SerDes;

namespace Streamiz.Kafka.Net.Stream
{
    /// <summary>
    /// Class used to configure the key and value serdes in Stream-Table join operation.
    /// </summary>
    public class StreamTableJoinProps<K, V1, V2>
    {
        /// <summary>
        /// Creates a <see cref="StreamTableJoinProps{K,V1,V2}"/>
        /// </summary>
        /// <typeparam name="K">the key type</typeparam>
        /// <typeparam name="V1">the value type</typeparam>
        /// <typeparam name="V2">the other value type</typeparam>
        public StreamTableJoinProps(
            ISerDes<K> keySerdes,
            ISerDes<V1> valueSerdes,
            ISerDes<V2> otherValueSerdes)
        {
            KeySerdes = keySerdes;
            LeftValueSerdes = valueSerdes;
            RightValueSerdes = otherValueSerdes;
        }

        /// <summary>
        /// Key serdes
        /// </summary>
        public ISerDes<K> KeySerdes { get; internal set; }

        /// <summary>
        /// Stream value serdes
        /// </summary>
        public ISerDes<V1> LeftValueSerdes { get; internal set; }

        /// <summary>
        /// Table value serdes
        /// </summary>
        public ISerDes<V2> RightValueSerdes { get; internal set; }

        /// <summary>
        /// Grace period of the join (KIP-923), null if none. See <see cref="WithGracePeriod"/>.
        /// </summary>
        public TimeSpan? GracePeriod { get; private set; }

        /// <summary>
        /// Buffer the stream records during <paramref name="gracePeriod"/> before joining them (KIP-923): each record is then
        /// joined with the table value valid at the record's timestamp, even if the table received later updates in the
        /// meantime, and out-of-order stream records are joined with the right table version.
        /// A record is emitted once the stream time passes its timestamp plus the grace period; records older than that
        /// are joined right away.
        /// The table must be a versioned table (see <see cref="Table.InMemoryVersioned"/> / <see cref="Table.RocksDbVersioned"/>),
        /// and the grace period can't be larger than its history retention.
        /// </summary>
        /// <param name="gracePeriod">grace period, zero means no buffering</param>
        /// <returns>this</returns>
        public StreamTableJoinProps<K, V1, V2> WithGracePeriod(TimeSpan gracePeriod)
        {
            if (gracePeriod < TimeSpan.Zero)
                throw new ArgumentException("The grace period of a stream-table join can't be negative", nameof(gracePeriod));
            GracePeriod = gracePeriod;
            return this;
        }
    }
}