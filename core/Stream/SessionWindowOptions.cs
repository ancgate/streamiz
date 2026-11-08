using System;

namespace Streamiz.Kafka.Net.Stream
{
    /// <summary>
    /// Session windows group the records of a key into sessions: periods of activity separated by an inactivity gap.
    /// A record is added to an existing session of its key when it is at most the inactivity gap away from it,
    /// and a record which falls between two sessions merges them. Otherwise a new session starts.
    /// <para>
    /// A session is defined by the timestamps of its first and last records, both inclusive: [start, end].
    /// Sessions are returned as <see cref="SessionWindow"/>, and the output keys can be deserialized with
    /// <see cref="SerDes.SessionWindowedSerDes{T}"/>.
    /// </para>
    /// <para>
    /// A session can still be extended by a record up to the inactivity gap after its end, so a session is closed
    /// (and late records for it are dropped) once the stream time passes its end + inactivity gap + grace period.
    /// </para>
    /// </summary>
    public class SessionWindowOptions
    {
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="inactivityGapMs">the max inactivity gap (in ms) between two records of the same session</param>
        /// <param name="graceMs">the time (in ms) to admit out-of-order records after a session is closed</param>
        protected SessionWindowOptions(long inactivityGapMs, long graceMs)
        {
            if (inactivityGapMs <= 0)
                throw new ArgumentException("Gap time cannot be zero or negative.");
            if (graceMs < 0)
                throw new ArgumentException("Grace period must not be negative.");

            InactivityGapMs = inactivityGapMs;
            GracePeriodMs = graceMs;
        }

        /// <summary>
        /// Max inactivity gap (in ms) between two records of the same session
        /// </summary>
        public long InactivityGapMs { get; }

        /// <summary>
        /// Time (in ms) to admit out-of-order records after a session is closed (end + inactivity gap)
        /// </summary>
        public long GracePeriodMs { get; }

        /// <summary>
        /// Create session windows with an inactivity gap and no grace period.
        /// </summary>
        /// <param name="inactivityGapMs">the max inactivity gap (in ms) between two records of the same session</param>
        /// <returns>Return a <see cref="SessionWindowOptions"/> instance</returns>
        public static SessionWindowOptions Of(long inactivityGapMs)
            => new(inactivityGapMs, 0);

        /// <summary>
        /// Create session windows with an inactivity gap and a grace period.
        /// </summary>
        /// <param name="inactivityGapMs">the max inactivity gap (in ms) between two records of the same session</param>
        /// <param name="graceMs">the time (in ms) to admit out-of-order records after a session is closed</param>
        /// <returns>Return a <see cref="SessionWindowOptions"/> instance</returns>
        public static SessionWindowOptions Of(long inactivityGapMs, long graceMs)
            => new(inactivityGapMs, graceMs);

        /// <summary>
        /// Create session windows with an inactivity gap and no grace period.
        /// </summary>
        /// <param name="inactivityGap">the max inactivity gap between two records of the same session</param>
        /// <returns>Return a <see cref="SessionWindowOptions"/> instance</returns>
        public static SessionWindowOptions Of(TimeSpan inactivityGap)
            => Of((long) inactivityGap.TotalMilliseconds);

        /// <summary>
        /// Create session windows with an inactivity gap and a grace period.
        /// </summary>
        /// <param name="inactivityGap">the max inactivity gap between two records of the same session</param>
        /// <param name="grace">the time to admit out-of-order records after a session is closed</param>
        /// <returns>Return a <see cref="SessionWindowOptions"/> instance</returns>
        public static SessionWindowOptions Of(TimeSpan inactivityGap, TimeSpan grace)
            => Of((long) inactivityGap.TotalMilliseconds, (long) grace.TotalMilliseconds);
    }
}
