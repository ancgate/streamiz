using System;

namespace Streamiz.Kafka.Net.Stream
{
    /// <summary>
    /// Sliding windows (KIP-450) are fixed-size windows which slide continuously over the time axis:
    /// two records are in the same window if the difference of their timestamps is less than or equal to the time difference.
    /// Unlike hopping windows, the windows are not aligned to the epoch: they are created from the record timestamps,
    /// so each distinct set of records in a time difference gives one window, and empty windows are never created.
    /// <para>
    /// A window is defined by its start and end, both inclusive: [start, start + time difference].
    /// Each window is returned as a <see cref="TimeWindow"/> with <see cref="Window.EndMs"/> = start + time difference,
    /// so the output keys can be deserialized with a <see cref="SerDes.TimeWindowedSerDes{T}"/> using the time difference as window size.
    /// </para>
    /// <para>
    /// Records with a timestamp smaller than the time difference all go in the window [0, time difference],
    /// because a window can't start before 0.
    /// </para>
    /// </summary>
    public class SlidingWindowOptions
    {
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="timeDifferenceMs">the max time difference (in ms) between two records to be in the same window</param>
        /// <param name="graceMs">the time (in ms) to admit out-of-order records after the end of a window</param>
        protected SlidingWindowOptions(long timeDifferenceMs, long graceMs)
        {
            if (timeDifferenceMs < 0)
                throw new ArgumentException("Window time difference must not be negative.");
            if (graceMs < 0)
                throw new ArgumentException("Window grace period must not be negative.");

            TimeDifferenceMs = timeDifferenceMs;
            GracePeriodMs = graceMs;
        }

        /// <summary>
        /// Max time difference (in ms) between two records to be in the same window. It is also the size of the windows.
        /// </summary>
        public long TimeDifferenceMs { get; }

        /// <summary>
        /// Time (in ms) to admit out-of-order records after the end of a window
        /// </summary>
        public long GracePeriodMs { get; }

        /// <summary>
        /// Create sliding windows with a time difference and no grace period:
        /// out-of-order records are dropped once their windows are closed.
        /// </summary>
        /// <param name="timeDifferenceMs">the max time difference (in ms) between two records to be in the same window</param>
        /// <returns>Return a <see cref="SlidingWindowOptions"/> instance</returns>
        public static SlidingWindowOptions Of(long timeDifferenceMs)
            => new(timeDifferenceMs, 0);

        /// <summary>
        /// Create sliding windows with a time difference and a grace period.
        /// </summary>
        /// <param name="timeDifferenceMs">the max time difference (in ms) between two records to be in the same window</param>
        /// <param name="graceMs">the time (in ms) to admit out-of-order records after the end of a window</param>
        /// <returns>Return a <see cref="SlidingWindowOptions"/> instance</returns>
        public static SlidingWindowOptions Of(long timeDifferenceMs, long graceMs)
            => new(timeDifferenceMs, graceMs);

        /// <summary>
        /// Create sliding windows with a time difference and no grace period:
        /// out-of-order records are dropped once their windows are closed.
        /// </summary>
        /// <param name="timeDifference">the max time difference between two records to be in the same window</param>
        /// <returns>Return a <see cref="SlidingWindowOptions"/> instance</returns>
        public static SlidingWindowOptions Of(TimeSpan timeDifference)
            => Of((long) timeDifference.TotalMilliseconds);

        /// <summary>
        /// Create sliding windows with a time difference and a grace period.
        /// </summary>
        /// <param name="timeDifference">the max time difference between two records to be in the same window</param>
        /// <param name="grace">the time to admit out-of-order records after the end of a window</param>
        /// <returns>Return a <see cref="SlidingWindowOptions"/> instance</returns>
        public static SlidingWindowOptions Of(TimeSpan timeDifference, TimeSpan grace)
            => Of((long) timeDifference.TotalMilliseconds, (long) grace.TotalMilliseconds);
    }
}
