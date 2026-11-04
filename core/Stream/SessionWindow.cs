namespace Streamiz.Kafka.Net.Stream
{
    /// <summary>
    /// A session based window specified by a start and end timestamp, both inclusive.
    /// A session window can have the same start and end (a session with a single record).
    /// </summary>
    public class SessionWindow : Window
    {
        /// <summary>
        /// Create a new window for the specified start and end time (both inclusive).
        /// </summary>
        /// <param name="startMs">the start timestamp of the window</param>
        /// <param name="endMs">the end timestamp of the window</param>
        public SessionWindow(long startMs, long endMs)
            : base(startMs, endMs)
        {
        }

        /// <summary>
        /// Check if the given window overlaps with this window. Windows are inclusive, so sharing a single timestamp is an overlap.
        /// </summary>
        /// <param name="other">another session window</param>
        /// <returns>true if <paramref name="other"/> overlaps with this window, false otherwise</returns>
        public override bool Overlap(Window other)
            => other is SessionWindow window && !(window.EndMs < StartMs || EndMs < window.StartMs);
    }
}
