using Streamiz.Kafka.Net.Metrics;
using Streamiz.Kafka.Net.Processors;

namespace Streamiz.Kafka.Net.State.Internal
{
    /// <summary>
    /// A store (the metered wrapper) which records the restoration of its content from the changelog.
    /// </summary>
    internal interface IRestoreSensorStore
    {
        /// <summary>
        /// Sensor recording the latency and rate of restoration batches, null before the store is initialized.
        /// </summary>
        Sensor RestoreSensor { get; }
    }

    internal static class RestoreSensorStoreExtensions
    {
        /// <summary>
        /// Run <paramref name="restore"/> and record its latency in the store's restore sensor, if it has one.
        /// </summary>
        internal static void MeasureRestore(this IStateStore store, System.Action restore)
        {
            var sensor = (store as IRestoreSensorStore)?.RestoreSensor;
            if (sensor == null || sensor.NoRunnable)
                restore();
            else
                Crosscutting.ActionHelper.MeasureLatency(restore, sensor);
        }
    }
}
