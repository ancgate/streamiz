using Microsoft.Extensions.Logging;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table.Internal;

namespace Streamiz.Kafka.Net.Processors
{
    internal abstract class AbstractKTableKTableJoinProcessor<K, V1, V2, VR> : AbstractProcessor<K, Change<V1>>
    {
        protected readonly IKTableValueGetter<K, V2> valueGetter;
        protected readonly IValueJoiner<V1, V2, VR> joiner;
        protected readonly bool sendOldValues;

        public AbstractKTableKTableJoinProcessor(IKTableValueGetter<K, V2> valueGetter, IValueJoiner<V1, V2, VR> joiner, bool sendOldValues)
        {
            this.valueGetter = valueGetter;
            this.joiner = joiner;
            this.sendOldValues = sendOldValues;
        }

        // KIP-914: an out-of-order record of a versioned table (not the latest value of its key) is not joined
        protected bool SkipNonLatest(Change<V1> value)
        {
            if (value.IsLatest)
                return false;

            log.LogDebug(
                "{LogPrefix}Skipping out-of-order record from versioned table while performing table-table join. topic=[{Topic}] partition=[{Partition}] offset=[{Offset}]",
                logPrefix, Context.Topic, Context.Partition, Context.Offset);
            droppedRecordsSensor.Record();
            return true;
        }

        public override void Close()
        {
            base.Close();
            valueGetter.Close();
        }

        public override void Init(ProcessorContext context)
        {
            base.Init(context);
            valueGetter.Init(context);
        }
    }
}