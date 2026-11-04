namespace Streamiz.Kafka.Net.Table.Internal
{
    internal class Change<T>
    {
        public T OldValue { get; }
        public T NewValue { get; }

        /// <summary>
        /// False if the change comes from an out-of-order record of a versioned table, which is not the latest value of its key (KIP-914).
        /// </summary>
        public bool IsLatest { get; }

        public Change(T old, T @new, bool isLatest = true)
        {
            OldValue = old;
            NewValue = @new;
            IsLatest = isLatest;
        }

        public override string ToString() => $"OldValue:{OldValue}|NewValue:{NewValue}|IsLatest:{IsLatest}";
    }
}
