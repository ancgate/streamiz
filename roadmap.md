# Roadmap Feature

- [x] Dead letter queue mecanism
- [x] Reafactor External Call with similar behavior like ParrallelConsumer + a real async approach
- [ ] Auto scaling consumption (inspired KEDA)
- [x] State store restore handler with a batch approach
- [x] Evict cache store as a batch of records instead of unitary records
- [x] KIP-613 : end-to-end latency metrics
- [x] KIP-450 : Sliding windows
- [x] Session Windows
- [x] Rename WithRecordTimestamp to AlterRecordTimestamp
- [x] Versioned State Store
- [x] https://cwiki.apache.org/confluence/display/KAFKA/KIP-923%3A+Add+A+Grace+Period+to+Stream+Table+Join
- [x] KIP-424 : Allow suppression of intermediate events based on wall clock time
- [ ] Extensions Streamiz
- [x] At-most-once processing guarantee
- [ ] Support “broadcast” pattern
- [ ] Replace repartition topics with network shuffles
- [ ] Cross Cluster