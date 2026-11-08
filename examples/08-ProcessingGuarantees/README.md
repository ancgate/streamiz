# 08 - Processing guarantees

What happens to the records being processed when the application crashes, set with `StreamConfig.Guarantee`.

| | At-least-once (default) | Exactly-once | At-most-once |
| --- | --- | --- | --- |
| `Guarantee` | `AT_LEAST_ONCE` | `EXACTLY_ONCE` | `AT_MOST_ONCE` |
| Offsets committed | after the records are processed | with the outputs, in a Kafka transaction | right after the poll, before processing |
| After a crash | records since the last commit processed again | aborted transaction, records processed again: each counted once | records being processed are lost |
| Duplicated output | possible | no (for `read_committed` consumers) | no |
| Lost record | no | no | possible |
| Producer | retries | idempotent, transactional | no idempotence, `MessageSendMaxRetries = 0` |
| Consumers of the output | should be idempotent | must read with `isolation.level=read_committed` | - |
| Commit interval default | 30 s | 100 ms | 30 s |
| Cost | lowest | transactions: more latency and broker load | lowest |
| Use it for | most applications | balances, counters, payments: state changed exactly once | notifications, SMS, sampled metrics: a duplicate is worse than a gap |

```bash
cd examples/08-ProcessingGuarantees/src
dotnet run -- <example>
```

Docs: [configuration](../../docs/stream-configuration.md) (`Guarantee`).

## at-least-once

`payments` (key: payment id) → `ledger`. The ledger entry keeps the payment id as key, so a consumer can detect a duplicate written after a crash. `CommitIntervalMs` bounds how much is processed again.

```
pay-1 -> acc-1;1500  =>  ledger: pay-1 -> ledger-entry|pay-1|acc-1;1500
```

## exactly-once

`transactions` (key: account, value: amount in cents) → the balance of each account → `balances`. A duplicate would credit an account twice; with `EXACTLY_ONCE` the output, the store's changelog and the consumed offsets are committed together. Setting the guarantee also sets `IsolationLevel = ReadCommitted`, an idempotent producer and a 100 ms commit interval (each commit ends a transaction). Kafka needs 3 brokers by default for its transaction log (or lower `transaction.state.log.replication.factor` / `min.isr` in development).

```
acc-1 -> +1500, acc-1 -> -200, acc-2 -> +700  =>  balances: acc-1 -> 1300, acc-2 -> 700
```

## at-most-once

`order-shipped` (key: customer, value: order id) → `push-notifications`, which a service sends to the customer's phone. Sending a notification twice is worse than missing one, so the offsets are committed as soon as the records are polled and the producer never retries.

```
customer-1 -> order-42  =>  push-notifications: Your order order-42 is on its way!
```

## Tests

The guarantees are about crashes and transactions, which the `TopologyTestDriver` doesn't simulate (it always runs at-least-once). The tests check what each guarantee sets in the configuration, and the results of the topologies. To observe them, run an example against the cluster of `environment/docker-compose.yml`, kill it (`kill -9`) while it processes, restart it and compare the outputs.
