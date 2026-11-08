# 07 - Error handling

What happens when a record can't be read, processed or written, and how to skip it without stopping the application or losing it.

| Where it fails | Handler (`StreamConfig`) | Default | Example |
| --- | --- | --- | --- |
| Deserializing a consumed record | `DeserializationExceptionHandler` | `FAIL` | `skip-bad-json` |
| Your code, while processing a record | `ProcessingExceptionHandler` | `FAIL` | `processing-errors` |
| Writing an output record to Kafka | `ProductionExceptionHandler` | `FAIL` | `production-errors` |
| Anything that reaches the stream thread | `InnerExceptionHandler` | `FAIL` | `inner-exception-continue` |
| An external call that keeps failing | `RetryPolicy` (`EndRetryBehavior`) | `SKIP` | `async-retry-dlq` |

Records skipped by the deserialization handler, the processing handler or an async retry policy can be sent to a **dead letter queue** (`DeadLetterQueueTopicName`): see `dead-letter-queue`.

```bash
cd examples/07-ErrorHandling/src
dotnet run -- <example>
```

Docs: [configuration](../../docs/stream-configuration.md) (handlers and dead letter queue), [async processing](../../docs/async-processing.md).

## skip-bad-json

`payments` holds JSON payments read with `JsonSerDes<Payment>`. An invalid record makes the serdes throw; the `DeserializationExceptionHandler` logs it and returns `CONTINUE`, so it's dropped.

```
p1 -> {"Id":"p1","Amount":10.5,"Currency":"EUR"}   =>  payment-amounts: 10.5 EUR
p2 -> {not json                                     =>  skipped (handler called)
p3 -> {"Id":"p3","Amount":3,"Currency":"USD"}       =>  payment-amounts: 3 USD
```

The handler is called once per bad record.

## processing-errors

`orders` values are `<item>;<quantity>`; `int.Parse` throws on `mouse;two`. The `ProcessingExceptionHandler` gets an `ErrorHandlerContext` (topic, partition, offset, timestamp, headers, raw key and value, the processor which failed, the task) and the exception:

- `CONTINUE` for a bad record (`FormatException`): it's skipped and the next records are processed, without closing the tasks or rebalancing.
- `FAIL` for anything else: the exception goes to the stream thread (and the `InnerExceptionHandler`).

```
o1 -> keyboard;2   =>  parsed-orders: 2 x keyboard
o2 -> mouse;two    =>  skipped: orders[0]@1 in processor parse-order
o3 -> screen;1     =>  parsed-orders: 1 x screen
```

## dead-letter-queue

With `DeadLetterQueueTopicName = "orders-dlq"`, the skipped records are written to that topic as they were consumed (raw key, value, headers, timestamp), with headers describing the error:

| Header | Value |
| --- | --- |
| `__streams.errors.exception` | `System.FormatException` |
| `__streams.errors.message` | the exception message |
| `__streams.errors.stacktrace` | the exception and its stack trace |
| `__streams.errors.topic` / `.partition` / `.offset` | where the record came from: `orders`, `0`, `1` |

Both a processing error (`orders`) and a deserialization error (`payments`) end up there. Create the topic before starting: Streamiz doesn't create it. With exactly-once, the dead letter record is written in the same transaction as the outputs.

## production-errors

The `ProductionExceptionHandler` gets the `DeliveryReport` of a record Kafka refused, after the producer's own retries (`MessageSendMaxRetries`):

- `CONTINUE` for a record too large for the topic (retrying won't help);
- `RETRY` for timeouts and missing replicas: the record is sent again on the next iteration;
- `FAIL` for the rest (authorization, unknown topic...).

The test calls the handler directly: the test driver can't make Kafka refuse a record. Production errors are not sent to the dead letter queue.

## inner-exception-continue

The `InnerExceptionHandler` receives the exceptions which reach the stream thread. `CONTINUE` closes the tasks, rejoins the consumer group and skips the record which failed; the records processed since the last commit can be processed again (at-least-once):

```
o1 -> keyboard, o2 -> poison, o3 -> screen  =>  orders-checked: keyboard (maybe twice), screen
```

Prefer the `ProcessingExceptionHandler` to skip bad records: it doesn't close the tasks nor trigger a rebalance. The test runs with `TopologyTestDriver.Mode.ASYNC_CLUSTER_IN_MEMORY`, which runs a real stream thread.

## async-retry-dlq

`MapValuesAsync` calls an external fraud-check service with a `RetryPolicy`: `HttpRequestException` is retried twice, 10 ms apart, then `EndRetryBehavior.SKIP` drops the record, and the dead letter queue keeps it (exception `NoneRetryableException`). `AsyncProcessingMode.V2` runs the calls inside the stream task. See [10-AsyncProcessing](../10-AsyncProcessing/README.md) for the async engines.

```
p1 -> payment-1               =>  payments-checked: payment-1:ok
p2 -> payment-2-unreachable   =>  payments-dlq: payment-2-unreachable
p3 -> payment-3               =>  payments-checked: payment-3:ok
```
